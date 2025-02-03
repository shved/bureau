use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::Cursor;
use std::{fmt, io};
use tokio_util::codec::{Decoder, Encoder};

const CODEC_BUFFER_MAX: usize = 4 * 1024 * 1024; // 4KB.
const LNG_SEC: usize = 2; // 2B.

/// Codec for server side. Decodes requests and encodes responses.
/// The opposite of ClientMessenger.
#[derive(Debug, Default, Clone, Copy)]
pub struct ServerMessenger {}

/// Client side codec. Encodes requests for server and decodes servers responses.
/// The opposite of ServerMessenger.
#[derive(Debug, Default, Clone, Copy)]
pub struct ClientMessenger {}

/// Request commands supported.
#[derive(Debug, Clone)]
pub enum Request {
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes },
}

/// Mode is byte coded command mode of a request. Tells the server how properly decode the following
/// bytes of request message. Basic commands are get value by given key, set value for a key.
#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Mode {
    Get = 0x00,
    Set = 0x01,
    Unknown = 0xFF,
}

impl Mode {
    pub fn from_byte(byte: u8) -> Self {
        match byte {
            0x00 => Mode::Get,
            0x01 => Mode::Set,
            _ => Mode::Unknown,
        }
    }

    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/// Possible responses structures.
#[derive(Debug, Clone)]
pub enum Response {
    Ok,
    OkValue { value: Bytes },
    Error { message: Bytes },
}

/// Status is byte coded response status. Tells the client how to properly decode following bytes
/// of the response. Basic response statuses are Ok, Ok with value provided and Error with error message.
#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Status {
    Ok = 0x00,
    OkValue = 0x01,
    Error = 0x02,
    Unknown = 0xFF,
}

impl Status {
    pub fn from_byte(byte: u8) -> Self {
        match byte {
            0x00 => Status::Ok,
            0x01 => Status::OkValue,
            0x02 => Status::Error,
            _ => Status::Unknown,
        }
    }

    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/*
Request message layout schema.
--------------------------------------------------------------------------------------------
| Message Size (LNG_SEC)| Mode | Key Size | Key | Value Size (Optional) | Value (Optional) |
--------------------------------------------------------------------------------------------
|          2B           |  1B  |    2B    | ... |          2B           |       ...        |
--------------------------------------------------------------------------------------------
*/
impl Decoder for ServerMessenger {
    type Item = Request;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < LNG_SEC + 1 {
            // Not enough data to read length marker.
            return Ok(None);
        }

        // Read length marker.
        let mut length_bytes = [0u8; LNG_SEC];
        length_bytes.copy_from_slice(&src[..LNG_SEC]);
        let length = u16::from_le_bytes(length_bytes) as usize;

        // Check that the length is not too large to avoid a denial of
        // service attack where the server runs out of memory.
        if length > CODEC_BUFFER_MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame of length {} is too large", length),
            ));
        }

        if src.len() < LNG_SEC + length {
            // Frame has not yet arrived.
            // We reserve more space in the buffer. This is not strictly
            // necessary, but is a good idea performance-wise.
            src.reserve(LNG_SEC + length - src.len());

            // We inform the Framed that we need more bytes to form the next frame.
            return Ok(None);
        }

        // Use advance to modify src such that it no longer contains this frame.
        // TODO: It should not be necessary here to make slice owned to pass to parse function.
        let data = src[LNG_SEC..LNG_SEC + length].to_vec();
        src.advance(LNG_SEC + length);

        // Parse the payload.
        match Request::parse(data.as_ref()) {
            Ok(request) => Ok(Some(request)),
            Err(error) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
        }
    }
}

/*
Request layout schema.
--------------------------------------------------------------------
| Mode | Key Size | Key | Value Size (Optional) | Value (Optional) |
--------------------------------------------------------------------
|  1B  |    2B    | ... |          2B           |       ...        |
--------------------------------------------------------------------
*/
impl Request {
    /// Parses protocol message into a Request object.
    fn parse(raw: &[u8]) -> crate::Result<Self> {
        if raw.len() < 4 {
            return Err("message invalid".into());
        }

        let mut buf = Cursor::new(raw);

        let cmd_mode = Mode::from_byte(buf.get_u8());
        match cmd_mode {
            Mode::Get => {
                let key_size = buf.get_u16();
                let key = buf.copy_to_bytes(key_size as usize);
                Ok(Request::Get { key })
            }
            Mode::Set => {
                let key_size = buf.get_u16();
                let key = buf.copy_to_bytes(key_size as usize);
                let value_size = buf.get_u16();
                let value = buf.copy_to_bytes(value_size as usize);
                Ok(Request::Set { key, value })
            }
            _ => Err("unknown command".into()),
        }
    }

    /// Handy function to help command line client translate string input into a request message.
    pub fn from_string_input(input: String) -> std::result::Result<Request, io::Error> {
        let mut parts = input.split_whitespace();

        let command_switch = parts.next().ok_or(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no command given",
        ))?;

        match command_switch {
            "GET" => {
                let key_str = parts.next().ok_or(io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "no key provided with GET request",
                ))?;

                return Ok(Request::Get {
                    key: Bytes::from(key_str.to_owned()),
                });
            }
            "SET" => {
                let key_str = parts.next().ok_or(io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "no key provided with SET request",
                ))?;

                let value_str = parts.next().ok_or(io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "no value provided with SET request",
                ))?;

                return Ok(Request::Set {
                    key: Bytes::from(key_str.to_owned()),
                    value: Bytes::from(value_str.to_owned()),
                });
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid command switch",
            )),
        }
    }
}

/*
Response layout schema.
-----------------------------------------------------------------------------
| Message Size (LNG_SEC)| Status | Value Size (Optional) | Value (Optional) |
-----------------------------------------------------------------------------
|          2B           |   1B   |          2B           |       ...        |
-----------------------------------------------------------------------------
*/
impl Encoder<Response> for ServerMessenger {
    type Error = std::io::Error;

    /// Encodes Response on the server side. It does add total message length in the beginning
    /// of the message. Length does not include itself (2B).
    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Response::Ok => {
                dst.put_u16(1);
                dst.put_u8(Status::Ok.as_byte());

                Ok(())
            }
            Response::OkValue { value } => {
                let len = 1 + 2 + value.len();

                dst.put_u16(len as u16);
                dst.put_u8(Status::OkValue.as_byte());
                dst.put_u16(value.len() as u16);
                dst.put_slice(&value);

                Ok(())
            }
            Response::Error { message } => {
                let len = 1 + 2 + message.len();

                dst.put_u16(len as u16);
                dst.put_u8(Status::Error.as_byte());
                dst.put_u16(message.len() as u16);
                dst.put_slice(&message);

                Ok(())
            }
        }
    }
}

/*
Response message layout schema.
-----------------------------------------------------------------------------
| Message Size (LNG_SEC)| Status | Value Size (Optional) | Value (Optional) |
-----------------------------------------------------------------------------
|          2B           |   1B   |          2B           |       ...        |
-----------------------------------------------------------------------------
*/
impl Decoder for ClientMessenger {
    type Item = Response;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < LNG_SEC + 1 {
            // Not enough data to read length marker.
            return Ok(None);
        }

        // Read length marker.
        let mut length_bytes = [0u8; LNG_SEC];
        length_bytes.copy_from_slice(&src[..LNG_SEC]);
        let length = u16::from_le_bytes(length_bytes) as usize;

        // Check that the length is not too large to avoid a denial of
        // service attack where the server runs out of memory.
        if length > CODEC_BUFFER_MAX {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("frame of length {} is too large", length),
            ));
        }

        if src.len() < LNG_SEC + length {
            // Frame has not yet arrived.
            // We reserve more space in the buffer. This is not strictly
            // necessary, but is a good idea performance-wise.
            src.reserve(LNG_SEC + length - src.len());

            // We inform the Framed that we need more bytes to form the next frame.
            return Ok(None);
        }

        // Use advance to modify src such that it no longer contains this frame.
        // TODO: It should not be necessary here to make slice owned to pass to parse function.
        let data = src[LNG_SEC..LNG_SEC + length].to_vec();
        src.advance(LNG_SEC + length);

        // Parse the payload.
        match Response::parse(data.as_ref()) {
            Ok(response) => Ok(Some(response)),
            Err(error) => Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error)),
        }
    }
}

/*
Response layout schema.
-----------------------------------------------------
| Status | Value Size (Optional) | Value (Optional) |
-----------------------------------------------------
|   1B   |          2B           |       ...        |
-----------------------------------------------------
*/
impl Response {
    fn parse(raw: &[u8]) -> crate::Result<Response> {
        if raw.is_empty() {
            return Err("message invalid".into());
        }

        let mut buf = Cursor::new(raw);

        let status = Status::from_byte(buf.get_u8());
        match status {
            Status::Ok => Ok(Response::Ok),
            Status::OkValue => {
                let value_size = buf.get_u16();
                let value = buf.copy_to_bytes(value_size as usize);
                Ok(Response::OkValue { value })
            }
            Status::Error => {
                let message_size = buf.get_u16();
                let message = buf.copy_to_bytes(message_size as usize);
                Ok(Response::Error { message })
            }
            _ => Err("unknown command".into()),
        }
    }
}

/*
Request layout schema.
--------------------------------------------------------------------------------------------
| Message Size (LNG_SEC)| Mode | Key Size | Key | Value Size (Optional) | Value (Optional) |
--------------------------------------------------------------------------------------------
|          2B           |  1B  |    2B    | ... |          2B           |       ...        |
--------------------------------------------------------------------------------------------
*/
impl Encoder<Request> for ClientMessenger {
    type Error = std::io::Error;

    /// Encodes Request on the client side. It does add total message length in the beginning
    /// of the message. Length does not include itself (2B).
    fn encode(&mut self, item: Request, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Request::Set { key, value } => {
                let len = 1 + 2 + key.len() + 2 + value.len();

                dst.put_u16(len as u16);
                dst.put_u8(Mode::Set.as_byte());
                dst.put_u16(key.len() as u16);
                dst.put_slice(&key);
                dst.put_u16(value.len() as u16);
                dst.put_slice(&value);

                Ok(())
            }
            Request::Get { key } => {
                let len = 1 + 2 + key.len();

                dst.put_u16(len as u16);
                dst.put_u8(Mode::Get.as_byte());
                dst.put_u16(key.len() as u16);
                dst.put_slice(&key);

                Ok(())
            }
        }
    }
}

impl fmt::Display for Request {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Request::Set { key, value } => write!(
                f,
                "SET {} = {}",
                String::from_utf8_lossy(key),
                String::from_utf8_lossy(value),
            ),
            Request::Get { key } => write!(f, "GET {}", String::from_utf8_lossy(key)),
        }
    }
}

impl fmt::Display for Response {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Response::Ok => write!(f, "OK"),
            Response::OkValue { value } => write!(f, "OK {}", String::from_utf8_lossy(value)),
            Response::Error { message } => {
                write!(
                    f,
                    "ERR {}",
                    String::from_utf8(message.to_vec())
                        .unwrap_or("could not decode error message".to_owned())
                )
            }
        }
    }
}
