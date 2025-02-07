use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::Cursor;
use std::{fmt, io};
use tokio_util::codec::{Decoder, Encoder};

#[cfg(test)]
use strum::EnumIter;

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

/// Request Mode is byte coded command of a request. Tells the server how properly decode the following
/// bytes of request message. Basic commands are get value by given key, set value for a key.
#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[cfg_attr(test, derive(EnumIter))]
enum RequestMode {
    Get = 0x00,
    Set = 0x01,
    Unknown = 0xFF,
}

impl RequestMode {
    pub fn from_byte(byte: u8) -> Self {
        match byte {
            0x00 => RequestMode::Get,
            0x01 => RequestMode::Set,
            _ => RequestMode::Unknown,
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
#[cfg_attr(test, derive(EnumIter))]
enum ResponseStatus {
    Ok = 0x00,
    OkValue = 0x01,
    Error = 0x02,
    Unknown = 0xFF,
}

impl ResponseStatus {
    pub fn from_byte(byte: u8) -> Self {
        match byte {
            0x00 => ResponseStatus::Ok,
            0x01 => ResponseStatus::OkValue,
            0x02 => ResponseStatus::Error,
            _ => ResponseStatus::Unknown,
        }
    }

    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/*
Request message layout schema.
-----------------------------------
| Message Size (LNG_SEC)| Payload |
-----------------------------------
|          2B           |   ...   |
-----------------------------------
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
        let length = u16::from_be_bytes(length_bytes) as usize;

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
Request payload layout schema.
--------------------------------------------------------------------
| Mode | Key Size | Key | Value Size (Optional) | Value (Optional) |
--------------------------------------------------------------------
|  1B  |    2B    | ... |          2B           |       ...        |
--------------------------------------------------------------------
*/
impl Request {
    fn parse(raw: &[u8]) -> crate::Result<Self> {
        if raw.len() < 4 {
            return Err("message invalid".into());
        }

        let mut buf = Cursor::new(raw);

        let cmd_mode = RequestMode::from_byte(buf.get_u8());
        match cmd_mode {
            RequestMode::Get => {
                if buf.remaining() < 3 {
                    // Get request can't be emtpy.
                    return Err("too few bytes provided for get request".into());
                }

                let key_size = buf.get_u16();

                if key_size as usize > buf.remaining() {
                    return Err("not enough bytes to decode key".into());
                }

                if buf.remaining() > key_size as usize {
                    return Err("too much bytes given".into());
                }

                let key = buf.copy_to_bytes(key_size as usize);
                Ok(Request::Get { key })
            }
            RequestMode::Set => {
                if buf.remaining() < 3 {
                    // Set request can't be emtpy.
                    return Err("too few bytes provided for set request".into());
                }

                let key_size = buf.get_u16();

                if key_size as usize > buf.remaining() {
                    return Err("not enough bytes to decode key".into());
                }

                let key = buf.copy_to_bytes(key_size as usize);
                let value_size = buf.get_u16();

                if value_size as usize > buf.remaining() {
                    return Err("not enough bytes to decode value".into());
                }

                if buf.remaining() > value_size as usize {
                    return Err("too much bytes given".into());
                }

                let value = buf.copy_to_bytes(value_size as usize);
                Ok(Request::Set { key, value })
            }
            _ => Err("unknown command".into()),
        }
    }

    /// Handy function to help command line client translate string input into a request message.
    pub fn from_string(input: String) -> std::result::Result<Request, io::Error> {
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

                Ok(Request::Get {
                    key: Bytes::from(key_str.to_owned()),
                })
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

                Ok(Request::Set {
                    key: Bytes::from(key_str.to_owned()),
                    value: Bytes::from(value_str.to_owned()),
                })
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid command switch",
            )),
        }
    }
}

/*
Response message layout schema.
-----------------------------------
| Message Size (LNG_SEC)| Payload |
-----------------------------------
|          2B           |   ...   |
-----------------------------------
*/
impl Encoder<Response> for ServerMessenger {
    type Error = std::io::Error;

    /// Encodes Response on the server side. It does add total message length in the beginning
    /// of the message. Length does not include itself (2B).
    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Response::Ok => {
                dst.put_u16(1);
                dst.put_u8(ResponseStatus::Ok.as_byte());

                Ok(())
            }
            Response::OkValue { value } => {
                let len = 1 + 2 + value.len();

                dst.put_u16(len as u16);
                dst.put_u8(ResponseStatus::OkValue.as_byte());
                dst.put_u16(value.len() as u16);
                dst.put_slice(&value);

                Ok(())
            }
            Response::Error { message } => {
                let len = 1 + 2 + message.len();

                dst.put_u16(len as u16);
                dst.put_u8(ResponseStatus::Error.as_byte());
                dst.put_u16(message.len() as u16);
                dst.put_slice(&message);

                Ok(())
            }
        }
    }
}

/*
Response message layout schema.
-----------------------------------
| Message Size (LNG_SEC)| Payload |
-----------------------------------
|          2B           |   ...   |
-----------------------------------
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
        let length = u16::from_be_bytes(length_bytes) as usize;

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
Response payload layout schema.
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

        let status = ResponseStatus::from_byte(buf.get_u8());
        match status {
            ResponseStatus::Ok => {
                if buf.remaining() > 0 {
                    return Err("too much bytes given".into());
                }

                Ok(Response::Ok)
            }
            ResponseStatus::OkValue => {
                if buf.remaining() < 3 {
                    // Value cant be empty.
                    return Err("too few bytes provided for ok value response ".into());
                }

                let value_size = buf.get_u16();

                if value_size as usize > buf.remaining() {
                    return Err("not enough bytes to decode value".into());
                }

                if buf.remaining() > value_size as usize {
                    return Err("too much bytes given".into());
                }

                let value = buf.copy_to_bytes(value_size as usize);
                Ok(Response::OkValue { value })
            }
            ResponseStatus::Error => {
                if buf.remaining() < 3 {
                    // Error can't be emtpy.
                    return Err("too few bytes provided for error response".into());
                }

                let message_size = buf.get_u16();

                if message_size as usize > buf.remaining() {
                    return Err("not enough bytes to decode error message".into());
                }

                if buf.remaining() > message_size as usize {
                    return Err("too much bytes given".into());
                }

                let message = buf.copy_to_bytes(message_size as usize);
                Ok(Response::Error { message })
            }
            _ => Err("unknown command".into()),
        }
    }
}

/*
Request message layout schema.
-----------------------------------
| Message Size (LNG_SEC)| Payload |
-----------------------------------
|          2B           |   ...   |
-----------------------------------
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
                dst.put_u8(RequestMode::Set.as_byte());
                dst.put_u16(key.len() as u16);
                dst.put_slice(&key);
                dst.put_u16(value.len() as u16);
                dst.put_slice(&value);

                Ok(())
            }
            Request::Get { key } => {
                let len = 1 + 2 + key.len();

                dst.put_u16(len as u16);
                dst.put_u8(RequestMode::Get.as_byte());
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use strum::IntoEnumIterator;

    #[test]
    fn test_request_mode() {
        let modes: Vec<RequestMode> = RequestMode::iter().collect();
        for mode in modes {
            assert_eq!(RequestMode::from_byte(mode.as_byte()), mode);
        }
    }

    #[test]
    fn test_response_status() {
        let statuses: Vec<ResponseStatus> = ResponseStatus::iter().collect();
        for status in statuses {
            assert_eq!(ResponseStatus::from_byte(status.as_byte()), status);
        }
    }

    #[test]
    fn test_valid_request_parse() {
        let set_request_raw: &[u8] = &[
            RequestMode::Set.as_byte(), // Request mode.
            0,
            5, // Len of the key is 5.
            1,
            2,
            3,
            4,
            5,
            0,
            10, // Len of the value is 10.
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            0,
        ];

        let req = Request::parse(set_request_raw);
        assert!(req.is_ok());
        let req = req.unwrap();
        assert!(matches!(req, Request::Set { .. }));
        if let Request::Set { key, value } = req {
            let expected_key: &[u8] = &[1, 2, 3, 4, 5];
            assert_eq!(key, Bytes::from(expected_key));
            let expected_value: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9, 0];
            assert_eq!(value, Bytes::from(expected_value));
        }

        let get_request_raw: &[u8] = &[
            RequestMode::Get.as_byte(), // Request mode.
            0,
            5, // Len of the key is 5.
            1,
            2,
            3,
            4,
            5,
        ];

        let req = Request::parse(get_request_raw);
        assert!(req.is_ok());
        let req = req.unwrap();
        assert!(matches!(req, Request::Get { .. }));
        if let Request::Get { key } = req {
            let expected_key: &[u8] = &[1, 2, 3, 4, 5];
            assert_eq!(key, Bytes::from(expected_key));
        }
    }

    #[test]
    fn test_invalid_request_parse() {
        let raw: &[u8] = &[19, 1, 2];
        assert!(Request::parse(raw).is_err(), "incorrect request mode byte");

        let raw: &[u8] = &[1];
        assert!(Request::parse(raw).is_err(), "slice is to short");

        let raw: &[u8] = &[];
        assert!(Request::parse(raw).is_err(), "slice is empty");

        let raw: &[u8] = &[RequestMode::Get.as_byte(), 0, 1];
        assert!(Request::parse(raw).is_err(), "request is empty");

        let raw: &[u8] = &[RequestMode::Get.as_byte(), 0, 3, 1, 2];
        assert!(
            Request::parse(raw).is_err(),
            "key is shorter then the given length"
        );

        let raw: &[u8] = &[RequestMode::Get.as_byte(), 0, 3, 1, 2, 3, 4];
        assert!(
            Request::parse(raw).is_err(),
            "key is longer then the given length"
        );

        let raw: &[u8] = &[RequestMode::Set.as_byte(), 0, 1];
        assert!(Request::parse(raw).is_err(), "request is empty");

        let raw: &[u8] = &[RequestMode::Set.as_byte(), 0, 3, 1, 2, 3, 0, 3, 1, 2];
        assert!(
            Request::parse(raw).is_err(),
            "value is shorter then the given length"
        );

        let raw: &[u8] = &[RequestMode::Set.as_byte(), 0, 3, 1, 2, 3, 0, 3, 1, 2, 3, 4];
        assert!(
            Request::parse(raw).is_err(),
            "value is longer then the given length"
        );
    }

    #[test]
    fn test_valid_response_parse() {
        let ok_response_raw: &[u8] = &[ResponseStatus::Ok.as_byte()];

        let resp = Response::parse(ok_response_raw);
        assert!(resp.is_ok());
        let resp = resp.unwrap();
        assert!(matches!(resp, Response::Ok));

        let ok_value_response_raw: &[u8] = &[
            ResponseStatus::OkValue.as_byte(),
            0,
            5, // Len of the value is 5.
            1,
            2,
            3,
            4,
            5,
        ];

        let resp = Response::parse(ok_value_response_raw);
        assert!(resp.is_ok());
        let resp = resp.unwrap();
        assert!(matches!(resp, Response::OkValue { .. }));
        if let Response::OkValue { value } = resp {
            let expected_value: &[u8] = &[1, 2, 3, 4, 5];
            assert_eq!(value, Bytes::from(expected_value));
        }

        let error_response_raw: &[u8] = &[
            ResponseStatus::Error.as_byte(),
            0,
            5, // Len of the message is 5.
            1,
            2,
            3,
            4,
            5,
        ];

        let resp = Response::parse(error_response_raw);
        assert!(resp.is_ok());
        let resp = resp.unwrap();
        assert!(matches!(resp, Response::Error { .. }));
        if let Response::Error { message } = resp {
            let expected_message: &[u8] = &[1, 2, 3, 4, 5];
            assert_eq!(message, Bytes::from(expected_message));
        }
    }

    #[test]
    fn test_invalid_response_parse() {
        let raw: &[u8] = &[19, 1, 2];
        assert!(
            Response::parse(raw).is_err(),
            "incorrect response status byte"
        );

        let raw: &[u8] = &[2];
        assert!(Response::parse(raw).is_err(), "slice is to short");

        let raw: &[u8] = &[];
        assert!(Response::parse(raw).is_err(), "slice is empty");

        let raw: &[u8] = &[ResponseStatus::Ok.as_byte(), 0];
        assert!(
            Response::parse(raw).is_err(),
            "too much bytes given for OK response"
        );

        let raw: &[u8] = &[ResponseStatus::OkValue.as_byte(), 0, 3];
        assert!(Response::parse(raw).is_err(), "value is empty");

        let raw: &[u8] = &[ResponseStatus::OkValue.as_byte(), 0, 3, 1, 2];
        assert!(
            Response::parse(raw).is_err(),
            "value is shorter then the given length"
        );

        let raw: &[u8] = &[ResponseStatus::OkValue.as_byte(), 0, 3, 1, 2, 3, 4];
        assert!(
            Response::parse(raw).is_err(),
            "value is longer then the given length"
        );

        let raw: &[u8] = &[ResponseStatus::Error.as_byte(), 0, 0];
        assert!(Response::parse(raw).is_err(), "message is empty");

        let raw: &[u8] = &[ResponseStatus::Error.as_byte(), 0, 3, 1, 2];
        assert!(
            Response::parse(raw).is_err(),
            "message is shorter then the given length"
        );

        let raw: &[u8] = &[ResponseStatus::Error.as_byte(), 0, 3, 1, 2, 3, 4];
        assert!(
            Response::parse(raw).is_err(),
            "message is longer then the given length"
        );
    }

    #[test]
    fn test_request_from_string() {
        let string = String::from("");
        let req = Request::from_string(string);
        assert!(req.is_err());

        let string = String::from("aaa");
        let req = Request::from_string(string);
        assert!(req.is_err());

        let string = String::from("GET ");
        let req = Request::from_string(string);
        assert!(req.is_err());

        let string = String::from("get asdf");
        let req = Request::from_string(string);
        assert!(req.is_err());

        let string = String::from("SET asdf ");
        let req = Request::from_string(string);
        assert!(req.is_err());

        let string = String::from("SET asdf");
        let req = Request::from_string(string);
        assert!(req.is_err());

        let string = String::from("GET asdf");
        let req = Request::from_string(string);
        assert!(req.is_ok());
        let req = req.unwrap();
        assert!(matches!(req, Request::Get { .. }));
        if let Request::Get { key } = req {
            assert_eq!(key, Bytes::copy_from_slice(&[97, 115, 100, 102]));
        }

        let string = String::from("SET asdf asdf");
        let req = Request::from_string(string);
        assert!(req.is_ok());
        let req = req.unwrap();
        assert!(matches!(req, Request::Set { .. }));
        if let Request::Set { key, value } = req {
            assert_eq!(key, Bytes::copy_from_slice(&[97, 115, 100, 102]));
            assert_eq!(value, Bytes::copy_from_slice(&[97, 115, 100, 102]));
        }
    }

    #[test]
    fn test_response_decoder() {
        // Valid and complete OK response.
        let raw: Bytes = Bytes::copy_from_slice(&[0, 1, ResponseStatus::Ok.as_byte()]);
        let mut src = BytesMut::from(raw);
        let resp = ClientMessenger::default().decode(&mut src);
        assert!(resp.is_ok(), "{:?}", resp.err());
        let resp = resp.unwrap();
        assert!(resp.is_some(), "expected some ok response, got none");
        let resp = resp.unwrap();
        assert!(matches!(resp, Response::Ok));

        // Incomplete OK response.
        let raw: Bytes = Bytes::copy_from_slice(&[0, 1]);
        let mut src = BytesMut::from(raw);
        let resp = ClientMessenger::default().decode(&mut src);
        assert!(resp.is_ok(), "{:?}", resp.err());
        let resp = resp.unwrap();
        assert!(resp.is_none(), "expected incomplete response to be none");

        // Valid and complete OK Value response.
        let raw: Bytes =
            Bytes::copy_from_slice(&[0, 4, ResponseStatus::OkValue.as_byte(), 0, 1, 1]);
        let mut src = BytesMut::from(raw);
        let resp = ClientMessenger::default().decode(&mut src);
        assert!(resp.is_ok(), "{:?}", resp.err());
        let resp = resp.unwrap();
        assert!(resp.is_some(), "expected some ok value response, got none");
        let resp = resp.unwrap();
        assert!(matches!(resp, Response::OkValue { .. }));
        if let Response::OkValue { value } = resp {
            assert_eq!(value, Bytes::copy_from_slice(&[1]));
        }

        // Incomplete OK Value response.
        let raw: Bytes = Bytes::copy_from_slice(&[0, 4, ResponseStatus::OkValue.as_byte(), 0]);
        let mut src = BytesMut::from(raw);
        let resp = ClientMessenger::default().decode(&mut src);
        assert!(resp.is_ok(), "{:?}", resp.err());
        let resp = resp.unwrap();
        assert!(resp.is_none(), "expected incomplete response to be none");

        // Valid and complete Error response.
        let raw: Bytes = Bytes::copy_from_slice(&[0, 4, ResponseStatus::Error.as_byte(), 0, 1, 1]);
        let mut src = BytesMut::from(raw);
        let resp = ClientMessenger::default().decode(&mut src);
        assert!(resp.is_ok(), "{:?}", resp.err());
        let resp = resp.unwrap();
        assert!(resp.is_some(), "expected some error response, got none");
        let resp = resp.unwrap();
        assert!(matches!(resp, Response::Error { .. }));
        if let Response::Error { message } = resp {
            assert_eq!(message, Bytes::copy_from_slice(&[1]));
        }

        // Incomplete Error response.
        let raw: Bytes = Bytes::copy_from_slice(&[0, 4, ResponseStatus::Error.as_byte(), 0]);
        let mut src = BytesMut::from(raw);
        let resp = ClientMessenger::default().decode(&mut src);
        assert!(resp.is_ok(), "{:?}", resp.err());
        let resp = resp.unwrap();
        assert!(resp.is_none(), "expected incomplete response to be none");
    }

    #[test]
    fn test_request_decoder() {
        // Valid and complete get request.
        let raw: Bytes = Bytes::copy_from_slice(&[0, 6, RequestMode::Get.as_byte(), 0, 3, 1, 2, 3]);
        let mut src = BytesMut::from(raw);
        let req = ServerMessenger::default().decode(&mut src);
        assert!(req.is_ok(), "{:?}", req.err());
        let req = req.unwrap();
        assert!(req.is_some(), "expected some get request, got none");
        let req = req.unwrap();
        assert!(matches!(req, Request::Get { .. }));
        if let Request::Get { key } = req {
            assert_eq!(key, Bytes::copy_from_slice(&[1, 2, 3]));
        }

        // Incomplete get request.
        let raw: Bytes = Bytes::copy_from_slice(&[0, 6, RequestMode::Get.as_byte(), 0, 3, 1]);
        let mut src = BytesMut::from(raw);
        let req = ServerMessenger::default().decode(&mut src);
        assert!(req.is_ok(), "{:?}", req.err());
        let req = req.unwrap();
        assert!(req.is_none(), "expected incomplete request to be none");

        // Valid and complete set request.
        let raw: Bytes =
            Bytes::copy_from_slice(&[0, 7, RequestMode::Set.as_byte(), 0, 1, 1, 0, 1, 2]);
        let mut src = BytesMut::from(raw);
        let req = ServerMessenger::default().decode(&mut src);
        assert!(req.is_ok(), "{:?}", req.err());
        let req = req.unwrap();
        assert!(req.is_some(), "expected some set request, got none");
        let req = req.unwrap();
        assert!(matches!(req, Request::Set { .. }));
        if let Request::Set { key, value } = req {
            assert_eq!(key, Bytes::copy_from_slice(&[1]));
            assert_eq!(value, Bytes::copy_from_slice(&[2]));
        }

        // Incomplete set request.
        let raw: Bytes = Bytes::copy_from_slice(&[0, 7, RequestMode::Set.as_byte(), 0, 1, 1, 0]);
        let mut src = BytesMut::from(raw);
        let req = ServerMessenger::default().decode(&mut src);
        assert!(req.is_ok(), "{:?}", req.err());
        let req = req.unwrap();
        assert!(req.is_none(), "expected incomplete request to be none");
    }

    #[test]
    fn test_request_encoder() {
        let req = Request::Get {
            key: Bytes::copy_from_slice(&[1, 2, 3]),
        };
        let mut dst = BytesMut::new();
        let encoded = ClientMessenger::default().encode(req, &mut dst);
        assert!(encoded.is_ok());
        assert_eq!(
            dst,
            BytesMut::from(Bytes::copy_from_slice(&[
                0,
                6,
                RequestMode::Get.as_byte(),
                0,
                3,
                1,
                2,
                3
            ]))
        );

        let req = Request::Set {
            key: Bytes::copy_from_slice(&[1, 2, 3]),
            value: Bytes::copy_from_slice(&[1, 2, 3]),
        };
        let mut dst = BytesMut::new();
        let encoded = ClientMessenger::default().encode(req, &mut dst);
        assert!(encoded.is_ok());
        assert_eq!(
            dst,
            BytesMut::from(Bytes::copy_from_slice(&[
                0,
                11,
                RequestMode::Set.as_byte(),
                0,
                3,
                1,
                2,
                3,
                0,
                3,
                1,
                2,
                3,
            ]))
        );
    }

    #[test]
    fn test_response_encoder() {
        let resp = Response::Ok;
        let mut dst = BytesMut::new();
        let encoded = ServerMessenger::default().encode(resp, &mut dst);
        assert!(encoded.is_ok());
        assert_eq!(
            dst,
            BytesMut::from(Bytes::copy_from_slice(&[
                0,
                1,
                ResponseStatus::Ok.as_byte(),
            ]))
        );

        let resp = Response::OkValue {
            value: Bytes::copy_from_slice(&[1, 2, 3]),
        };
        let mut dst = BytesMut::new();
        let encoded = ServerMessenger::default().encode(resp, &mut dst);
        assert!(encoded.is_ok());
        assert_eq!(
            dst,
            BytesMut::from(Bytes::copy_from_slice(&[
                0,
                6,
                ResponseStatus::OkValue.as_byte(),
                0,
                3,
                1,
                2,
                3
            ]))
        );

        let resp = Response::Error {
            message: Bytes::copy_from_slice(&[1, 2, 3]),
        };
        let mut dst = BytesMut::new();
        let encoded = ServerMessenger::default().encode(resp, &mut dst);
        assert!(encoded.is_ok());
        assert_eq!(
            dst,
            BytesMut::from(Bytes::copy_from_slice(&[
                0,
                6,
                ResponseStatus::Error.as_byte(),
                0,
                3,
                1,
                2,
                3
            ]))
        );
    }
}
