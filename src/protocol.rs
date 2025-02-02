use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io::Cursor;
use tokio_util::codec::{Decoder, Encoder};

const CODEC_BUFFER_MAX: usize = 4 * 1024; // 4B.
const LNG_SEC: usize = 2; // 2B.
const ERR_MODE: u8 = 0xFF;

#[derive(Debug, Default)]
pub struct Messager {}

/// Request commands supported.
#[derive(Debug)]
pub enum Request {
    Get { key: Bytes },
    Set { key: Bytes, value: Bytes },
}

/// Possible responses structures.
#[derive(Debug)]
pub enum Response {
    Get { key: Bytes, value: Bytes },
    Set { key: Bytes, value: Bytes },
    Error { msg: Bytes },
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum CommandMode {
    Get = 0x00,
    Set = 0x01,
    Unknown = 0xFF,
}

impl CommandMode {
    pub fn from_byte(byte: u8) -> Self {
        match byte {
            0x00 => CommandMode::Get,
            0x01 => CommandMode::Set,
            _ => CommandMode::Unknown,
        }
    }

    pub fn as_byte(self) -> u8 {
        self as u8
    }
}

/*
Request layout schema.
-----------------------------------------------------------------------
| Command | Key Size | Key | Value Size (Optional) | Value (Optional) |
-----------------------------------------------------------------------
|    1B   |    2B    | ... |          2B           |       ...        |
-----------------------------------------------------------------------
*/
impl Request {
    fn parse(raw: &[u8]) -> crate::Result<Request> {
        if raw.len() < 4 {
            return Err("message invalid".into());
        }

        let mut buf = Cursor::new(raw);

        let cmd_mode = CommandMode::from_byte(buf.get_u8());
        match cmd_mode {
            CommandMode::Get => {
                let key_size = buf.get_u16();
                let key = buf.copy_to_bytes(key_size as usize);
                Ok(Request::Get { key })
            }
            CommandMode::Set => {
                let key_size = buf.get_u16();
                let key = buf.copy_to_bytes(key_size as usize);
                let value_size = buf.get_u16();
                let value = buf.copy_to_bytes(value_size as usize);
                Ok(Request::Set { key, value })
            }
            _ => Err("unknown command".into()),
        }
    }
}

/*
Request message layout schema.
-----------------------------------------------------------------------------------------------
| Message Size (LNG_SEC)| Command | Key Size | Key | Value Size (Optional) | Value (Optional) |
-----------------------------------------------------------------------------------------------
|          2B           |    1B   |    2B    | ... |          2B           |       ...        |
-----------------------------------------------------------------------------------------------
*/
impl Decoder for Messager {
    type Item = Request;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < 2 {
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

impl Encoder<Response> for Messager {
    type Error = std::io::Error;

    fn encode(&mut self, item: Response, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            Response::Get { key, value } => {
                dst.put_u8(CommandMode::Get.as_byte());
                dst.put_u16(key.len() as u16);
                dst.put_slice(&key);
                dst.put_u16(value.len() as u16);
                dst.put_slice(&value);

                Ok(())
            }
            Response::Set { key, value } => {
                dst.put_u8(CommandMode::Set.as_byte());
                dst.put_u16(key.len() as u16);
                dst.put_slice(&key);
                dst.put_u16(value.len() as u16);
                dst.put_slice(&value);

                Ok(())
            }
            Response::Error { msg } => {
                dst.put_u8(ERR_MODE);
                dst.put_u16(msg.len() as u16);
                dst.put_slice(&msg);

                Ok(())
            }
        }
    }
}
