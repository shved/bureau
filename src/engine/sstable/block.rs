use bytes::{Buf, BufMut, Bytes};
use std::io::Cursor;

/*
Block layout schema.
------------------------------------------------------------------------------------------------------------------
|                  Offsets Section                  |             Data Section             |        Extra        |
------------------------------------------------------------------------------------------------------------------
| Num of offsets (2B) | Offset #1 | ... | Offset #N | Entry #1 | Entry #2 | ... | Entry #N | Block Checksum (4B) |
------------------------------------------------------------------------------------------------------------------

Single entry layout schema.
-----------------------------------------------------
|                  Entry #1                   | ... |
-----------------------------------------------------
| key_len (2B) | key | value_len (2B) | value | ... |
-----------------------------------------------------
*/

/// A block will be always exactly this size for the sake of easy time reading it from disk.
pub const BLOCK_BYTE_SIZE: usize = 4 * 1024; // 4 KB.

/// 2B key/value len hint.
const U16_SIZE: u32 = std::mem::size_of::<u16>() as u32; // 2.

const CHECKSUM_SIZE: usize = std::mem::size_of::<u32>(); // 4.

/// The size of an empty block. Reserved for offsets count and checksum.
const INITIAL_BLOCK_SIZE: u32 = U16_SIZE + CHECKSUM_SIZE as u32;

/// An overhead that a single k/v pair adds to the block.
/// Includes key len flag, value len flag, and a spot in the offsets section.
pub const ENTRY_OVERHEAD: u32 = U16_SIZE * 3;

#[derive(Debug)]
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
    pub first_key: Bytes,
    pub last_key: Bytes,
    size: u32,
}

impl Block {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
            first_key: Bytes::default(),
            last_key: Bytes::default(),
            size: INITIAL_BLOCK_SIZE,
        }
    }

    /// Adds a key/value pair to block and returns true.
    /// If the block is full it does not add it and returns false.
    pub fn add(&mut self, key: Bytes, value: Bytes) -> bool {
        let entry_size = entry_size(&key, &value);

        if self.size + entry_size > BLOCK_BYTE_SIZE as u32 {
            return false;
        }

        self.size += entry_size;

        if self.first_key.is_empty() {
            self.first_key = key.clone();
        }

        // Keep track of the last added key.
        self.last_key = key.clone();

        // Add the offset of the data into the offset array.
        self.offsets.push(self.data.len() as u16);

        // Encode key length.
        self.data.put_u16((key.len()) as u16);
        // Encode key content.
        self.data.put(key);
        // Encode value length.
        self.data.put_u16(value.len() as u16);
        // Encode value content.
        self.data.put(value);

        true
    }

    /// Puts the contents of the block into a sequence of bytes.
    /// Schema that is used can be found on top of the mod source code.
    pub fn encode(&self) -> Vec<u8> {
        assert!(!self.is_empty(), "Attempt to encode an empty block");
        let mut buf = Vec::with_capacity(BLOCK_BYTE_SIZE);

        buf.put_u16(self.offsets.len() as u16);
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.extend(&self.data);

        // Fill the vector up to its capacity (leaving the space required for checksum).
        if buf.len() != buf.capacity() - CHECKSUM_SIZE {
            buf.extend((buf.len()..buf.capacity() - CHECKSUM_SIZE).map(|_| 0));
        }

        let checksum = crc32fast::hash(&buf[..buf.capacity() - CHECKSUM_SIZE]);
        buf.put_u32(checksum);

        assert_eq!(
            buf.len(),
            BLOCK_BYTE_SIZE,
            "Block encoded exceeds the block byte size"
        );

        assert_eq!(
            buf.capacity(),
            BLOCK_BYTE_SIZE,
            "Block encoded exceeds the block byte size"
        );

        buf
    }

    pub fn decode(raw: &[u8]) -> Self {
        assert_eq!(
            raw.len(),
            BLOCK_BYTE_SIZE,
            "Byte slice to decode a block exceeds the block size"
        );

        let mut buf = Cursor::new(raw);

        let checksum = crc32fast::hash(&raw[..buf.remaining() - CHECKSUM_SIZE]);
        let offsets_cnt = buf.get_u16();
        let mut offsets = Vec::with_capacity(offsets_cnt as usize * std::mem::size_of::<u16>());
        for _ in 0..offsets_cnt {
            offsets.push(buf.get_u16());
        }

        let data_start = buf.position() as usize;
        let data_end = data_start + buf.remaining() - CHECKSUM_SIZE;
        let data_len = data_end - data_start;
        let data: Vec<u8> = raw[data_start..data_end].to_vec();
        buf.advance(data_len);

        assert_eq!(buf.get_u32(), checksum, "Checksum mismatch in block decode");

        Self {
            data,
            offsets,
            first_key: Bytes::default(), // Field used while decoding the SsTable.
            last_key: Bytes::default(),  // Field used while decoding the SsTable.
            size: 0,                     // The field only used should not be used on decoded block.
        }
    }

    pub fn get(&self, key: Bytes) -> Option<Bytes> {
        assert!(!self.is_empty(), "Attempt to get value from an empty block");

        let mut low = 0;
        let mut high = self.offsets.len() - 1;

        while low <= high {
            let mid = low + (high - low) / 2;

            let read_key = self.parse_frame(self.offsets[mid] as usize);

            match read_key.cmp(&key) {
                std::cmp::Ordering::Less => low = mid + 1,
                std::cmp::Ordering::Greater => high = mid - 1,
                std::cmp::Ordering::Equal => {
                    return Some(self.parse_frame(self.offsets[mid] as usize + 2 + key.len()))
                }
            }
        }

        None
    }

    fn parse_frame(&self, offset: usize) -> Bytes {
        let mut len_bytes: [u8; 2] = [0, 0];
        len_bytes.copy_from_slice(&self.data[offset..offset + 2]);
        let len = u16::from_be_bytes(len_bytes) as usize;
        Bytes::copy_from_slice(&self.data[offset + 2..offset + 2 + len])
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.len() < 1
    }
}

impl std::fmt::Display for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        let mut keys = Vec::<String>::new();
        for offset in self.offsets.clone() {
            let frame = self.parse_frame(offset as usize);
            keys.push(String::from_utf8_lossy(&frame).into_owned());
        }

        write!(f, "block keys: {:?}", keys)
    }
}

pub fn entry_size(key: &Bytes, value: &Bytes) -> u32 {
    key.len() as u32 + value.len() as u32 + ENTRY_OVERHEAD
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn make_full_block() -> Block {
        let mut bl = Block::new();
        loop {
            if !bl.add(
                Bytes::from(Uuid::now_v7().to_string()),
                Bytes::from(Uuid::now_v7().to_string()),
            ) {
                break;
            }
        }
        bl
    }

    #[test]
    fn test_entry_size() {
        assert_eq!(entry_size(&Bytes::from("foo"), &Bytes::from("bar")), 12);
    }

    #[test]
    fn test_add() {
        let mut bl = Block::new();
        let entry = (Bytes::from("foo"), Bytes::from("bar"));
        bl.add(entry.0.clone(), entry.1.clone());
        assert_eq!(bl.size, INITIAL_BLOCK_SIZE + entry_size(&entry.0, &entry.1));
    }

    #[test]
    fn test_get() {
        let mut bl = Block::new();
        bl.add(Bytes::from("buddha"), Bytes::from("om"));
        bl.add(Bytes::from("dharma"), Bytes::from("ah"));
        bl.add(Bytes::from("sangha"), Bytes::from("hum"));

        let value = bl.get(Bytes::from("buddha"));
        assert!(value.is_some());
        assert_eq!(value.unwrap(), Bytes::from("om"));

        let value = bl.get(Bytes::from("dharma"));
        assert!(value.is_some());
        assert_eq!(value.unwrap(), Bytes::from("ah"));

        let value = bl.get(Bytes::from("sangha"));
        assert!(value.is_some());
        assert_eq!(value.unwrap(), Bytes::from("hum"));

        let value = bl.get(Bytes::from("grief"));
        assert!(value.is_none());
    }

    #[test]
    fn test_parse_frame() {
        let mut bl = Block::new();
        bl.add(Bytes::from("foo"), Bytes::from("bar"));
        bl.add(Bytes::from("bar"), Bytes::from("foo"));

        let key_1 = bl.parse_frame(0);
        assert_eq!(key_1, Bytes::from("foo"));
        let value_1 = bl.parse_frame(5);
        assert_eq!(value_1, Bytes::from("bar"));
        let key_2 = bl.parse_frame(10);
        assert_eq!(key_2, Bytes::from("bar"));
        let value_2 = bl.parse_frame(15);
        assert_eq!(value_2, Bytes::from("foo"));
    }

    #[test]
    fn test_is_empty() {
        let mut bl = Block::new();
        assert!(bl.is_empty());

        bl.add(Bytes::from("buddha"), Bytes::from("om"));
        bl.add(Bytes::from("dharma"), Bytes::from("ah"));
        bl.add(Bytes::from("sangha"), Bytes::from("hum"));
        assert!(!bl.is_empty());
    }

    #[test]
    #[should_panic]
    fn test_encode_empty_table_panics() {
        let bl = Block::new();
        bl.encode();
    }

    #[test]
    fn test_encode() {
        let bl = make_full_block();
        let encoded = bl.encode();
        let mut encoded = Cursor::new(encoded);
        assert_eq!(encoded.remaining(), 4 * 1024);

        let offsets_cnt = encoded.get_u16();
        assert_eq!(offsets_cnt, 52);
    }

    #[test]
    fn test_decode() {
        let bl = make_full_block();
        let encoded = bl.encode();
        let decoded = Block::decode(encoded.as_ref());
        assert_eq!(decoded.first_key, Bytes::default());
        assert_eq!(decoded.last_key, Bytes::default());
        assert_eq!(decoded.data.len(), 3986);
        assert_eq!(decoded.offsets.len(), 52);
        assert_eq!(decoded.size, 0);
        let first_frame = decoded.parse_frame(0);
        assert_eq!(first_frame.len(), 36);
    }
}
