use bytes::{Buf, BufMut, Bytes};

/*
Block layout schema.
--------------------------------------------------------------------------------------------------------------
|                Offset Section                |             Data Section             |         Extra        |
--------------------------------------------------------------------------------------------------------------
| Num of offsets | Offset #1 | ... | Offset #N | Entry #1 | Entry #2 | ... | Entry #N | Block Checksum (u32) |
--------------------------------------------------------------------------------------------------------------

Single entry layout schema.
-----------------------------------------------------
|                  Entry #1                   | ... |
-----------------------------------------------------
| key_len (2B) | key | value_len (2B) | value | ... |
-----------------------------------------------------
*/

/// A block will be always exactly this size for the sake of easy time reading it from disk.
pub const BLOCK_BYTESIZE: usize = 4 * 1024; // 4 KB.

/// 2B key/value len hint.
const U16_SIZE: u32 = std::mem::size_of::<u16>() as u32; // 2.

const CHECKSUM_SIZE: usize = std::mem::size_of::<u32>(); // 4.

/// The size of an empty block. Reserved for offsets count and checksum.
const INITIAL_BLOCK_SIZE: u32 = U16_SIZE + CHECKSUM_SIZE as u32;

/// An overhead that a single k/v pair adds to the block.
/// Includes key len flag, value len flag, and a spot in the offsets section.
pub const SINGLE_UNIT_OVERHEAD: u32 = U16_SIZE * 3;

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
        let entry_size = Entry::size(&key, &value);

        if self.size + entry_size > BLOCK_BYTESIZE as u32 && !self.is_empty() {
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
    pub fn encode(&self) -> Bytes {
        assert!(!self.is_empty(), "Attempt to encode an empty block");
        let mut buf = Vec::with_capacity(BLOCK_BYTESIZE);

        buf.put_u16(self.offsets.len() as u16);
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.extend(&self.data);

        // Fill the vector up to its capacity (leaving the space required for checksum).
        if buf.len() != buf.capacity() - CHECKSUM_SIZE {
            buf.extend((buf.len()..buf.capacity() - CHECKSUM_SIZE).map(|_| 0));
        }

        let checksum = crc32fast::hash(&buf);
        buf.put_u32(checksum);

        assert!(
            buf.len() == BLOCK_BYTESIZE,
            "Block encoded exceeds the block bytesize"
        );

        buf.into()
    }

    pub fn decode(mut raw: &[u8]) -> Self {
        assert!(
            raw.len() == BLOCK_BYTESIZE,
            "Byte slice to decode a block exceeds the block size"
        );

        let checksum = crc32fast::hash(&raw[..raw.remaining() - CHECKSUM_SIZE]);
        let offsets_num = raw.get_u16();
        let mut offsets = Vec::with_capacity(offsets_num as usize * 2);
        for _ in 0..offsets_num {
            offsets.push(raw.get_u16());
        }

        let mut data = Vec::with_capacity(raw.len() - CHECKSUM_SIZE);
        data.put(raw[..].take(raw.len() - CHECKSUM_SIZE));

        assert!(
            raw.get_u32() == checksum,
            "Checksum mismatch in block decode"
        );

        Self {
            data,
            offsets,
            first_key: Bytes::default(),
            last_key: Bytes::default(),
            size: 0, // The field should not be used on decoded block.
        }
    }

    pub fn get(&self, key: Bytes) -> Option<Bytes> {
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

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[derive(Debug)]
pub struct Entry {
    _key: Bytes,
    _value: Bytes,
}

impl Entry {
    pub fn size(key: &Bytes, value: &Bytes) -> u32 {
        key.len() as u32 + value.len() as u32 + SINGLE_UNIT_OVERHEAD
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_size() {
        assert_eq!(
            super::Entry::size(&Bytes::from("foo"), &Bytes::from("bar")),
            12
        );
    }

    #[test]
    fn encode() {
        // todo!("test encode()")
    }

    #[test]
    fn decode() {
        // todo!("test decode()")
    }
}
