use bloomfilter::Bloom;
use bytes::{Buf, BufMut, Bytes};
use std::io::Cursor;

pub const MAX_ELEM: usize = 6400;
pub const PROBABILITY: f64 = 0.01;
pub const BLOOM_SIZE: usize = 7714; // 7714B.
pub const CHECKSUM_SIZE: usize = 4; // 4B.
pub const ENCODED_LEN: usize = BLOOM_SIZE + CHECKSUM_SIZE; // 7718B.

pub trait BloomSerializable {
    fn encode(&self) -> Vec<u8>;
    fn decode(src: &[u8]) -> Self;
}

/*
Bloom filter layout schema.
----------------------------------------------
| Bloomfilter serialized to bytes | Checksum |
----------------------------------------------
|              7714B              | u32 (4B) |
----------------------------------------------
*/
impl BloomSerializable for Bloom<Bytes> {
    fn encode(&self) -> Vec<u8> {
        let mut encoded = self.to_bytes();

        let checksum = crc32fast::hash(&encoded);
        encoded.put_u32(checksum);

        assert_eq!(encoded.len(), BLOOM_SIZE + CHECKSUM_SIZE);

        encoded
    }

    // TODO: Remove panics, return Result.
    fn decode(raw: &[u8]) -> Self {
        assert_eq!(
            raw.len(),
            ENCODED_LEN,
            "Blob should be {} bytes, but {} was passed",
            ENCODED_LEN,
            raw.len()
        );

        let checksum = crc32fast::hash(&raw[..BLOOM_SIZE]);

        let mut vec = Vec::from(raw);

        let sum_vec: Vec<u8> = vec.drain(BLOOM_SIZE..).collect();
        let sum_decoded = Cursor::new(sum_vec).get_u32();

        let decoded = Bloom::<Bytes>::from_bytes(vec).unwrap();

        assert_eq!(
            checksum, sum_decoded,
            "Checksum mismatch in bloom filter decode"
        );

        decoded
    }
}

pub fn new() -> Bloom<Bytes> {
    Bloom::new_for_fp_rate(MAX_ELEM, PROBABILITY).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode() {
        let mut original = new();
        original.set(&Bytes::from("foo"));
        original.set(&Bytes::from("bar"));

        let encoded = original.encode();
        assert_eq!(encoded.len(), ENCODED_LEN);

        let decoded = Bloom::decode(encoded.as_slice());
        assert!(decoded.check(&Bytes::from("foo")));
        assert!(decoded.check(&Bytes::from("bar")));
    }
}
