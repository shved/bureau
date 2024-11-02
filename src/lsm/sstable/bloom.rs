use bloomfilter::Bloom;
use bytes::{Buf, BufMut, Bytes};
use std::io::Cursor;

pub const MAX_ELEM: usize = 6400;
pub const PROBABILITY: f64 = 0.01;
pub const CHECKSUM_SIZE: usize = 4; // 4B.
pub const BLOOM_SIZE: usize = 7713; // 7713B.
pub const ENCODED_LEN: usize = BLOOM_SIZE + CHECKSUM_SIZE; // 7717B.

pub trait BloomSerializable {
    fn encode(&self) -> Vec<u8>;
    fn decode(src: &[u8]) -> Self;
}

/*
Bloom filter layout schema.
----------------------------------------------------------------------------------
| Number of bits |  K num   |         Sip Keys         |    Bitmap    | Checksum |
----------------------------------------------------------------------------------
|      u64       | u32 (4B) | [(u64, u64), (u64, u64)] | 101001010... | u32 (4B) |
----------------------------------------------------------------------------------
*/
impl BloomSerializable for Bloom<Bytes> {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BLOOM_SIZE + CHECKSUM_SIZE);

        // Bitmap bits parameter. Number of bits.
        buf.put_u64(self.number_of_bits());

        // K num.
        buf.put_u32(self.number_of_hash_functions());

        // Sip keys.
        let sip_keys = self.sip_keys();
        buf.put_u64(sip_keys[0].0);
        buf.put_u64(sip_keys[0].1);
        buf.put_u64(sip_keys[1].0);
        buf.put_u64(sip_keys[1].1);

        // Bitmap itself.
        buf.put(self.bitmap().as_ref());

        let checksum = crc32fast::hash(&buf[..buf.capacity() - CHECKSUM_SIZE]);
        buf.put_u32(checksum);

        assert_eq!(buf.capacity(), BLOOM_SIZE + CHECKSUM_SIZE);

        buf
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

        let mut buf = Cursor::new(raw);
        let checksum = crc32fast::hash(&raw[..BLOOM_SIZE]);

        let bitmap_bits = buf.get_u64();
        let k_num = buf.get_u32();
        let sip_keys = [
            (buf.get_u64(), buf.get_u64()),
            (buf.get_u64(), buf.get_u64()),
        ];
        let bitmap = buf.copy_to_bytes(buf.remaining() - CHECKSUM_SIZE);

        let checksum_decoded = buf.get_u32();

        assert_eq!(
            checksum, checksum_decoded,
            "Checksum mismatch in bloom filter decode"
        );

        Self::from_existing(bitmap.as_ref(), bitmap_bits, k_num, sip_keys)
    }
}

pub fn new() -> Bloom<Bytes> {
    Bloom::new_for_fp_rate(MAX_ELEM, PROBABILITY)
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
        assert_eq!(decoded.bit_vec(), original.bit_vec());
        assert!(decoded.check(&Bytes::from("foo")));
        assert!(decoded.check(&Bytes::from("bar")));
    }
}
