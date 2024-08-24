use bloomfilter::Bloom;
use bytes::{Buf, BufMut, Bytes};

pub const MAX_ELEM: usize = 6400;
pub const PROBABILITY: f64 = 0.01;
pub const CHECKSUM_SIZE: usize = 4; // 4B.
pub const BLOOM_SIZE: usize = 7713; // 7713B.

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

        buf
    }

    // TODO: Remove panics, return Result.
    fn decode(mut raw: &[u8]) -> Self {
        let checksum = crc32fast::hash(&raw[..raw.remaining() - CHECKSUM_SIZE]);

        let bitmap_bits = raw.get_u64();
        let k_num = raw.get_u32();
        let sip_keys = [
            (raw.get_u64(), raw.get_u64()),
            (raw.get_u64(), raw.get_u64()),
        ];
        let bitmap = raw.copy_to_bytes(raw.remaining() - CHECKSUM_SIZE);

        let checksum_decoded = raw.get_u32();

        if checksum != checksum_decoded {
            panic!("Checksum mismatch in bloom filter decode")
        }
        assert!(!raw.has_remaining());

        Self::from_existing(bitmap.as_ref(), bitmap_bits, k_num, sip_keys)
    }
}

fn new() -> Bloom<Bytes> {
    Bloom::new_for_fp_rate(MAX_ELEM, PROBABILITY)
}
