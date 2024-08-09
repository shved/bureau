use bloomfilter::Bloom;
use bytes::{Buf, BufMut, Bytes};

const DEFAULT_MAX_ELEM: usize = 6500;
const DEFAULT_PROBABILITY: f64 = 0.01;

pub trait BloomSerializable {
    fn encode(&self) -> Vec<u8>;
    fn decode(src: &[u8]) -> Self;
}

/*
Bloom filter layout schema. 7832 Bytes in size.
--------------------------------------------------------------------
| Number of bits | K num |         Sip Keys         |    Bitmap    |
--------------------------------------------------------------------
|      u64       |  u32  | [(u64, u64), (u64, u64)] | 101001010... |
--------------------------------------------------------------------
*/
impl BloomSerializable for Bloom<Bytes> {
    // TODO: Set capasity based on the size used.
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

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

        buf
    }

    fn decode(mut src: &[u8]) -> Self {
        let bitmap_bits = src.get_u64();
        let k_num = src.get_u32();
        let sip_keys = [
            (src.get_u64(), src.get_u64()),
            (src.get_u64(), src.get_u64()),
        ];
        let bytes_should_remain = bitmap_bits / 8 + (bitmap_bits % 8 != 0) as u64;
        assert_eq!(src.remaining() as u64, bytes_should_remain);
        let bitmap = src.copy_to_bytes(src.remaining());
        assert!(!src.has_remaining());

        Self::from_existing(bitmap.as_ref(), bitmap_bits, k_num, sip_keys)
    }
}

// TODO: Make it adjustable.
fn new() -> Bloom<Bytes> {
    Bloom::new_for_fp_rate(DEFAULT_MAX_ELEM, DEFAULT_PROBABILITY)
}
