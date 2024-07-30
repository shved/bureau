use bytes::{Buf, BufMut, Bytes};

/*
Block layout schema.
----------------------------------------------------------------------------------------------------
|             Data Section             |              Offset Section             |      Extra      |
----------------------------------------------------------------------------------------------------
| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
----------------------------------------------------------------------------------------------------

Single entry layout schema.
-----------------------------------------------------------------------
|                           Entry #1                            | ... |
-----------------------------------------------------------------------
| key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
-----------------------------------------------------------------------

Borrowed from https://skyzh.github.io/mini-lsm/week1-03-block.html.
*/

/*
Approximate size of block. We can't tell exactly what it will be because putting
a bunch of key value pairs together to fit the exact limit is basically
a knapsack problem (https://en.wikipedia.org/wiki/Knapsack_problem). Thats why
we are good with approximate size here.
*/
const BLOCK_BYTESYZE: u16 = 4 * 1024; // 4KB.

const U16_SIZE: usize = 2; // 2B key/value len hint.

// An overhead that a single k/v pair adds to the block.
// Includes key len flag, value len flag, and a spot in the offsets section.
const SINGLE_UNIT_OVERHEAD: usize = U16_SIZE * 3;

#[derive(Debug)]
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
    first_key: Vec<u8>,
}

pub fn entry_size(key: &Bytes, value: &Bytes) -> usize {
    &key.len() + &value.len() + SINGLE_UNIT_OVERHEAD
}

impl Block {
    pub fn new() -> Self {
        Self {
            data: Vec::new(),
            offsets: Vec::new(),
            first_key: Vec::new(),
        }
    }

    // Adds a key/value pair to block and returns true. If the block is full it does not add it
    // and returns false.
    pub fn add(&mut self, key: Bytes, value: Bytes) -> bool {
        // assert!(!key.is_empty(), "key must not be empty");

        // TODO: Probably better to have a counter on block? Extract this to a block builder with extra state?
        if self.calc_size() + entry_size(&key, &value) > BLOCK_BYTESYZE as usize && !self.is_empty()
        {
            return false;
        }

        if self.first_key.is_empty() {
            self.first_key = key.to_vec();
        }

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

    pub fn encode(&self) -> Bytes {
        assert!(!self.is_empty());
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        // Put number of elements as a block footer.
        buf.put_u16(offsets_len as u16);
        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        // Get number of elements in the block.
        let entry_offsets_len = (&data[data.len() - U16_SIZE..]).get_u16() as usize;
        let data_end = data.len() - U16_SIZE - entry_offsets_len * U16_SIZE;
        let offsets_raw = &data[data_end..data.len() - U16_SIZE];
        // Get offsets array.
        let offsets = offsets_raw
            .chunks(U16_SIZE)
            .map(|mut x| x.get_u16())
            .collect();
        // Retrieve data.
        let data = data[0..data_end].to_vec();
        Self {
            data,
            offsets,
            first_key: Vec::new(), // TODO: May be extract the first key here.
        }
    }

    fn calc_size(&self) -> usize {
        self.data.len() // Data section size.
            + self.offsets.len() * U16_SIZE // Offsets size.
            + U16_SIZE // Number of elements.
    }

    fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_size() {
        assert_eq!(
            super::entry_size(&Bytes::from("foo"), &Bytes::from("bar")),
            12
        );
    }

    #[test]
    fn encode() {
        todo!("test encode()")
    }

    #[test]
    fn decode() {
        todo!("test decode()")
    }
}
