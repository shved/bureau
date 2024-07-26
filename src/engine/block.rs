use bytes::Bytes;

/*
Block layout schema.
----------------------------------------------------------------------------------------------------
|             Data Section             |              Offset Section             |      Extra      |
----------------------------------------------------------------------------------------------------
| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
----------------------------------------------------------------------------------------------------

Single entry layout schema.
----------------------------------------------------------------------------------------------------
|             Data Section             |              Offset Section             |      Extra      |
----------------------------------------------------------------------------------------------------
| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
----------------------------------------------------------------------------------------------------

Borrowed from https://skyzh.github.io/mini-lsm/week1-03-block.html.
*/

/*
Approximate size of block. We can't tell exactly what it will be because putting
a bunch of key value pairs together to fit the exact limit is basically
a knapsack problem (https://en.wikipedia.org/wiki/Knapsack_problem). Thats why
we are good with approximate size here.
*/
const BLOCK_BYTESYZE: usize = 4 * 1024; // 4KB.

const KEYVAL_LEN_BYTESIZE: usize = 2; // 2B key/value len hint.
const BLOCK_ENTRIES_NUM_BYTESIZE: usize = 1; // 1B number of entries in block hint.
const OFFSET_BYTESIZE: usize = 2; // 2B offset hint.

#[derive(Debug)]
pub struct Block {
    checksum: Bytes,
    data: Bytes,
    footer: Bytes,
    len: u16,
}

#[derive(Debug)]
struct Key {
    len: u8,
    key: Bytes,
}

#[derive(Debug)]
struct Value {
    len: u16,
    value: Bytes,
}

pub fn entry_size(key: &Bytes, value: &Bytes) -> usize {
    KEYVAL_LEN_BYTESIZE + &key.len() + KEYVAL_LEN_BYTESIZE + &value.len() + OFFSET_BYTESIZE
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
}
