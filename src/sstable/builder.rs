use std::collections::btree_map::BTreeMap;

pub struct Builder {
    // builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub meta: Vec<BlockMeta>,
    block_size: usize,
    key_hashes: Vec<u32>,
}

impl Builder {
    fn new(data: &BtreeMap<Bytes, Bytes>) -> Builder {
        Builder { data }
    }

    fn build(&self) -> SSTable {
        self.data.iter()
    }
}
