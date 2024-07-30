use std::collections::btree_map::BTreeMap;

pub struct Builder {}

impl Builder {
    fn new(data: &BtreeMap<Bytes, Bytes>) -> Builder {
        Builder { data }
    }

    fn build(&self) -> SSTable {
        self.data.iter()
    }
}
