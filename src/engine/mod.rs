mod block;
mod compaction;
mod index;
mod memtable;
mod sstable;
mod wal;

use bytes::Bytes;

#[derive(Debug)]
pub struct Engine {
    memtable: memtable::MemTable,
    shadow_table: memtable::MemTable,
    index: index::Index,
}

impl Engine {
    pub fn new() -> Engine {
        Engine {
            memtable: memtable::MemTable::new(),
            shadow_table: memtable::MemTable::new(),
            index: index::Index::new(),
        }
    }

    pub fn insert(&mut self, key: Bytes, value: Bytes) {
        match self.memtable.insert(key, value) {
            memtable::InsertResult::Full => self.swap_tables(),
            _ => (),
        }
    }

    pub fn get(&self, key: Bytes) -> Option<Bytes> {
        self.memtable.get(key)
    }

    // Swap memtable and shadow table to data while sstable is building.
    fn swap_tables(&mut self) {
        unimplemented!("TODO")
    }
}
