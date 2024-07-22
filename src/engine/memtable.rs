use crate::engine::block;
use crate::engine::wal::Wal;

use bytes::Bytes;
use std::collections::btree_map::BTreeMap;

/*
If memtable size reaches SSTABLE_BYTESYZE - RESERVE, we make an sstable
right away because next key-value pair will most likely overflow the block size.
TODO: adjust RESERVE value based on the statistics unit.
*/
const RESERVE: usize = 64;
const SSTABLE_BYTESIZE: usize = 64 * 1024; // 64KB (16 blocks).

#[derive(Debug)]
pub struct MemTable {
    map: BTreeMap<Bytes, Bytes>,
    wal: Wal,
    size: usize,
}

pub enum InsertResult {
    Full,
    Available,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            map: BTreeMap::new(),
            wal: Wal {},
            size: 0,
        }
    }

    pub fn from_wal(wal: Wal) -> MemTable {
        unimplemented!("TODO");
    }

    pub fn insert(&mut self, key: Bytes, value: Bytes) -> InsertResult {
        self.update_size(&key, &value);
        self.map.insert(key, value);
        if self.full() {
            return InsertResult::Full;
        }

        InsertResult::Available
    }

    pub fn get(&self, key: Bytes) -> Option<Bytes> {
        self.map.get(&key).map(|e| e.clone())
    }

    fn update_size(&mut self, key: &Bytes, value: &Bytes) {
        // First, check if the key is already there.
        let mut old_entry_size: usize = 0;
        if self.map.contains_key(key) {
            // It is fine to get value here since writes are syncronized via channel.
            let old_value = self.map.get(key).unwrap(); // unwrap() is fine here.
            old_entry_size = block::entry_size(key, &old_value);
        }

        let entry_size = block::entry_size(key, value);

        self.size = self.size - old_entry_size + entry_size;
    }

    fn full(&self) -> bool {
        if self.size > (SSTABLE_BYTESIZE - RESERVE) {
            return true;
        }

        false
    }
}
