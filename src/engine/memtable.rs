use crate::engine::wal::Wal;

use bytes::Bytes;
use std::collections::btree_map::BTreeMap;
use std::error::Error;

#[derive(Debug)]
pub struct MemTable {
    map: BTreeMap<Bytes, Bytes>,
    wal: Wal,
    size: usize,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            map: BTreeMap::new(),
            wal: Wal {}, // TODO: Make use of.
            size: 0,
        }
    }

    pub fn from_wal(wal: Wal) -> MemTable {
        unimplemented!("TODO");
        MemTable {
            map: BTreeMap::new(), // TODO: Fill it in from the WAL.
            wal,
            size: 0, // TODO: Put the actual size here.
        }
    }

    pub fn insert(&mut self, key: Bytes, value: Bytes) {
        self.map.insert(key, value);
    }

    pub fn get(&self, key: Bytes) -> Option<Bytes> {
        self.map.get(&key).map(|e| e.clone())
    }
}
