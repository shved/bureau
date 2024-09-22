use crate::lsm::sstable::block;
use crate::lsm::wal::Wal;
use bytes::Bytes;
use std::collections::btree_map::BTreeMap;

const SSTABLE_BYTESIZE: u32 = 64 * 1024; // 64KB (16 blocks).

#[derive(Debug, Clone)]
pub struct MemTable {
    pub map: BTreeMap<Bytes, Bytes>,
    size: u32,
}

#[derive(Debug)]
pub enum InsertResult {
    Full,
    Available,
}

impl MemTable {
    pub fn new() -> MemTable {
        MemTable {
            map: BTreeMap::new(),
            size: 0,
        }
    }

    pub fn from_wal(_wal: Wal) -> MemTable {
        todo!();
    }

    // TODO: Introduce fn probe_size to check if key will overflow and preemptively
    // switch memtables. Key/value will go to the fresh empty table.
    pub fn insert(&mut self, key: Bytes, value: Bytes) -> InsertResult {
        self.update_size(&key, &value);
        self.map.insert(key, value);
        if self.is_full() {
            return InsertResult::Full;
        }

        InsertResult::Available
    }

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.map.get(key).cloned()
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.size = 0;
    }

    fn update_size(&mut self, key: &Bytes, value: &Bytes) {
        // First, check if the key is already there.
        let mut old_entry_size: u32 = 0;
        if self.map.contains_key(key) {
            // It is fine to get value here since access is syncronized.
            let old_value = self.map.get(key).unwrap(); // unwrap() is fine here.
            old_entry_size = block::Entry::size(key, &old_value);
        }

        let entry_size = block::Entry::size(key, value);

        // This should never overflow unsigned since we only subtract the size of
        // whats already in there.
        self.size = self.size - old_entry_size + entry_size;
    }

    pub fn is_full(&self) -> bool {
        if self.size > (SSTABLE_BYTESIZE - reserve()) {
            return true;
        }

        false
    }
}

/*
If memtable size reaches SSTABLE_BYTESYZE - reserve(), we make an sstable
right away because next key-value pair will most likely overflow the block size.
*/
fn reserve() -> u32 {
    crate::lsm::KEY_LIMIT + crate::lsm::VALUE_LIMIT + block::SINGLE_UNIT_OVERHEAD
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_full() {
        let mut mt = MemTable::new();
        mt.size = SSTABLE_BYTESIZE - reserve() - 13;
        let old_size = mt.size;

        let res = mt.insert(Bytes::from("bar"), Bytes::from("foo"));
        assert!(matches!(res, InsertResult::Available { .. }));

        assert!(!mt.is_full());

        let res = mt.insert(Bytes::from("foo"), Bytes::from("bar"));

        assert!(mt.is_full());
        assert_eq!(mt.size, old_size + 24);
        assert!(matches!(res, InsertResult::Full { .. }));
    }

    #[test]
    fn insert() {
        let mut mt = MemTable::new();
        assert_eq!(mt.size, 0);

        mt.insert(Bytes::from("foo"), Bytes::from("bar"));
        assert_eq!(mt.size, 12);

        mt.insert(Bytes::from("bar"), Bytes::from("foo"));
        assert_eq!(mt.size, 24);
        assert_eq!(mt.get(&Bytes::from("bar")).unwrap(), Bytes::from("foo"));

        mt.insert(Bytes::from("foo"), Bytes::from("bar"));
        assert_eq!(mt.size, 24);

        mt.insert(Bytes::from("foo"), Bytes::from("barbar"));
        assert_eq!(mt.size, 27);
        assert_eq!(mt.get(&Bytes::from("foo")).unwrap(), Bytes::from("barbar"));

        mt.insert(Bytes::from("foo"), Bytes::from("bar"));
        assert_eq!(mt.size, 24);
    }

    #[test]
    fn clear() {
        let mut mt = MemTable::new();
        mt.insert(Bytes::from("foo"), Bytes::from("bar"));
        mt.insert(Bytes::from("bar"), Bytes::from("foo"));

        mt.clear();

        assert_eq!(mt.size, 0);
        assert!(mt.get(&Bytes::from("foo")).is_none());
        assert!(mt.get(&Bytes::from("bar")).is_none());
    }
}
