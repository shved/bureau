use crate::engine;
use crate::engine::sstable::block;
use crate::engine::wal::Wal;
use bytes::Bytes;
use std::collections::btree_map::BTreeMap;

pub const SSTABLE_BYTESIZE: u32 = 64 * 1024; // 64KB (16 blocks).
const MAX_ENTRY_SIZE: u32 = engine::MAX_KEY_SIZE + engine::MAX_VALUE_SIZE + block::ENTRY_OVERHEAD;

/// It's a map with ordered keys. Size keeps track of memtable size in bytes according to layout of sstable.
/// Max size is a limit after which a table will be flushed to disk. Note that all the size calculations here
/// before memtable being encoded are just approximation. We don't want to recalculate all the sstable layout
/// every time a key is added or being updated. Number of blocks in the table and its final size are nor strict
/// numbers nor guaranteed. What really matters is that we have blocks that are 4Kb (a memory page) in size
/// and that we want minimize blocks padding (zeroes at the end of a block) if possible.
#[derive(Debug, Clone)]
pub struct MemTable {
    pub map: BTreeMap<Bytes, Bytes>,
    size: u32,
    max_size: u32,
}

#[derive(Debug)]
pub enum ProbeResult {
    Available(u32),
    Full,
}

#[derive(Debug)]
pub enum SsTableSize {
    Default,
    Is(usize),
}

impl MemTable {
    #[allow(clippy::new_without_default)]
    pub fn new(size: SsTableSize) -> MemTable {
        let max_size = match size {
            SsTableSize::Default => SSTABLE_BYTESIZE,
            SsTableSize::Is(size) => size as u32,
        };

        if (max_size as usize) < block::BLOCK_BYTE_SIZE {
            panic!("SsTable should be at least one block in size.")
        }

        // Give the table initial size that is approximation of padding between blocks.
        // Numbers are arbitrary, not accurate but will result in a more consistent payload between blocks.
        let initial_size = (max_size / block::BLOCK_BYTE_SIZE as u32) * (engine::MAX_KEY_SIZE / 2);

        MemTable {
            map: BTreeMap::new(),
            size: initial_size,
            max_size,
        }
    }

    pub fn from_wal(_wal: Wal) -> MemTable {
        todo!();
    }

    /// The only purpose of this function is to check weither given key and value will owerflow
    /// the table size. If its not, the new table size will be returned with the result.
    pub fn probe(&self, key: &Bytes, value: &Bytes) -> ProbeResult {
        let new_size = self.new_size(key, value);
        if self.will_overflow(new_size) {
            return ProbeResult::Full;
        }

        ProbeResult::Available(new_size)
    }

    /// Along with key and value insert can take an optional size to update its state. If the size
    /// isn't provided it will explicitly call a function to calculate it. It could be a size is
    /// already known if probe function was called befor inserting a value.
    pub fn insert(&mut self, key: Bytes, value: Bytes, new_size: Option<u32>) {
        if let Some(new_size) = new_size {
            self.size = new_size;
        } else {
            self.size = self.new_size(&key, &value);
        }

        self.map.insert(key, value);
    }

    pub fn get(&self, key: &Bytes) -> Option<Bytes> {
        self.map.get(key).cloned()
    }

    pub fn clear(&mut self) {
        self.map.clear();
        self.size = 0;
    }

    /// A table that still has a room for one more huge entry is not considered full.
    pub fn is_full(&self) -> bool {
        if self.size > self.max_size - MAX_ENTRY_SIZE {
            return true;
        }

        false
    }

    /// It calculates a new size of the table based on the values to be inserted.
    /// It should handle the case when the key is already present in the table so
    /// that it wont be caunted twice.
    fn new_size(&self, key: &Bytes, value: &Bytes) -> u32 {
        // First, check if the key is already there.
        let mut old_entry_size: u32 = 0;
        if self.map.contains_key(key) {
            // It is fine to get value here since access is syncronized.
            let old_value = self.map.get(key).unwrap(); // unwrap() is fine here.
            old_entry_size = block::entry_size(key, old_value);
        }

        let entry_size = block::entry_size(key, value);

        self.size - old_entry_size + entry_size
    }

    fn will_overflow(&self, new_size: u32) -> bool {
        if new_size > self.max_size {
            return true;
        }

        false
    }

    // Function exists for debug purposes to list all the keys in block.
    pub fn keys(&self) -> Vec<String> {
        let keys: Vec<String> = self
            .map
            .keys()
            .cloned()
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect();

        keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::sstable::block;

    #[test]
    fn test_is_full() {
        let mut mt = MemTable::new(SsTableSize::Default);
        mt.size = mt.max_size - MAX_ENTRY_SIZE;
        assert!(!mt.is_full());

        mt.size += 1;
        assert!(mt.is_full());
    }

    #[test]
    fn test_new_size() {
        let mut mt = MemTable::new(SsTableSize::Default);
        let first_key = Bytes::from("foo");
        let first_value = Bytes::from("bar");
        let size = block::entry_size(&first_key, &first_value);
        mt.insert(first_key, first_value, Some(size));

        let second_key = Bytes::from("language");
        let second_value = Bytes::from("rust");
        assert_eq!(
            mt.new_size(&second_key, &second_value),
            mt.size + block::entry_size(&second_key, &second_value)
        );

        let dup_key = Bytes::from("foo");
        let new_value = Bytes::from("not-bar");
        assert_eq!(
            mt.new_size(&dup_key, &new_value),
            block::entry_size(&dup_key, &new_value)
        );
    }

    #[test]
    fn test_insert() {
        let mut mt = MemTable::new(SsTableSize::Is(block::BLOCK_BYTE_SIZE));
        assert_eq!(mt.size, 256);

        mt.insert(Bytes::from("foo"), Bytes::from("bar"), Some(256 + 12));
        assert_eq!(mt.size, 268);
        assert_eq!(mt.map.get(&Bytes::from("foo")), Some(&Bytes::from("bar")));
    }

    #[test]
    fn test_clear() {
        let mut mt = MemTable::new(SsTableSize::Default);
        mt.insert(Bytes::from("foo"), Bytes::from("bar"), Some(12));
        mt.insert(Bytes::from("bar"), Bytes::from("foo"), Some(24));

        mt.clear();

        assert_eq!(mt.size, 0);
        assert!(mt.get(&Bytes::from("foo")).is_none());
        assert!(mt.get(&Bytes::from("bar")).is_none());
    }

    #[test]
    fn test_will_overflow() {
        let mt = MemTable::new(SsTableSize::Is(block::BLOCK_BYTE_SIZE));
        assert!(mt.will_overflow((block::BLOCK_BYTE_SIZE + 1) as u32));
        assert!(!mt.will_overflow(block::BLOCK_BYTE_SIZE as u32));
    }
}
