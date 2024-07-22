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
    wal: wal::Wal,
    shadow_table_written: bool, // TODO: May be move it to table level somehow.
}

impl Engine {
    pub fn new() -> Engine {
        Engine {
            memtable: memtable::MemTable::new(),
            shadow_table: memtable::MemTable::new(),
            shadow_table_written: true,
            index: index::Index::new(),
            wal: wal::Wal {},
        }
    }

    pub fn insert(&mut self, key: Bytes, value: Bytes) {
        match self.memtable.insert(key, value) {
            memtable::InsertResult::Full => self.swap_tables(),
            _ => (),
        }
    }

    pub fn get(&self, key: Bytes) -> Option<Bytes> {
        // TODO: First search cache.

        if let Some(value) = self.memtable.get(&key) {
            return Some(value);
        }

        if let Some(value) = self.shadow_table.get(&key) {
            return Some(value);
        }

        // TODO: Then search index.
        // self.search_index(&key)

        None
    }

    // Swap memtable and shadow table to data while sstable is building.
    fn swap_tables(&mut self) {
        // TODO: When SSTable is written WAL should be rotated.
        if self.shadow_table_written {
            self.shadow_table.clear(); // Ensure its empty.
            std::mem::swap(&mut self.memtable, &mut self.shadow_table);
        }

        panic!("handle edge case somehow");
    }
}
