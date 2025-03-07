use crate::engine::dispatcher::Command;
use crate::engine::memtable::{MemTable, SsTableSize};
use crate::engine::sstable::SsTable;
use crate::{Result, Storage};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;
use tokio::time::{self, Duration};
use uuid::Uuid;

/// Struct to hold state of compaction. Track UUIDs sorted from oldest to fresh.
/// Head is to track current table being compacted.
#[derive(Debug, Clone, Copy)]
struct CompactionIter<'a> {
    pub track: &'a [Uuid],
    pub last_index: usize,
    pub head: usize,
}

impl<'a> CompactionIter<'a> {
    fn new(entries: &'a mut [Uuid]) -> Self {
        assert!(entries.len() > 1);

        entries.sort();

        Self {
            track: entries,
            last_index: entries.len() - 1,
            head: 0,
        }
    }
}

impl Iterator for CompactionIter<'_> {
    type Item = Uuid;

    fn next(&mut self) -> Option<Self::Item> {
        assert!(self.track.len() > 1);

        if self.head == self.last_index {
            // No need to shrink the very last table
            // since it's nothing to compare it with.
            return None;
        }

        let cur = self.track[self.head];

        self.head += 1;

        Some(cur)
    }
}

// TODO: Compaction is rather unoptimized, very basic and straightforward. First it could be
// optimized by first checking with table index if tables do even have potential to have same
// keys or not and then only read those blocks that are potentially have intersection with
// the given table. For now it just checks for all the records.
pub async fn run<T: Storage>(storage: T, dispatcher_tx: Sender<Command>) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(5 * 60));

    loop {
        interval.tick().await;

        let mut entries = storage.list_entries()?;

        if entries.len() < 10 {
            continue;
        }

        let iter = CompactionIter::new(entries.as_mut());
        for uuid in iter.into_iter() {
            let mut table = storage.open(&uuid)?;
            let mut map = SsTable::decode(&mut table)?;
            for i in iter.head + 1..iter.last_index {
                let mut compare_table = storage.open(&iter.track[i])?;
                let compare_map = SsTable::decode(&mut compare_table)?;
                remove_duplicates(&mut map, &compare_map);
            }

            let mt = Arc::new(MemTable::from_map(SsTableSize::Default, &map));

            dispatcher_tx.send(Command::Update(uuid, mt)).await.ok();
        }
    }
}

fn remove_duplicates(first: &mut BTreeMap<Bytes, Bytes>, second: &BTreeMap<Bytes, Bytes>) {
    let keys_to_delete: Vec<Bytes> = first
        .iter()
        .filter(|(k, v)| second.get(*k) == Some(v))
        .map(|(k, _)| k.clone())
        .collect();

    for key in keys_to_delete {
        first.remove(&key);
    }
}
