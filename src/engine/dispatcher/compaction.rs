use crate::engine::dispatcher::Command;
use crate::engine::memtable::{MemTable, SsTableSize};
use crate::engine::sstable::block;
use crate::engine::sstable::SsTable;
use crate::{Result, Storage};
use bytes::Bytes;
use std::collections::BTreeMap;
use tokio::sync::mpsc::Sender;
use tokio::time::{self, Duration, Instant};
use tracing::info;
use uuid::Uuid;

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
            info!(
                "skipping compaction; there are only {} entries",
                entries.len()
            );
            continue;
        } else {
            info!("compaction started for {} tables", entries.len());
        }
        let start = Instant::now();

        let total = compaction(storage.clone(), &dispatcher_tx, entries.as_mut()).await?;

        let elapsed = start.elapsed().as_millis();
        info!(
            "compaction finished in {} ms, {} bytes shrinked in total",
            elapsed, total
        );
    }
}

#[allow(clippy::needless_range_loop)]
async fn compaction<T: Storage>(
    storage: T,
    disp_tx: &Sender<Command>,
    entries: &mut [Uuid],
) -> Result<usize> {
    assert!(entries.len() > 2);

    entries.sort();

    let mut total_shrinked: usize = 0;

    for i in 0..entries.len() - 1 {
        let mut table = storage.open(&entries[i])?;
        let mut map = SsTable::decode(&mut table)?;
        let mut shrinked_bytes: usize = 0;

        for j in i + 1..entries.len() {
            let mut compare_table = storage.open(&entries[j])?;
            let compare_map = SsTable::decode(&mut compare_table)?;
            let res = compact(&mut map, &compare_map);
            shrinked_bytes += res;
            if map.is_empty() {
                break;
            }
        }

        if shrinked_bytes > 0 {
            let m = if map.is_empty() {
                None
            } else {
                Some(MemTable::from_map(SsTableSize::Default, &map))
            };

            let _ = disp_tx.send(Command::Update(entries[i], m)).await;

            total_shrinked += shrinked_bytes;

            info!(
                "table {} shrinked for {} bytes",
                &entries[i], shrinked_bytes
            );
        }
    }

    Ok(total_shrinked)
}

fn compact(first: &mut BTreeMap<Bytes, Bytes>, second: &BTreeMap<Bytes, Bytes>) -> usize {
    let keys_to_delete: Vec<Bytes> = first
        .keys()
        .filter(|k| second.contains_key(*k))
        .cloned()
        .collect();

    let mut shrinked_bytes: usize = 0;

    for key in keys_to_delete {
        if let Some(value) = first.remove(&key) {
            shrinked_bytes =
                shrinked_bytes + block::ENTRY_OVERHEAD as usize + key.len() + value.len();
        }
    }

    shrinked_bytes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::dispatcher::Command;
    use crate::engine::memtable::{MemTable, ProbeResult, SsTableSize};
    use crate::engine::sstable::SsTable;
    use crate::storage::mem;
    use tokio::sync::mpsc;

    #[test]
    fn test_compact_fn() {
        let mut first = BTreeMap::new();
        first.insert(Bytes::from("Fyodor"), Bytes::from("_Dostoevsky_"));
        first.insert(Bytes::from("Leo"), Bytes::from("_Tolstoy_"));
        first.insert(Bytes::from("Jerome"), Bytes::from("_Salinger_"));

        let mut second = BTreeMap::new();
        second.insert(Bytes::from("Leo"), Bytes::from("Tolstoy"));
        second.insert(Bytes::from("Anton"), Bytes::from("Checkov"));

        assert!(compact(&mut first, &second) > 0);
        assert!(!first.contains_key(&Bytes::from("Leo")));
    }

    #[tokio::test]
    async fn test_compaction() {
        let (disp_tx, mut disp_rx) = mpsc::channel::<Command>(64);
        let storage = mem::new();
        let test_key1 = Bytes::from("test_key1");
        let test_key2 = Bytes::from("test_key2");
        let test_key3 = Bytes::from("test_key3");
        let keys = vec![test_key1.clone(), test_key2.clone(), test_key3.clone()];

        for _ in 0..3 {
            let sstable = create_sstable(keys.clone());
            let encoded = SsTable::encode(&sstable);
            storage.write(&sstable.id, &encoded).unwrap();
        }

        let mut entries = storage.list_entries().unwrap();

        compaction(storage.clone(), &disp_tx, &mut entries)
            .await
            .unwrap();

        let mut messages: Vec<Command> = vec![];

        drop(disp_tx);

        while let Some(msg) = disp_rx.recv().await {
            messages.push(msg);
        }

        assert_eq!(messages.len(), 2);
        assert!(matches!(messages[0], Command::Update { .. }));
        assert!(matches!(messages[1], Command::Update { .. }));

        if let Some(Command::Update(_, mt)) = messages.first() {
            assert!(mt.is_some());
            let mt = mt.clone().unwrap();
            assert!(&mt.get(&test_key1).is_none());
            assert!(&mt.get(&test_key2).is_none());
            assert!(&mt.get(&test_key3).is_none());
        }

        if let Some(Command::Update(_, mt)) = messages.get(1) {
            assert!(mt.is_some());
            let mt = mt.clone().unwrap();
            assert!(&mt.get(&test_key1).is_none());
            assert!(&mt.get(&test_key2).is_none());
            assert!(&mt.get(&test_key3).is_none());
        }

        let mut last_table = storage.open(entries.last().unwrap()).unwrap();
        let last_table = SsTable::decode(&mut last_table).unwrap();
        assert!(last_table.contains_key(&test_key1));
        assert!(last_table.contains_key(&test_key2));
        assert!(last_table.contains_key(&test_key3));
    }

    fn create_sstable(preset_keys: Vec<Bytes>) -> SsTable {
        let mut mt = MemTable::new(SsTableSize::Is(4 * 1024), None);
        for k in preset_keys {
            mt.insert(k, Bytes::from(Uuid::now_v7().to_string()), None);
        }

        loop {
            // Fill it with random simple uuids.
            let key = Bytes::from(Uuid::now_v7().to_string());
            let value = Bytes::from(Uuid::now_v7().to_string());
            match mt.probe(&key, &value) {
                ProbeResult::Available(new_size) => {
                    mt.insert(key, value, Some(new_size));
                }
                ProbeResult::Full => {
                    break;
                }
            }
        }

        SsTable::build_full(mt)
    }
}
