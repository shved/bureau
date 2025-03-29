mod cache;
pub mod compaction;
mod index;

use crate::engine::dispatcher::cache::{Cache, CacheValue, CheckResult};
use crate::engine::memtable::MemTable;
use crate::engine::sstable::SsTable;
use crate::{Responder, Result, Storage};
use bytes::Bytes;
use index::Index;
use tokio::sync::mpsc;
use tracing::info;
use uuid::Uuid;

#[derive(Debug)]
pub enum Command {
    Get {
        key: Bytes,
        responder: Responder<Option<Bytes>>,
    },
    CreateTable {
        data: MemTable,
        responder: Responder<()>,
    },
    Update(Uuid, Option<MemTable>),
    Shutdown {
        responder: Responder<()>,
    },
}

/// Dispatcher is managing SSTables on disk, syncronizing access and modification.
/// Dispatcher holds receiver to get commands from Engine, sstables buffer, that tells
/// how many sstables are allowed to be in the process of saving it to disk at the same time.
/// The lower this number, the lower memory bureau will consume under pressure. But all
/// the reads and writes will be suspended while buffer is full.
/// Dispatcher is also managing index which is a vector of all the tables ids persisted to disk.
// TODO: Experiment with possibly better disk access syncronisation mechanism where index will be shared
// across pool of threads, and every entry will be either free for access or locked for the moment
// it is being updated. For the moment new table is being written, the whole index will be locked.
#[derive(Debug)]
pub struct Dispatcher<T: Storage> {
    cmd_rx: mpsc::Receiver<Command>,
    storage: T,
    cache: Cache,
    index: Index,
    sst_buf_size: usize,
    sst_buf: usize,
}

impl<T: Storage> Dispatcher<T> {
    pub fn init(
        cmd_rx: mpsc::Receiver<Command>,
        sst_buf_size: usize,
        storage: T,
    ) -> std::result::Result<Self, anyhow::Error> {
        let mut entries = storage.list_entries()?;
        let index = Index::new(&mut entries);
        let cache = Cache::new(100);

        Ok(Dispatcher {
            cmd_rx,
            storage,
            cache,
            index,
            sst_buf_size,
            sst_buf: 0,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                Command::Get { key, responder } => {
                    // Defaults to Ok(None) which will be returned if none was found after all tables are checked.
                    let mut response: Result<Option<Bytes>, _> = Ok(None);
                    let cache_check = self.cache.check(&key);

                    if let CheckResult::Found(value) = cache_check {
                        info!(
                            "served cached value with {} frequency and {} generation (score {})",
                            value.score.frequency,
                            value.score.generation,
                            value.score()
                        );
                        response = Ok(Some(value.data));
                    } else {
                        // Go to disk to look for a value.
                        for (i, entry) in self.index.entries.iter().enumerate() {
                            let blob = self.storage.open(&entry.id).unwrap(); // TODO: Log error and send response to engine.

                            match SsTable::lookup(&blob, &key) {
                                Ok(Some(value)) => {
                                    if let CheckResult::Candidate(freq) = cache_check {
                                        self.cache.try_insert(
                                            key,
                                            CacheValue::new(value.clone(), freq, i + 1),
                                        )
                                    }
                                    response = Ok(Some(value));
                                    break;
                                }
                                Ok(None) => {
                                    // Go check the next table.
                                    continue;
                                }
                                Err(e) => {
                                    response = Err(e);
                                    break;
                                }
                            }
                        }
                    }

                    responder.send(response).ok();
                }
                Command::CreateTable { data, responder } => {
                    self.sst_buf += 1;
                    if self.sst_buf < self.sst_buf_size {
                        let _ = responder.send(Ok(())); // If buffer isnt full ack immediately to free engine thread.
                        let id = self.persist_table(data);
                        self.index.prepend(id);
                        self.sst_buf -= 1;
                    } else {
                        let id = self.persist_table(data);
                        self.index.prepend(id);
                        self.sst_buf -= 1;
                        let _ = responder.send(Ok(())); // If buffer is full, ack only when the table is on disk.
                    }
                }
                Command::Update(id, mem_table) => match mem_table {
                    None => {
                        self.storage.delete(&id)?;
                        self.index.delete(&id);
                    }
                    Some(memtable) => {
                        let sstable = SsTable::build(memtable);
                        let encoded = sstable.encode();

                        self.storage.write(&id, &encoded)?;
                    }
                },
                Command::Shutdown { responder } => {
                    let _ = self.storage.close();
                    let _ = responder.send(Ok(()));
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    /// Serializes and writes table to disk.
    /// It also visits and updates the cache along the way.
    fn persist_table(&mut self, data: MemTable) -> Uuid {
        self.update_cache(&data);

        let table = SsTable::build_full(data);
        let encoded_data = table.encode();

        // TODO: Actually handle when table can't be persisted.
        self.storage
            .write(&table.id, &encoded_data)
            .expect("Cant persist table");

        table.id
    }

    /// First, advance all cache records generations since index about to be
    /// updated with the new fresh table. Second, iterate all the new records
    /// to possibly update cached records if same keys found.
    fn update_cache(&mut self, data: &MemTable) {
        self.cache.advance();
        for (k, v) in data.map.iter() {
            self.cache.refresh_value(k, v);
        }
    }
}
