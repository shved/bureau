mod compaction;
mod index;

use crate::engine::memtable::MemTable;
use crate::engine::sstable::SsTable;
use crate::{Responder, Result, Storage};
use bytes::Bytes;
use index::Index;
use tokio::sync::mpsc;
use uuid::Uuid;

pub enum Command {
    Get {
        key: Bytes,
        responder: Responder<Option<Bytes>>,
    },
    CreateTable {
        data: MemTable,
        responder: Responder<()>,
    },
    ReplaceTables(((Uuid, Uuid), Uuid)), // TODO: To be used by a compaction thread.
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
#[derive(Debug)]
pub struct Dispatcher<T: Storage> {
    cmd_rx: mpsc::Receiver<Command>,
    storage: T,
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
        let index = Index::init(&mut entries);

        Ok(Dispatcher {
            cmd_rx,
            storage,
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

                    for entry in self.index.entries.iter() {
                        let blob = self.storage.open(&entry.id).unwrap(); // TODO: Log error and send response to engine.

                        match SsTable::lookup(&blob, &key) {
                            Ok(Some(value)) => {
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
                Command::ReplaceTables(((_old1, _old2), _new)) => {
                    todo!()
                }
                Command::Shutdown { responder } => {
                    let _ = self.storage.close();
                    let _ = responder.send(Ok(()));
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn persist_table(&self, data: MemTable) -> Uuid {
        let table = SsTable::build(data);
        let encoded_data = table.encode();

        // TODO: Actually handle when table can't be persisted.
        self.storage
            .write(&table.id, &encoded_data)
            .expect("Cant persist table");

        table.id
    }
}
