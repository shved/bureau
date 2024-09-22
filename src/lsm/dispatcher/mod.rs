mod compaction;
mod index;

use crate::lsm::memtable::MemTable;
use crate::lsm::sstable::SsTable;
use crate::Responder;
use bytes::Bytes;
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
}

/// Dispatcher is managing SSTables on disk, syncronizing access and modification.
/// Dispatcher holds receiver to get commands from Engine, sstables buffer, that tells
/// how many sstables are allowed to be in the process of saving it to disk at the same time.
/// The lower this number, the lower memory bureau will consume under pressure. But all
/// the reads and writes will be suspended while buffer is full.
/// Dispatcher is also managing index which is a vector of all the tables ids persisted to disk.
#[derive(Debug)]
pub struct Dispatcher {
    cmd_rx: mpsc::Receiver<Command>,
    sst_buf_size: usize,
    sst_buf: usize,
    index: index::Index,
}

impl Dispatcher {
    pub fn init(
        cmd_rx: mpsc::Receiver<Command>,
        sst_buf_size: usize,
    ) -> std::result::Result<Self, anyhow::Error> {
        let index = index::Index::init()?;

        Ok(Dispatcher {
            cmd_rx,
            sst_buf_size,
            sst_buf: 0,
            index,
        })
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                Command::Get { key, responder } => {
                    for entry in self.index.entries.iter() {
                        match SsTable::lookup(&entry.id, &key) {
                            Ok(Some(value)) => {
                                responder.send(Ok(Some(value))).ok();
                                return;
                            }
                            Ok(None) => {
                                continue;
                            }
                            Err(e) => {
                                responder.send(Err(e)).ok();
                                return;
                            }
                        }
                    }

                    responder.send(Ok(None)).ok();
                }
                Command::CreateTable { data, responder } => {
                    self.sst_buf += 1;
                    if self.sst_buf < self.sst_buf_size {
                        responder.send(Ok(())).ok(); // If buffer isnt full ack immediately to free engine thread.
                        let id = persist_table(data);
                        self.index.prepend(id);
                        self.sst_buf -= 1;
                    } else {
                        let id = persist_table(data);
                        self.index.prepend(id);
                        self.sst_buf -= 1;
                        responder.send(Ok(())).ok(); // If buffer is full, ack only when the table is on disk.
                    }
                }
                Command::ReplaceTables(((_old1, _old2), _new)) => {
                    todo!()
                }
            }
        }
    }
}

fn persist_table(data: MemTable) -> Uuid {
    let table = SsTable::build(data);
    let encoded_data = table.encode();

    // TODO: Actually handle when table can't be persisted.
    table
        .persist(&encoded_data)
        .expect("Cant dump table to disk");

    table.id
}
