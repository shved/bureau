mod compaction;
mod index;

use crate::lsm::memtable::MemTable;
use crate::lsm::sstable::SsTable;
use crate::Responder;
use bytes::Bytes;
use tokio::sync::mpsc;

pub enum Command {
    Get {
        key: Bytes,
        responder: Responder<Option<Bytes>>,
    },
    CreateTable {
        data: MemTable,
    },
}

/// Dispatcher is managing SSTables syncronizing access and modification.
#[derive(Debug)]
pub struct Dispatcher {
    cmd_rx: mpsc::Receiver<Command>,
    index: index::Index,
}

impl Dispatcher {
    pub fn init(cmd_rx: mpsc::Receiver<Command>) -> std::result::Result<Dispatcher, anyhow::Error> {
        let index = index::Index::init()?;

        Ok(Dispatcher { cmd_rx, index })
    }

    pub async fn run(mut self) {
        while let Some(cmd) = self.cmd_rx.recv().await {
            match cmd {
                Command::Get { key, responder } => {
                    for entry in &self.index.entries {
                        match SsTable::lookup(&entry.id, &key) {
                            Ok(Some(value)) => {
                                responder.send(Ok(Some(value))).ok();
                                return;
                            }
                            Ok(None) => {
                                responder.send(Ok(None)).ok();
                                return;
                            }
                            Err(e) => {
                                responder.send(Err(e)).ok();
                                return;
                            }
                        }
                    }

                    responder.send(Ok(None)).ok();
                }
                Command::CreateTable { data } => {
                    let table = SsTable::build(data);
                    let encoded_data = table.encode();
                    table
                        .persist(&encoded_data)
                        .expect("Cant dump table to disk");
                    self.index.prepend(table.id);
                }
            }
        }
    }
}
