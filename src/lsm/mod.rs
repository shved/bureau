mod cache;
mod dispatcher;
pub mod memtable;
mod sstable;
mod wal;

use crate::Responder;
use bytes::Bytes;
use dispatcher::Dispatcher;
use std::fs::create_dir;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use uuid::Uuid;

const DATA_PATH: &str = "/var/lib/bureau/";

pub enum Command {
    Get {
        key: Bytes,
        responder: Responder<Option<Bytes>>,
    },
    Set {
        key: Bytes,
        value: Bytes,
        responder: Responder<()>,
    },
}

#[derive(Debug)]
pub struct Engine {
    input_rx: mpsc::Receiver<Command>,
    // TODO: Channel to shutdown + tokio::select! inside run loop.
    // shutdown_rx: mpsc::Receiver<Command>,
    memtable: memtable::MemTable,
    shadow_table: memtable::MemTable,
    wal: wal::Wal,
    shadow_table_written: bool, // TODO: May be move it to table level somehow.
}

impl Engine {
    pub fn new(rx: mpsc::Receiver<Command>) -> Engine {
        Engine {
            input_rx: rx,
            memtable: memtable::MemTable::new(),
            shadow_table: memtable::MemTable::new(),
            shadow_table_written: true,
            wal: wal::Wal {},
        }
    }

    pub async fn run(mut self) {
        let p = Path::new(DATA_PATH);
        if !p.exists() {
            create_dir(p).expect(
                format!("Could not create data directory at {}", p.to_str().unwrap()).as_str(),
            );
        }

        let (disp_tx, disp_rx) = mpsc::channel::<dispatcher::Command>(64);
        let disp = Dispatcher::init(disp_rx).unwrap();

        tokio::spawn(disp.run());

        // TODO: Change it to select! here to handle shutdown.
        while let Some(cmd) = self.input_rx.recv().await {
            match cmd {
                Command::Get { key, responder } => {
                    match self.get_from_mem(&key) {
                        Some(value) => {
                            responder.send(Ok(Some(value))).ok();
                        }
                        None => {
                            disp_tx
                                .send(dispatcher::Command::Get { key, responder })
                                .await
                                .ok();
                        }
                    };
                }
                Command::Set {
                    key,
                    value,
                    responder,
                } => match self.memtable.insert(key, value) {
                    memtable::InsertResult::Available => {
                        responder.send(Ok(())).ok();
                    }
                    memtable::InsertResult::Full => {
                        self.swap_tables();
                        disp_tx
                            .send(dispatcher::Command::CreateTable {
                                // TODO: Im literally sending a 64kb struct here.
                                // Remake it here. Shadow table should be owned by dispatcher.
                                // We could initialize a new table here, and swap them.
                                // Dispatcher could even have a bunch of shadow tables at the same
                                // time to write them to disk.
                                data: self.shadow_table.clone(),
                            })
                            .await
                            .ok();
                        responder.send(Ok(())).ok();
                    }
                },
            };
        }
    }

    /// It only checks hot spots: cache, memtable, shadow table.
    fn get_from_mem(&self, key: &Bytes) -> Option<Bytes> {
        // TODO: First search cache.

        if let Some(value) = self.memtable.get(key) {
            return Some(value);
        }

        if let Some(value) = self.shadow_table.get(key) {
            return Some(value);
        }

        None
    }

    /// Swaps memtable and shadow table to data while sstable is building.
    fn swap_tables(&mut self) {
        // TODO: When SSTable is written WAL should be rotated.
        if self.shadow_table_written {
            self.shadow_table.clear(); // Ensure its empty.
            std::mem::swap(&mut self.memtable, &mut self.shadow_table);
        }

        // TODO: may be make it possible to spam memtables on demand instead of just swap two.
        todo!("handle edge case somehow");
    }
}

pub fn sstable_path(table_id: &Uuid) -> PathBuf {
    Path::new(DATA_PATH).join(table_id.to_string())
}
