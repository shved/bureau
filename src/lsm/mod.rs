mod cache;
mod dispatcher;
pub mod memtable;
mod sstable;
mod wal;

use crate::lsm::memtable::MemTable;
use crate::Responder;
use bytes::Bytes;
use dispatcher::Dispatcher;
use std::fs::create_dir;
use std::path::{Path, PathBuf};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

/// This is where data files will be stored.
const DATA_PATH: &str = "/var/lib/bureau/"; // TODO: Make configurable.

/// This is how many tables are allowed to be in the process of writing to disk at the same time.
/// Grow this number to make DB more tolerant to high amount writes, it will consume more memory
/// in return.
const DISPATCHER_BUFFER_SIZE: usize = 32; // TODO: Make configurable.

// TODO: Make configurable.
const KEY_LIMIT: u32 = 512; // 512B.

// TODO: Make configurable.
const VALUE_LIMIT: u32 = 2048; // 2KB.

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
    memtable: MemTable,
    wal: wal::Wal,
    data_path: String,
}

/// Engine is a working horse of the database. It holds memtable and a channel to communicate commands to.
impl Engine {
    pub fn new(rx: mpsc::Receiver<Command>, data_path: Option<String>) -> Engine {
        let path_str: String;

        if let Some(data_path) = data_path {
            path_str = data_path;
        } else {
            path_str = DATA_PATH.to_owned();
        }

        Engine {
            input_rx: rx,
            memtable: memtable::MemTable::new(),
            wal: wal::Wal {},
            data_path: path_str,
        }
    }

    /// This function is to run in the background reading commands from the channel. It itself also spawns
    /// a dispathcher thread that works with everything living on the disk a syncronized way.
    pub async fn run(mut self) {
        let path = Path::new(&self.data_path);

        if !path.exists() {
            create_dir(path).unwrap_or_else(|_| {
                panic!(
                    "Could not create data directory at {}",
                    path.to_str().unwrap()
                )
            });
        }

        let (disp_tx, disp_rx) = mpsc::channel::<dispatcher::Command>(64);
        let disp =
            Dispatcher::init(disp_rx, DISPATCHER_BUFFER_SIZE, self.data_path.clone()).unwrap(); // TODO: Remove unwrap();

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
                } => {
                    if let Err(err) = validate(&key, &value) {
                        responder.send(Err(err)).ok();
                        return;
                    }

                    match self.memtable.insert(key, value) {
                        memtable::InsertResult::Available => {
                            responder.send(Ok(())).ok();
                        }
                        memtable::InsertResult::Full => {
                            let (resp_tx, resp_rx) = oneshot::channel();

                            disp_tx
                                .send(dispatcher::Command::CreateTable {
                                    data: self.swap_tables(),
                                    responder: resp_tx,
                                })
                                .await
                                .ok();

                            resp_rx.await.ok(); // Blocks if dispatcher tables buffer is full.

                            responder.send(Ok(())).ok();
                        }
                    }
                }
            };
        }
    }

    /// It only checks hot spots: cache, memtable, shadow table.
    fn get_from_mem(&self, key: &Bytes) -> Option<Bytes> {
        // TODO: First search cache.

        if let Some(value) = self.memtable.get(key) {
            return Some(value);
        }

        None
    }

    /// Swaps memtable with fresh one and sends full table to dispatcher that syncronously write it to disk.
    fn swap_tables(&mut self) -> memtable::MemTable {
        // TODO: When SSTable is written WAL should be rotated.
        let mut swapped = memtable::MemTable::new();
        std::mem::swap(&mut self.memtable, &mut swapped);
        swapped
    }
}

fn validate(key: &Bytes, value: &Bytes) -> crate::Result<()> {
    if key.len() > KEY_LIMIT as usize {
        return Err(crate::Error::from("key is too long"));
    }

    if value.len() > VALUE_LIMIT as usize {
        return Err(crate::Error::from("value is too long"));
    }

    Ok(())
}

pub fn sstable_path(table_id: &Uuid) -> PathBuf {
    Path::new(DATA_PATH).join(table_id.to_string())
}
