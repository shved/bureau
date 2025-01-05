mod dispatcher;
pub mod memtable;
mod sstable;
mod wal;

use crate::engine::memtable::MemTable;
use crate::engine::memtable::SsTableSize;
use crate::Responder;
use crate::Storage;
use bytes::Bytes;
use dispatcher::Dispatcher;
use tokio::sync::{mpsc, oneshot};

/// This is where data files will be stored.
pub const DATA_PATH: &str = "/var/lib/bureau"; // TODO: Make configurable.

/// This is how many tables are allowed to be in the process of writing to disk at the same time.
/// Grow this number to make DB more tolerant to high amount writes, it will consume more memory
/// in return.
const DISPATCHER_BUFFER_SIZE: usize = 32; // TODO: Make configurable.

// TODO: Make configurable.
const MAX_KEY_SIZE: u32 = 512; // 512B.

// TODO: Make configurable.
const MAX_VALUE_SIZE: u32 = 2048; // 2KB.

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
#[allow(dead_code)]
pub struct Engine {
    input_rx: mpsc::Receiver<Command>,
    // TODO: Channel to shutdown + tokio::select! inside run loop.
    // shutdown_rx: mpsc::Receiver<Command>,
    memtable: MemTable,
    wal: wal::Wal,
}

/// Engine is a working horse of the database. It holds memtable and a channel to communicate commands to.
impl Engine {
    pub fn new(rx: mpsc::Receiver<Command>) -> Self {
        Engine {
            input_rx: rx,
            memtable: MemTable::new(SsTableSize::Default),
            wal: wal::Wal {},
        }
    }

    /// This function is to run in the background thread, to read and handle commands from
    /// the channel. It itself also spawns a dispathcher thread that works with everything
    /// living on the disk a syncronized way.
    pub async fn run<T: Storage>(mut self, storage: T) {
        storage
            .bootstrap()
            .unwrap_or_else(|e| panic!("Could not setup storage: {}", e));

        let (disp_tx, disp_rx) = mpsc::channel::<dispatcher::Command>(64);
        let disp = Dispatcher::init(disp_rx, DISPATCHER_BUFFER_SIZE, storage)
            .unwrap_or_else(|e| panic!("Could not initialize dispatcher: {}", e));

        let join_handle = tokio::spawn(async move {
            disp.run().await;
            tracing::error!("dispatched exited");
        });
        tokio::spawn(async move {
            tracing::error!("dispatched exit: {:?}", join_handle.await);
        });

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

                    match self.memtable.probe(&key, &value) {
                        memtable::ProbeResult::Available(new_size) => {
                            self.memtable.insert(key, value, Some(new_size));
                            responder.send(Ok(())).ok();
                        }
                        memtable::ProbeResult::Full => {
                            // Swap tables and respond to client first.
                            let old_table = self.swap_table();
                            self.memtable.insert(key, value, None);
                            responder.send(Ok(())).ok();

                            // Now send full table to dispatcher to put it to disk.
                            let (resp_tx, resp_rx) = oneshot::channel();

                            disp_tx
                                .send(dispatcher::Command::CreateTable {
                                    data: old_table,
                                    responder: resp_tx,
                                })
                                .await
                                .ok();

                            resp_rx.await.ok(); // Blocks if dispatcher tables buffer is full.
                        }
                    }
                }
            };
        }
    }

    /// It only checks hot spots: cache, memtable.
    fn get_from_mem(&self, key: &Bytes) -> Option<Bytes> {
        // TODO: First search cache.

        if let Some(value) = self.memtable.get(key) {
            return Some(value);
        }

        None
    }

    /// Swaps memtable with fresh one and sends full table to dispatcher that syncronously write it to disk.
    fn swap_table(&mut self) -> MemTable {
        // TODO: When SSTable is written WAL should be rotated.
        let mut swapped = MemTable::new(SsTableSize::Default);
        std::mem::swap(&mut self.memtable, &mut swapped);
        swapped
    }
}

fn validate(key: &Bytes, value: &Bytes) -> crate::Result<()> {
    if key.is_empty() {
        return Err(crate::Error::from("key is empty"));
    }

    if key.len() > MAX_KEY_SIZE as usize {
        return Err(crate::Error::from("key is too long"));
    }

    if value.is_empty() {
        return Err(crate::Error::from("value is empty"));
    }

    if value.len() > MAX_VALUE_SIZE as usize {
        return Err(crate::Error::from("value is too long"));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate() {
        let long_arr: &'static [u8; 513] = &[0; 513];
        let longer_arr: &'static [u8; 2049] = &[0; 2049];
        let long_key = Bytes::from_static(long_arr);
        let long_value = Bytes::from_static(longer_arr);

        if let Err(e) = validate(&long_key, &Bytes::from("asdf")) {
            assert_eq!(e.to_string(), "key is too long");
        } else {
            panic!()
        }

        if let Err(e) = validate(&Bytes::from("asdf"), &long_value) {
            assert_eq!(e.to_string(), "value is too long");
        } else {
            panic!()
        }

        if let Err(e) = validate(&Bytes::default(), &Bytes::from("asdf")) {
            assert_eq!(e.to_string(), "key is empty");
        } else {
            panic!()
        }

        if let Err(e) = validate(&Bytes::from("asdf"), &Bytes::default()) {
            assert_eq!(e.to_string(), "value is empty");
        } else {
            panic!()
        }
    }
}
