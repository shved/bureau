mod block;
mod compaction;
mod index;
mod memtable;
mod sstable;
mod wal;

use bytes::Bytes;

/*
TODO:
Here engine will run a process accepting messages on the channel and
calling set and get.

fn run() {
    let(tx, mut rx) = mpsc::channel(1000);

    while let Some(message) = rx.recv().await {
        println!("GOT = {}", message);
    }
 }
*/

#[derive(Debug)]
pub struct Engine {
    db: memtable::MemTable,
}

impl Engine {
    pub fn new() -> Engine {
        Engine {
            db: memtable::MemTable::new(),
        }
    }

    fn run() {}

    pub fn insert(&mut self, key: Bytes, value: Bytes) {
        self.db.insert(key, value)
    }

    pub fn get(&self, key: Bytes) -> Option<Bytes> {
        self.db.get(key)
    }
}
