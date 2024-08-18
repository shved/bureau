mod compaction;
mod index;

use crate::Responder;
use bytes::Bytes;
use std::collections::btree_map::BTreeMap;
use tokio::sync::oneshot;
use uuid::Uuid;

#[derive(Debug)]
pub struct Dispatcher {
    // TODO: BtreeMap not the best type to send here.
    new_table_data_rx: oneshot::Receiver<BTreeMap<Bytes, Bytes>>,
    new_table_id_tx: Responder<Uuid>,
}

impl SSTDispatcher {
    pub fn new(
        new_table_data_rx: oneshot::Receiver<BTreeMap<Bytes, Bytes>>,
        new_table_id_tx: Responder<Uuid>,
    ) -> SSTDispatcher {
        SSTDispatcher {
            new_table_data_rx,
            new_table_id_tx,
        }
    }

    pub async fn run(mut self) {
        // while let Ok(_map) = self.new_table_data_rx.await {}
    }
}
