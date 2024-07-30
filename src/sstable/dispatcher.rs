use super::builder::Builder;
use super::Name;
use crate::Responder;
use bytes::Bytes;
use std::collections::btree_map::BTreeMap;
use tokio::sync::oneshot;

enum DispatchResult {
    // Two SSTables to be replaced by a new one already present on disk.
    Replace((Name, Name), Name),
    New(Name),
}

struct SSTDispatcher {
    // TODO: BtreeMap not the best type to send here.
    new_table_data_rx: oneshot::Receiver<BTreeMap<Bytes, Bytes>>,
    new_table_id_tx: Responder<Name>,
}

impl SSTDispatcher {
    pub fn new(
        new_table_data_rx: oneshot::Receiver<BTreeMap<Bytes, Bytes>>,
        new_table_id_tx: Responder<Name>,
    ) -> SSTDispatcher {
        SSTDispatcher {
            new_table_data_rx,
            new_table_id_tx,
        }
    }

    pub async fn run(mut self) {
        while let Ok(map) = self.new_table_data_rx.await {
            let builder = Builder::new(&map);
        }
    }
}
