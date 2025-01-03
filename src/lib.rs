pub mod engine;
pub mod storage;

use std::io;
use tokio::sync::oneshot;
use uuid::Uuid;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type Responder<T> = oneshot::Sender<Result<T>>;

pub trait Storage: Clone + Send + 'static {
    type Entry: StorageEntry;

    /// Includes any setup required by storage to function. For file system it could be creating
    /// directories for data, for network blob storage it could register a bucket and so on.
    fn bootstrap(&self) -> io::Result<()>;

    /// Note that uuids of SsTables have to be alphabetically reverse ordered. Using UUIDs V7
    /// makes it properly work since sorting it will also order them in table creation time.
    fn list_entries(&self) -> io::Result<Vec<Uuid>>;

    /// It writes a new SsTable to storage.
    fn write(&self, table_id: &Uuid, data: &[u8]) -> io::Result<()>;

    /// Opens SsTable to sequentially read it later.
    fn open(&self, table_id: &Uuid) -> io::Result<Self::Entry>;
}

pub trait StorageEntry {
    /// Reads at exactly given position for the length of given vector.
    fn read_at(&self, data: &mut Vec<u8>, position: u64) -> io::Result<()>;
}
