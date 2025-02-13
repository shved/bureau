pub mod client;
mod engine;
pub mod protocol;
pub mod server;
pub mod storage;
pub mod wal;

use bytes::Bytes;
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

    /// Closes the storage. Can be used to flush buffers, free resources etc.
    fn close(&self) -> io::Result<()>;
}

pub trait StorageEntry {
    /// Reads at exactly given position for the length of given vector.
    fn read_at(&self, data: &mut Vec<u8>, position: u64) -> io::Result<()>;
}

pub trait WalStorage: Send + 'static
where
    Self: Sized,
{
    type LogPath;

    /// Setup storage and pick up existing records if found.
    fn init(path: Self::LogPath) -> io::Result<Self>;

    /// Returns unfinished log as a blob if any.
    fn persisted_data(&mut self) -> io::Result<Option<Bytes>>;

    /// Append data to WAL.
    fn append(&mut self, buf: bytes::Bytes) -> io::Result<()>;

    /// Changes underlying WAL generation (e.g. file).
    fn rotate(&mut self) -> io::Result<()>;
}
