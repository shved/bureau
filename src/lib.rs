pub mod engine;
pub mod sstable;

use tokio::sync::oneshot;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub type Responder<T> = oneshot::Sender<Result<T>>;
