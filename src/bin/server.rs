use bureau::wal::fs_storage::{FsStorage, LogPath};
use bureau::WalStorage;
use bureau::{server, server::ConnLimit};
use bureau::{storage, storage::DataPath};
use std::env;
use std::error::Error;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::{error, info};
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

#[tokio::main]
async fn main() -> bureau::Result<(), Box<dyn Error>> {
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(fmt::Layer::default())
        .init();

    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:12650".to_string());

    let stor = storage::new(DataPath::Default);
    let wal_stor = FsStorage::init(LogPath::Default)?;
    let listener = TcpListener::bind(&addr).await?;

    info!("Listening on: {}", addr);
    if let Err(e) = server::run(
        listener,
        ConnLimit::Default,
        stor,
        wal_stor,
        signal::ctrl_c(),
    )
    .await
    {
        error!("server exited: {}", e);
        std::process::exit(1);
    }

    Ok(())
}
