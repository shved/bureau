use anyhow::Error;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub struct Client {
    addr: SocketAddr,
    conn: TcpStream,
}

impl Client {
    pub async fn connect(addr: &str) -> std::result::Result<Self, Error> {
        let addr = addr.parse::<SocketAddr>()?;
        let conn = TcpStream::connect(&addr).await?;

        Ok(Client { addr, conn })
    }

    async fn reconnect(&mut self) -> std::result::Result<(), Error> {
        let mut timeout_ms = 100;
        let max_timeout_ms = 5000;

        while timeout_ms < max_timeout_ms {
            match TcpStream::connect(&self.addr).await {
                Ok(conn) => {
                    self.conn = conn;
                    return Ok(());
                }
                Err(_) => {
                    tokio::time::sleep(Duration::from_millis(timeout_ms)).await;
                    timeout_ms *= 2;
                }
            };
        }

        Ok(())
    }

    pub async fn send(&mut self, mut msg: String) -> std::result::Result<Vec<u8>, Error> {
        if !msg.ends_with('\n') {
            msg.push('\n');
        }

        let write_result = self.conn.write_all(msg.as_bytes()).await;

        // Check if the connection was reset or terminated.
        if let Err(e) = write_result {
            if is_connection_error(&e) {
                self.reconnect().await?;
                self.conn.write_all(msg.as_bytes()).await?;
            } else {
                return Err(e.into());
            }
        }

        // Read the response.
        let mut buffer = vec![0; 4096];
        let read_result = self.conn.read(&mut buffer).await;

        // Check if the connection was reset or terminated during reading.
        if let Err(e) = read_result {
            if is_connection_error(&e) {
                self.reconnect().await?;
                self.conn.write_all(msg.as_bytes()).await?;
                let n = self.conn.read(&mut buffer).await?;
                return Ok(buffer[..n].to_vec());
            } else {
                return Err(e.into());
            }
        }

        let n = read_result?;
        Ok(buffer[..n].to_vec())
    }
}

fn is_connection_error(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::NotConnected
    )
}
