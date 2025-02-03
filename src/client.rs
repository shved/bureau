use crate::protocol::{ClientMessenger, Request, Response};
use anyhow::Error;
use futures::sink::SinkExt;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;

pub struct Client {
    conn: Framed<TcpStream, ClientMessenger>,
}

impl Client {
    pub async fn connect(addr: &str) -> std::result::Result<Self, Error> {
        let addr = addr.parse::<SocketAddr>()?;
        let socket = TcpStream::connect(&addr).await?;

        Ok(Client {
            conn: Framed::new(socket, ClientMessenger::default()),
        })
    }

    pub async fn send(&mut self, req: Request) -> std::result::Result<Response, Error> {
        if let Err(e) = self.conn.send(req).await {
            return Err(e.into());
        }

        if let Some(response) = self.conn.next().await {
            return response.map_err(|e| e.into());
        };

        Ok(Response::Ok)
    }
}
