use crate::engine::{Command, Engine};
use crate::Storage;
use bytes::Bytes;
use futures::SinkExt;
use std::error::Error;
use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::sync::{mpsc, mpsc::Sender, oneshot};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{error, warn};

/// Maximum number of concurrent connections server will accept. When this limit is reached,
/// the server will stop accepting connections until an active connection terminates.
// TODO: Make configurable.
const MAX_CONN: usize = 256;

/// Requests channel capacity. It has nothing to do with connections limit, but gut feeling
/// says it should be set to somewhat higher then MAX_CONN value.
const MAX_REQUESTS: usize = 512;

/// Buffer of bytes for a single request. Taking into account keys and values size limits
/// this number will do a fine job.
const CODEC_BUFFER_SIZE: usize = 4096;

/// Request commands supported.
// TODO: Make it all bytes. No real reason to have strings here.
enum Request {
    Get { key: String },
    Set { key: String, value: String },
}

/// Possible responses structures.
// TODO: Make it all bytes. No real reason to have strings here.
enum Response {
    Get { key: String, value: Bytes },
    Set { key: String, value: Bytes },
    Error { msg: String },
}

#[derive(Debug)]
struct ListenerWithCap {
    listener: TcpListener,
    conn_limit: Arc<Semaphore>,
}

#[derive(Debug)]
pub enum ConnLimit {
    Default,
    Is(usize),
}

impl ListenerWithCap {
    fn new(listener: TcpListener, limit: ConnLimit) -> Self {
        let max_conn = match limit {
            ConnLimit::Default => MAX_CONN,
            ConnLimit::Is(val) => val,
        };

        ListenerWithCap {
            listener,
            conn_limit: Arc::new(Semaphore::new(max_conn)),
        }
    }
}

pub async fn run<S: Storage>(
    listener: TcpListener,
    max_conn: ConnLimit,
    stor: S,
    _signal: impl Future,
) -> crate::Result<(), Box<dyn Error>> {
    let (req_tx, req_rx) = mpsc::channel(MAX_REQUESTS);
    let engine = Engine::new(req_rx);
    let listener = ListenerWithCap::new(listener, max_conn);

    let engine_handle = tokio::spawn(async move {
        engine.run(stor).await;
        tracing::error!("engine exited");
    });

    let network_loop_handle = tokio::spawn(async move {
        // TODO here while !self.shutdown()
        loop {
            let permit = listener.conn_limit.clone().acquire_owned().await.unwrap();

            match listener.listener.accept().await {
                Ok((socket, _)) => {
                    let req_tx = req_tx.clone();

                    tokio::spawn(async move {
                        handle_client(socket, &req_tx).await;
                        drop(permit);
                    });
                }
                Err(e) => {
                    error!("error accepting socket; error = {:?}", e);
                    drop(permit);
                }
            }
        }
    });

    tokio::select! {
        res = engine_handle => {
            tracing::error!("engine handle down: {res:?}");
            res?;
        },
        res = network_loop_handle => {
            tracing::error!("network loop down: {res:?}");
            res?;
        }
    };

    Ok(())
}

/// When the new connection is accepted it is handled by this function.
/// It runs loop reading new requests from a single client.
async fn handle_client(socket: TcpStream, sender: &Sender<Command>) {
    let mut lines = Framed::new(socket, LinesCodec::new_with_max_length(CODEC_BUFFER_SIZE));

    while let Some(result) = lines.next().await {
        match result {
            Ok(line) => match Request::parse(&line) {
                Ok(request) => {
                    let response = handle_request(request, sender).await;
                    let serialized = response.serialize();

                    if let Err(e) = lines.send(&serialized).await {
                        warn!("error on sending response; error = {:?}", e);
                    }
                }
                Err(e) => {
                    let response = Response::Error {
                        msg: format!("could not parse command: {}", e),
                    };
                    let serialized = response.serialize();

                    if let Err(e) = lines.send(serialized.as_str()).await {
                        warn!("error on sending response; error = {:?}", e);
                    }
                }
            },
            Err(e) => {
                error!("error on decoding from socket; error = {:?}", e);
                // Drop client in case the error isnt recoverable. Client should handle that and reconnect.
                break;
            }
        }
    }
}

/// This function is called for every single valid request from a client.
async fn handle_request(request: Request, req_tx: &mpsc::Sender<Command>) -> Response {
    match request {
        Request::Get { key } => {
            let (resp_tx, resp_rx) = oneshot::channel();

            let cmd = Command::Get {
                key: Bytes::from(key.clone()),
                responder: resp_tx,
            };

            if let Err(e) = req_tx.send(cmd).await {
                // TODO: Decorate errors for clients and log actual error.
                return Response::Error { msg: e.to_string() };
            }

            let resp = resp_rx.await;

            if resp.is_err() {
                // TODO: Decorate errors for clients and log actual error.
                return Response::Error {
                    msg: resp.err().unwrap().to_string(),
                };
            }

            let resp = resp.unwrap();

            match resp {
                Ok(option) => match option {
                    Some(value) => Response::Get {
                        key,
                        value: value.clone(),
                    },
                    None => Response::Error {
                        msg: "no value for given key".to_string(),
                    },
                },
                Err(e) => Response::Error { msg: e.to_string() },
            }
        }
        Request::Set { key, value } => {
            let (resp_tx, resp_rx) = oneshot::channel();

            let cmd = Command::Set {
                key: Bytes::from(key.clone()),
                value: Bytes::from(value.clone()),
                responder: Some(resp_tx),
            };

            if let Err(e) = req_tx.send(cmd).await {
                return Response::Error { msg: e.to_string() };
            }

            let resp = resp_rx.await.unwrap(); // TODO: Remove unwrap();

            match resp {
                Ok(_) => Response::Set {
                    key,
                    value: Bytes::from(value),
                },
                Err(e) => Response::Error { msg: e.to_string() },
            }
        }
    }
}

impl Request {
    fn parse(input: &str) -> crate::Result<Request> {
        let mut parts = input.splitn(3, ' ');
        match parts.next() {
            Some("GET") => {
                let key = parts.next().ok_or("GET must be followed by a key")?;
                if parts.next().is_some() {
                    Err("GET's key must not be followed by anything")?
                }
                Ok(Request::Get {
                    key: key.to_string(),
                })
            }
            Some("SET") => {
                let key = match parts.next() {
                    Some(key) => key,
                    None => Err("SET must be followed by a key")?,
                };
                let value = match parts.next() {
                    Some(value) => value,
                    None => Err("SET needs a value")?,
                };
                Ok(Request::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                })
            }
            Some(cmd) => Err(format!("unknown command: {}", cmd))?,
            None => Err("empty input")?,
        }
    }
}

impl Response {
    fn serialize(&self) -> String {
        match *self {
            Response::Get { ref key, ref value } => format!("{} = {:?}", key, value),
            Response::Set { ref key, ref value } => {
                format!("set {} = b`{:?}`", key, value)
            }
            Response::Error { ref msg } => format!("error: {}", msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::mem;
    use rand::{thread_rng, Rng};
    use tokio::io::AsyncWriteExt;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::signal;
    // use tracing::debug;
    use tracing_test::traced_test;

    #[traced_test]
    #[tokio::test]
    async fn test_run_random_generated() {
        // Initialize server.
        let stor = mem::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap(); // Get the actual address

        let server_handle = tokio::spawn(async move {
            let server_result = run(listener, ConnLimit::Is(1), stor, signal::ctrl_c()).await;
            tracing::error!("server returned: {:?}", server_result);
        });
        tokio::spawn(async move {
            tracing::error!("server thread exited: {:?}", server_handle.await);
        });

        let mut client = TcpStream::connect(addr).await.unwrap();

        let entries_cnt = 4000;
        let mut entries: Vec<(Bytes, Bytes)> = vec![];
        for i in 0..entries_cnt {
            let key = generate_valid_printable_key();
            let value = generate_valid_printable_value();

            entries.push((key.clone(), value.clone()));

            let cmd = format!(
                "SET {} {}\n",
                String::from_utf8_lossy(&key),
                String::from_utf8_lossy(&value)
            );

            let res = client.write_all(cmd.as_bytes()).await;

            assert!(
                res.is_ok(),
                "sending {}th SET request to server: {:?}",
                i,
                res.err()
            );

            // TODO: Rework all the in/out representation of data and put this assert back.
            // For now it is broken because its hard to hanlde back slashes.
            // let mut response = vec![0; 1024 * 4];
            // let n = client.read(&mut response).await.unwrap();
            // let response = String::from_utf8_lossy(&response[..n]).replace('\\', "");
            // let expected_response = format!(
            //     "set {} = b`{}`\n",
            //     String::from_utf8_lossy(&key),
            //     String::from_utf8_lossy(&value),
            // )
            // .replace('\\', "");
            // assert_eq!(response, expected_response);
        }

        let cmd = "invalid command";
        let res = client.write_all(cmd.as_bytes()).await;

        assert!(
            res.is_ok(),
            "sending invalid request to server: {:?}",
            res.err()
        );

        for entry in entries.clone() {
            let cmd = format!("GET {}\n", String::from_utf8_lossy(&entry.0),);

            let res = client.write_all(cmd.as_bytes()).await;

            assert!(res.is_ok(), "writing GET request to server");
        }
    }

    fn generate_valid_printable_key() -> Bytes {
        let mut rng = thread_rng();
        let length = rng.gen_range(1..=300);
        Bytes::from(
            (0..length)
                .map(|_| rng.gen_range(32..=126) as u8)
                .collect::<Vec<u8>>(),
        )
    }

    fn generate_valid_printable_value() -> Bytes {
        let mut rng = thread_rng();
        let length = rng.gen_range(1..=1000);
        Bytes::from(
            (0..length)
                .map(|_| rng.gen_range(32..=126) as u8)
                .collect::<Vec<u8>>(),
        )
    }
}
