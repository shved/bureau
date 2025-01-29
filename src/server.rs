use crate::engine::{Command, Engine};
use crate::Storage;
use bytes::Bytes;
use futures::SinkExt;
use socket2::{SockRef, TcpKeepalive};
use std::error::Error;
use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;
use tokio::sync::{mpsc, mpsc::Sender, oneshot};
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{error, info, warn};

/// Maximum number of concurrent connections server will accept. When this limit is reached,
/// the server will stop accepting connections until an active connection terminates.
const MAX_CONN: usize = 128;

/// Requests channel capacity. It has nothing to do with connections limit, but gut feeling
/// says it should be set to somewhat higher then MAX_CONN value.
const MAX_REQUESTS: usize = 512;

/// Buffer of bytes for a single request. Taking into account keys and values size limits
/// this number will do a fine job.
const CODEC_BUFFER_SIZE: usize = 4096;

pub enum ConnLimit {
    Default,
    Is(usize),
}

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

/// Starts db engine and loop that accepts and handles connection. Signal Future is used
/// to shutdown the whole thing. Connections are limited by a given capacity.
pub async fn run<S: Storage>(
    listener: TcpListener,
    max_conn: ConnLimit,
    stor: S,
    signal: impl Future,
) -> crate::Result<(), Box<dyn Error>> {
    let (req_tx, req_rx) = mpsc::channel(MAX_REQUESTS);
    let engine_shutdown_command_tx = req_tx.clone();
    let engine = Engine::new(req_rx);
    let (network_shutdown_tx, _) = broadcast::channel::<()>(1);

    let max_conn = match max_conn {
        ConnLimit::Default => MAX_CONN,
        ConnLimit::Is(val) => val,
    };

    let engine_handle = tokio::spawn(async move {
        match engine.run(stor).await {
            Ok(()) => {
                tracing::info!("engine stoped");
            }
            Err(e) => {
                tracing::error!("engine exited with error: {:?}", e);
            }
        };
    });

    let clients_cnt = Arc::new(AtomicI64::new(0));

    let network_loop_handle = tokio::spawn({
        let mut network_shutdown_rx = network_shutdown_tx.subscribe();
        let clients_shutdown_tx = network_shutdown_tx.clone();
        let clients_cnt = clients_cnt.clone();

        async move {
            loop {
                tokio::select! {
                _ = network_shutdown_rx.recv() => {
                    info!("shutting down the server");
                    break;
                }
                socket = listener.accept() => {
                        match socket {
                            Ok((socket, _)) => {
                                if let Err(e) = apply_socket_options(&socket) {
                                    error!("setting up keep-alive options failed: {}", e);
                                    continue;
                                }

                                if clients_cnt.load(Ordering::Relaxed) >= max_conn as i64 {
                                    warn!("max connections reached, rejecting client");
                                    drop(socket);
                                    continue;
                                }

                                let req_tx = req_tx.clone();
                                let client_shutdown_rx = clients_shutdown_tx.subscribe();
                                clients_cnt.fetch_add(1, Ordering::Relaxed);
                                let clients_cnt = clients_cnt.clone();

                                tokio::spawn(async move {
                                    handle_client(socket, req_tx, client_shutdown_rx).await;
                                    clients_cnt.fetch_add(-1, Ordering::Relaxed);
                                });
                            }
                            Err(e) => {
                                error!("error accepting connection: {:?}", e);
                            }
                        }
                    }
                }
            }
        }
    });

    let engine_abort_handle = engine_handle.abort_handle();
    let network_abort_handle = network_loop_handle.abort_handle();

    // Block until either engine or network loop panic or shutdown signal comes.
    tokio::select! {
        _ = signal => {
            info!("shutdown signal received");
            let _ = network_shutdown_tx.send(());
        },
        res = engine_handle => {
            tracing::error!("engine exited: {:?}", res);
            res?;
        },
        res = network_loop_handle => {
            tracing::error!("network accept loop exited: {:?}", res);
            res?;
        }
    }

    // Block until either all clients terminated or shutdown timeout comes.
    let shutdown_timeout = Duration::from_secs(5);
    let shutdown_deadline = tokio::time::sleep(shutdown_timeout);

    tokio::select! {
        _ = shutdown_deadline => {
            warn!("forced shutdown after timeout");
        }
        _ = async {
            while clients_cnt.load(Ordering::Relaxed) > 0 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        } => {
            info!("all clients disconnected");
        }
    }

    // When all the network activity stoped tell the engine to stop.
    let (engine_shutdown_rx, engine_shutdown_tx) = oneshot::channel();
    engine_shutdown_command_tx
        .send(Command::Shutdown {
            responder: engine_shutdown_rx,
        })
        .await?;

    let _ = engine_shutdown_tx.await?;

    // Abort long running tasks just in case.
    engine_abort_handle.abort();
    network_abort_handle.abort();

    info!("bye!");

    Ok(())
}

fn apply_socket_options(socket: &TcpStream) -> Result<(), std::io::Error> {
    socket.set_nodelay(true)?;
    let sock_ref = SockRef::from(&socket);
    sock_ref.set_reuse_address(true)?;
    let mut ka = TcpKeepalive::new();
    ka = ka.with_time(Duration::from_secs(30));
    ka = ka.with_interval(Duration::from_secs(30));
    ka = ka.with_retries(3);
    sock_ref.set_tcp_keepalive(&ka)
}

/// When the new connection is accepted it is handled by this function. It runs loop
/// reading new requests from a single client. Once shutdown signal is recieved,
/// loop is exited and connection is being terminated.
async fn handle_client(
    socket: TcpStream,
    sender: Sender<Command>,
    mut shutdown: broadcast::Receiver<()>,
) {
    let mut framed_stream = Framed::new(socket, LinesCodec::new_with_max_length(CODEC_BUFFER_SIZE));

    loop {
        tokio::select! {
            result = framed_stream.next() => {
                match result {
                    Some(Ok(line)) => {
                        match Request::parse(&line) {
                            Ok(request) => {
                                let response = handle_request(request, &sender).await;
                                let serialized = response.serialize();

                                if let Err(e) = framed_stream.send(&serialized).await {
                                    warn!("error sending response: {:?}", e);
                                }
                            }
                            Err(e) => {
                                let response = Response::Error {
                                    msg: format!("could not parse command: {}", e),
                                };
                                let serialized = response.serialize();

                                if let Err(e) = framed_stream.send(serialized.as_str()).await {
                                    warn!("error sending response: {:?}", e);
                                }
                            }
                        }
                    }
                    Some(Err(e)) => {
                        error!("error reading from socket: {:?}", e);
                        // Close connection since it's probably broken. Client will reconnect.
                        break;
                    }
                    None => break, // Exit loop, connections was closed by client.
                }
            }
            _ = shutdown.recv() => {
                info!("shutdown signal received for connection");
                break; // Exit loop, connection is to shut down.
            }
        }
    }

    info!("connection closed");
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
    use crate::{client::Client, storage::mem};
    use rand::{thread_rng, Rng};
    use std::sync::Mutex;
    use tokio::net::TcpListener;
    use tokio::signal;
    use tokio::task::JoinHandle;
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

        let mut client = Client::connect(addr.to_string().as_str()).await.unwrap();

        let entries = generate_valid_entries(4000);
        let entries_to_read = entries.clone();
        for entry in entries {
            let cmd = format!(
                "SET {} {}\n",
                String::from_utf8_lossy(&entry.0),
                String::from_utf8_lossy(&entry.1)
            );

            let res = client.send(cmd).await;

            assert!(
                res.is_ok(),
                "sending SET request to server: {:?}",
                res.err()
            );
        }

        let cmd = "invalid command".to_owned();
        let res = client.send(cmd).await;

        assert!(
            res.is_ok(),
            "sending invalid request to server: {:?}",
            res.err()
        );

        for entry in entries_to_read {
            let cmd = format!("GET {}\n", String::from_utf8_lossy(&entry.0),);

            let res = client.send(cmd).await;

            assert!(res.is_ok(), "writing GET request to server");
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn test_run_random_async() {
        // Initialize server.
        let stor = mem::new();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap(); // Get the actual address

        let server_handle = tokio::spawn(async move {
            let server_result = run(listener, ConnLimit::Is(2), stor, signal::ctrl_c()).await;
            assert!(server_result.is_ok());
        });
        tokio::spawn(async move {
            tracing::error!("server thread exited: {:?}", server_handle.await);
        });

        let clients_with_data: Vec<(Client, Vec<(Bytes, Bytes)>)> = vec![
            (
                Client::connect(addr.to_string().as_str()).await.unwrap(),
                generate_valid_entries(1000),
            ),
            (
                Client::connect(addr.to_string().as_str()).await.unwrap(),
                generate_valid_entries(1000),
            ),
            (
                Client::connect(addr.to_string().as_str()).await.unwrap(),
                generate_valid_entries(1000),
            ),
        ];

        let handles: Mutex<Vec<JoinHandle<()>>> = Default::default();

        for mut cwd in clients_with_data {
            let client_handle = tokio::spawn(async move {
                for entry in cwd.1 {
                    let cmd = format!(
                        "SET {} {}\n",
                        String::from_utf8_lossy(&entry.0),
                        String::from_utf8_lossy(&entry.1)
                    );

                    let res = cwd.0.send(cmd).await;

                    assert!(
                        res.is_ok(),
                        "sending SET request to server: {:?}",
                        res.err()
                    );
                }
            });
            handles.lock().unwrap().push(client_handle);
        }

        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await; // Small delay to ensure the task is ready
            signal::ctrl_c().await.unwrap();
        });

        for h in handles.into_inner().unwrap() {
            assert!(h.await.is_ok());
        }
    }

    fn generate_valid_entries(count: usize) -> Vec<(Bytes, Bytes)> {
        (0..count)
            .map(|_| {
                (
                    generate_valid_printable_key(),
                    generate_valid_printable_value(),
                )
            })
            .collect()
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
