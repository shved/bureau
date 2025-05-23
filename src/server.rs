use crate::engine::{Command, Engine};
use crate::protocol::{Request, Response, ServerMessenger};
use crate::{Storage, WalStorage};
use bytes::Bytes;
use futures::SinkExt;
use socket2::{SockRef, TcpKeepalive};
use std::future::Future;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, mpsc::Sender, oneshot};
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

/// Maximum number of concurrent connections server will accept. When this limit is reached,
/// the server will stop accepting connections until an active connection terminates.
const MAX_CONN: usize = 128;

/// Requests channel capacity. It has nothing to do with connections limit, but gut feeling
/// says it should be set to somewhat higher then MAX_CONN value.
const MAX_REQUESTS: usize = 512;

pub enum ConnLimit {
    Default,
    Is(usize),
}

/// Starts db engine and loop that accepts and handles connection. Signal Future is used
/// to shutdown the whole thing. Connections are limited by a given capacity.
pub async fn run<S: Storage, W: WalStorage>(
    listener: TcpListener,
    max_conn: ConnLimit,
    storage: S,
    wal_storage: W,
    signal: impl Future,
) -> crate::Result<()>
where
    <S as Storage>::Entry: Send,
{
    storage
        .bootstrap()
        .map_err(|e| format!("could not setup storage: {}", e))?;

    let (req_tx, req_rx) = mpsc::channel(MAX_REQUESTS);
    let engine_shutdown_command_tx = req_tx.clone();
    let engine =
        Engine::init(req_rx, wal_storage).map_err(|e| format!("could not setup engine: {}", e))?;
    let (network_shutdown_tx, _) = broadcast::channel::<()>(1);

    let max_conn = match max_conn {
        ConnLimit::Default => MAX_CONN,
        ConnLimit::Is(val) => val,
    };

    let engine_handle = tokio::spawn(async move {
        match engine.run(storage).await {
            Ok(()) => {
                info!("engine stoped");
            }
            Err(e) => {
                error!("engine exited with error: {:?}", e);
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
            error!("engine exited: {:?}", res);
            res?;
        },
        res = network_loop_handle => {
            error!("network accept loop exited: {:?}", res);
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
    let mut framed_stream = Framed::new(socket, ServerMessenger::default());

    info!("connection established");

    loop {
        tokio::select! {
            result = framed_stream.next() => {
                match result {
                    Some(Ok(request)) => {
                        let response = handle_request(request, &sender).await;

                        if let Err(e) = framed_stream.send(response).await {
                            warn!("error sending response: {:?}", e);
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
                key: key.clone(),
                responder: resp_tx,
            };

            if let Err(e) = req_tx.send(cmd).await {
                // TODO: Decorate errors for clients and log actual error.
                return Response::Error {
                    message: Bytes::from(e.to_string()),
                };
            }

            let resp = resp_rx.await;

            if resp.is_err() {
                // TODO: Decorate errors for clients and log actual error.
                return Response::Error {
                    message: Bytes::from(resp.err().unwrap().to_string()),
                };
            }

            let resp = resp.unwrap();

            match resp {
                Ok(option) => match option {
                    Some(value) => Response::OkValue {
                        value: value.clone(),
                    },
                    None => Response::Error {
                        message: Bytes::from("no value for given key"),
                    },
                },
                Err(e) => Response::Error {
                    message: Bytes::from(e.to_string()),
                },
            }
        }
        Request::Set { key, value } => {
            let (resp_tx, resp_rx) = oneshot::channel();

            let cmd = Command::Set {
                key: key.clone(),
                value: value.clone(),
                responder: Some(resp_tx),
            };

            if let Err(e) = req_tx.send(cmd).await {
                return Response::Error {
                    message: Bytes::from(e.to_string()),
                };
            }

            let resp = resp_rx.await.unwrap(); // TODO: Remove unwrap();

            match resp {
                Ok(_) => Response::Ok,
                Err(e) => Response::Error {
                    message: Bytes::from(e.to_string()),
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::Request;
    use crate::wal::mem_storage::{InitialState, MemStorage};
    use crate::{client::Client, storage::mem};
    use rand::{rng, Rng};
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
        let wal_stor = MemStorage::init(InitialState::Blank).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap(); // Get the actual address

        let server_handle = tokio::spawn(async move {
            let server_result =
                run(listener, ConnLimit::Is(1), stor, wal_stor, signal::ctrl_c()).await;
            tracing::error!("server returned: {:?}", server_result);
        });
        tokio::spawn(async move {
            tracing::error!("server thread exited: {:?}", server_handle.await);
        });

        let mut client = Client::connect(addr.to_string().as_str()).await.unwrap();

        let entries = generate_valid_entries(4000);
        let entries_to_read = entries.clone();
        for entry in entries {
            let cmd = Request::Set {
                key: entry.0,
                value: entry.1,
            };
            let res = client.send(cmd).await;

            assert!(
                res.is_ok(),
                "sending SET request to server: {:?}",
                res.err()
            );
        }

        for entry in entries_to_read {
            let cmd = Request::Get { key: entry.0 };

            let res = client.send(cmd).await;

            assert!(res.is_ok(), "writing GET request to server");
        }
    }

    #[traced_test]
    #[tokio::test]
    async fn test_run_random_async() {
        let requests_count = 1000;
        let stor = mem::new();
        let wal_stor = MemStorage::init(InitialState::Blank).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap(); // Get the actual address

        let server_handle = tokio::spawn(async move {
            let server_result =
                run(listener, ConnLimit::Is(2), stor, wal_stor, signal::ctrl_c()).await;
            assert!(server_result.is_ok());
        });
        tokio::spawn(async move {
            match server_handle.await {
                Err(e) => tracing::error!("server thread exited: {:?}", e),
                Ok(()) => tracing::info!("server exited"),
            }
        });

        let clients_with_data: Vec<(Client, Vec<(Bytes, Bytes)>)> = vec![
            (
                Client::connect(addr.to_string().as_str()).await.unwrap(),
                generate_valid_entries(requests_count),
            ),
            (
                Client::connect(addr.to_string().as_str()).await.unwrap(),
                generate_valid_entries(requests_count),
            ),
            (
                Client::connect(addr.to_string().as_str()).await.unwrap(),
                generate_valid_entries(requests_count),
            ),
        ];

        let handles: Mutex<Vec<JoinHandle<()>>> = Default::default();
        let responses: Arc<Mutex<Vec<Result<Response, anyhow::Error>>>> =
            Arc::new(Mutex::new(vec![]));

        for mut cwd in clients_with_data {
            let responses = Arc::clone(&responses);
            let client_handle = tokio::spawn(async move {
                for entry in cwd.1 {
                    let cmd = Request::Set {
                        key: entry.0,
                        value: entry.1,
                    };

                    let res = cwd.0.send(cmd).await;
                    responses.lock().unwrap().push(res);
                }
            });
            handles.lock().unwrap().push(client_handle);
        }

        tokio::spawn(async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            signal::ctrl_c().await.unwrap();
        });

        for h in handles.into_inner().unwrap() {
            assert!(h.await.is_ok());
        }

        let responses = responses.lock().unwrap();
        let error_count = responses.iter().filter(|res| res.is_err()).count();

        // We assume here one of three clients will struggle since the server connection
        // limit is set to be 2. This means there will be error responses but amount of those
        // will never be higher then the amount of requests sent by a client (requests_count).
        assert!(
            error_count < requests_count,
            "error count is too high: {}",
            error_count
        );
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
        let mut rng = rng();
        let length = rng.random_range(1..=300);
        Bytes::from(
            (0..length)
                .map(|_| rng.random_range(32..=126) as u8)
                .collect::<Vec<u8>>(),
        )
    }

    fn generate_valid_printable_value() -> Bytes {
        let mut rng = rng();
        let length = rng.random_range(1..=1000);
        Bytes::from(
            (0..length)
                .map(|_| rng.random_range(32..=126) as u8)
                .collect::<Vec<u8>>(),
        )
    }
}
