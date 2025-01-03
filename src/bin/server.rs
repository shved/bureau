use bureau::lsm::{Command, Engine};
use bureau::storage;
use bureau::storage::DataPath;
use bytes::Bytes;
use futures::SinkExt;
use std::env;
use std::error::Error;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};
use tracing::{error, info, warn};

enum Request {
    Get { key: String },
    Set { key: String, value: String },
}

enum Response {
    Get { key: String, value: Bytes },
    Set { key: String, value: Bytes },
    Error { msg: String },
}

#[tokio::main]
async fn main() -> bureau::Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt::init();
    // TODO: Assemble config from env here.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:12650".to_string());

    let listener = TcpListener::bind(&addr).await?;
    info!("Listening on: {}", addr);

    let (req_tx, req_rx) = mpsc::channel(64);
    let stor = storage::new(DataPath::Default);
    let engine = Engine::new(req_rx, stor);

    let engine_handle = tokio::spawn(async move {
        engine.run().await;
        tracing::error!("engine exited");
    });

    let network_loop_handle = tokio::spawn(async move {
        loop {
            let req_tx = req_tx.clone();

            match listener.accept().await {
                Ok((socket, _)) => {
                    tokio::spawn(async move {
                        let mut lines = Framed::new(socket, LinesCodec::new());

                        if let Some(result) = lines.next().await {
                            match result {
                                Ok(line) => match Request::parse(&line) {
                                    Ok(request) => {
                                        let response = handle_request(request, req_tx).await;
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
                                }
                            }
                        }
                    });
                }
                Err(e) => error!("error accepting socket; error = {:?}", e),
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

async fn handle_request(request: Request, req_tx: mpsc::Sender<Command>) -> Response {
    match request {
        Request::Get { key } => {
            let (resp_tx, resp_rx) = oneshot::channel();

            let cmd = Command::Get {
                key: Bytes::from(key.clone()),
                responder: resp_tx,
            };

            if let Err(e) = req_tx.send(cmd).await {
                return Response::Error { msg: e.to_string() };
            }

            let resp = resp_rx.await;

            // TODO: Log error instead of sending it to client;
            if resp.is_err() {
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
                responder: resp_tx,
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
    fn parse(input: &str) -> bureau::Result<Request> {
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
            Response::Get { ref key, ref value } => format!("{:?} = {:?}", key, value),
            Response::Set { ref key, ref value } => {
                format!("set {} = `{:?}`", key, value)
            }
            Response::Error { ref msg } => format!("error: {}", msg),
        }
    }
}
