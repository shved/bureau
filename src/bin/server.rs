use bytes::Bytes;
use futures::SinkExt;
use std::env;
use std::error::Error;
use std::sync::{Arc, Mutex};
use tokio::net::TcpListener;
use tokio_stream::StreamExt;
use tokio_util::codec::{Framed, LinesCodec};

use bureau::engine::Engine;

enum Request {
    Get { key: String },
    Set { key: String, value: String },
}

enum Response {
    Get { key: Bytes, value: Bytes },
    Set { key: String, value: String },
    Error { msg: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // TODO: Assemble config from env here.
    let addr = env::args()
        .nth(1)
        .unwrap_or_else(|| "127.0.0.1:12650".to_string());

    let listener = TcpListener::bind(&addr).await?;
    println!("Listening on: {}", addr);

    let engine = Arc::new(Mutex::new(Engine::new()));

    // TODO: Extract loop into a server module.
    loop {
        let engine_instance = engine.clone();

        match listener.accept().await {
            Ok((socket, _)) => {
                tokio::spawn(async move {
                    let mut lines = Framed::new(socket, LinesCodec::new());

                    while let Some(result) = lines.next().await {
                        match result {
                            Ok(line) => {
                                let response = handle_request(&line, &*engine_instance);

                                let response = response.serialize();

                                if let Err(e) = lines.send(response.as_str()).await {
                                    println!("error on sending response; error = {:?}", e);
                                }
                            }
                            Err(e) => {
                                println!("error on decoding from socket; error = {:?}", e);
                            }
                        }
                    }
                });
            }
            Err(e) => println!("error accepting socket; error = {:?}", e),
        }
    }
}

// TODO: Extract request handler into a module.
fn handle_request(line: &str, db: &Mutex<Engine>) -> Response {
    let request = match Request::parse(line) {
        Ok(req) => req,
        Err(e) => return Response::Error { msg: e },
    };

    let mut db = db.lock().unwrap();

    match request {
        Request::Get { key } => match db.get(key.clone().into()) {
            Some(value) => Response::Get {
                key: key.into(),
                value: value.clone(),
            },
            None => Response::Error {
                msg: format!("no key {}", key),
            },
        },
        Request::Set { key, value } => {
            db.insert(key.clone().into(), value.clone().into());
            Response::Set { key, value }
        }
    }
}

impl Request {
    fn parse(input: &str) -> Result<Request, String> {
        let mut parts = input.splitn(3, ' ');
        match parts.next() {
            Some("GET") => {
                let key = parts.next().ok_or("GET must be followed by a key")?;
                if parts.next().is_some() {
                    return Err("GET's key must not be followed by anything".into());
                }
                Ok(Request::Get {
                    key: key.to_string(),
                })
            }
            Some("SET") => {
                let key = match parts.next() {
                    Some(key) => key,
                    None => return Err("SET must be followed by a key".into()),
                };
                let value = match parts.next() {
                    Some(value) => value,
                    None => return Err("SET needs a value".into()),
                };
                Ok(Request::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                })
            }
            Some(cmd) => Err(format!("unknown command: {}", cmd)),
            None => Err("empty input".into()),
        }
    }
}

impl Response {
    fn serialize(&self) -> String {
        match *self {
            Response::Get { ref key, ref value } => format!("{:?} = {:?}", key, value),
            Response::Set { ref key, ref value } => {
                format!("set {} = `{}`", key, value)
            }
            Response::Error { ref msg } => format!("error: {}", msg),
        }
    }
}
