use crate::thread_pool::ThreadPool;
use crate::KvsEngine;
use crate::{engine::Result, KvsError};
use crate::{Request, Response};
use log::info;
use std::io;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;

pub struct KvsServer<E: KvsEngine, T: ThreadPool> {
    store: E,
    pool: T,
    receiver: Option<mpsc::Receiver<()>>,
}

impl<E: KvsEngine, T: ThreadPool> KvsServer<E, T> {
    pub fn new(engine: E, pool: T, receiver: Option<mpsc::Receiver<()>>) -> Self {
        KvsServer {
            store: engine,
            pool: pool,
            receiver: receiver,
        }
    }

    pub fn run(self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        listener.set_nonblocking(true).unwrap();

        for stream in listener.incoming() {
            if let Some(rx) = self.receiver.as_ref() {
                if rx.try_recv().is_ok() {
                    break;
                }
            };
            match stream {
                Ok(mut s) => {
                    let kv_store = self.store.clone();
                    self.pool.spawn(move || {
                        let request = read_cmd(&mut s).unwrap();
                        let response = process_cmd(kv_store, request).unwrap();
                        respond(&mut s, response).unwrap();
                    })
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => return Err(KvsError::IoError(e)),
            }
        }

        Ok(())
    }
}

fn read_cmd(stream: &mut TcpStream) -> Result<Request> {
    info!("connection from {:?}", stream.peer_addr()?);

    //TODO: fixed-size buffer is a bug
    let mut buffer = [0; 1024];

    let size = stream.read(&mut buffer)?;
    let request: Request = serde_json::from_slice(&buffer[..size])?;
    Ok(request)
}

fn process_cmd(kv_store: impl KvsEngine, msg: Request) -> Result<Response> {
    let response = match msg {
        Request::Set { ref key, ref value } => {
            kv_store.set(key.to_owned(), value.to_owned())?;
            Response::Ok(None)
        }
        Request::Get { ref key } => match kv_store.get(key.to_owned())? {
            Some(value) => Response::Ok(Some(value)),
            None => Response::Ok(None),
        },
        Request::Remove { ref key } => match kv_store.remove(key.to_owned()) {
            Err(_) => Response::Err("Key not found".to_owned()),
            Ok(_) => Response::Ok(None),
        },
    };
    Ok(response)
}

fn respond(stream: &mut TcpStream, resp: Response) -> Result<()> {
    let response = serde_json::to_vec(&resp)?;
    stream.write_all(&response)?;
    Ok(())
}
