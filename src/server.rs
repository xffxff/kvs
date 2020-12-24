use crate::engine::Result;
use crate::thread_pool::ThreadPool;
use crate::KvsEngine;
use crate::{Request, Response};
use log::info;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};

pub struct KvsServer<E: KvsEngine, T: ThreadPool> {
    store: E,
    pool: T,
}

impl<E: KvsEngine, T: ThreadPool> KvsServer<E, T> {
    pub fn new(engine: E, pool: T) -> Self {
        KvsServer {
            store: engine,
            pool,
        }
    }

    pub fn run(self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            let mut stream = stream?;
            let kv_store = self.store.clone();
            self.pool.spawn(move || {
                let request = read_cmd(&mut stream).unwrap();
                let response = process_cmd(kv_store, request).unwrap();
                respond(&mut stream, response).unwrap();
            })
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
