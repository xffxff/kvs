use crate::engine::Result;
use crate::thread_pool::{RayonThreadPool, ThreadPool};
use crate::{KvsEngine, Message};
use log::info;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;

pub struct KvsServer<E: KvsEngine + Sync> {
    store: E,
}

impl<E: KvsEngine + Sync> KvsServer<E> {
    pub fn new(engine: E) -> Self {
        KvsServer { store: engine }
    }

    pub fn run(self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        let ncpu = num_cpus::get();
        // let pool = SharedQueueThreadPool::new(ncpu as u32)?;
        let pool = RayonThreadPool::new(ncpu as u32)?;

        let kv_store = Arc::new(self.store);

        for stream in listener.incoming() {
            let mut stream = stream?;
            let kv_store = Arc::clone(&kv_store);
            pool.spawn(move || {
                let request = read_cmd(&mut stream).unwrap();
                let response = process_cmd(kv_store, request).unwrap();
                respond(&mut stream, response).unwrap();
            })
        }

        Ok(())
    }
}

fn read_cmd(stream: &mut TcpStream) -> Result<Message> {
    info!("connection from {:?}", stream.peer_addr()?);

    //TODO: fixed-size buffer is a bug
    let mut buffer = [0; 1024];

    let size = stream.read(&mut buffer)?;
    let request: Message = serde_json::from_slice(&buffer[..size])?;
    Ok(request)
}

fn process_cmd(kv_store: Arc<impl KvsEngine + Sync>, msg: Message) -> Result<Message> {
    let response = match msg {
        Message::Set { ref key, ref value } => {
            kv_store.set(key.to_owned(), value.to_owned())?;
            Message::Reply {
                reply: "Ok".to_owned(),
            }
        }
        Message::Get { ref key } => match kv_store.get(key.to_owned())? {
            Some(value) => Message::Reply { reply: value },
            None => Message::Reply {
                reply: "Key not found".to_owned(),
            },
        },
        Message::Remove { ref key } => match kv_store.remove(key.to_owned()) {
            Err(_) => Message::Err {
                err: "Key not found".to_owned(),
            },
            Ok(_) => Message::Reply {
                reply: "Ok".to_owned(),
            },
        },
        _ => Message::Err {
            err: "Invalid command".to_owned(),
        },
    };
    Ok(response)
}

fn respond(stream: &mut TcpStream, resp: Message) -> Result<()> {
    let response = serde_json::to_vec(&resp)?;
    stream.write_all(&response)?;
    Ok(())
}
