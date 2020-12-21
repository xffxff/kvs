#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsEngine, SledKvStore};
use kvs::{KvsError, Message, Result};
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::io::prelude::*;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::sync::Arc;
use std::{fs::OpenOptions, net::TcpStream};
use structopt::StructOpt;

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    enum Engine {
        kvs,
        sled
    }
}

#[derive(Debug, StructOpt)]
pub struct ApplicationArguments {
    #[structopt(long = "addr", default_value = "127.0.0.1:4000")]
    addr: SocketAddr,

    #[structopt(long = "engine", possible_values = &Engine::variants())]
    engine: Option<Engine>,
}

fn main() -> Result<()> {
    simple_logging::log_to_stderr(LevelFilter::Info);
    let opt = ApplicationArguments::from_args();

    info!("server version: {}", env!("CARGO_PKG_VERSION"));
    info!("IP:PORT {:?}", opt.addr);
    info!("Engine: {:?}", opt.engine);

    let engine = get_engine(opt.engine)?;
    match engine {
        Engine::kvs => {
            run(KvStore::open("./").unwrap(), opt.addr).unwrap();
        }
        Engine::sled => {
            run(SledKvStore::open("./").unwrap(), opt.addr).unwrap();
        }
    }
    Ok(())
}

fn run(kv_store: impl KvsEngine + Sync, addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr)?;

    let ncpu = num_cpus::get();
    // let pool = SharedQueueThreadPool::new(ncpu as u32)?;
    let pool = RayonThreadPool::new(ncpu as u32)?;

    let kv_store = Arc::new(kv_store);

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

fn get_engine(possible_engine: Option<Engine>) -> Result<Engine> {
    let mut persisted_engine: Option<Engine> = None;
    if Path::new("config").exists() {
        let f = OpenOptions::new().read(true).open("config")?;
        let engine = serde_json::from_reader(f)?;
        persisted_engine = Some(engine);
    }

    let engine: Engine;
    match possible_engine {
        Some(v) => match persisted_engine {
            Some(p) => {
                if v != p {
                    return Err(KvsError::MismatchEngine);
                }
                engine = v;
            }
            None => engine = v,
        },
        None => match persisted_engine {
            Some(v) => engine = v,
            None => engine = Engine::kvs,
        },
    }

    let f = OpenOptions::new()
        .write(true)
        .read(true)
        .truncate(true)
        .create(true)
        .open("config")?;

    match engine {
        Engine::kvs => {
            // let kv_store = KvStore::open("./")?;
            serde_json::to_writer(f, &engine)?;
            Ok(Engine::kvs)
            // Ok(Box::new(kv_store))
        }
        Engine::sled => {
            // let kv_store = SledKvStore::open("./")?;
            serde_json::to_writer(f, &engine)?;
            Ok(Engine::sled)
            // Ok(Box::new(kv_store))
        }
    }
}
