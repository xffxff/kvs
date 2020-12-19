#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
use kvs::{KvStore, KvsEngine, SledKvStore};
use kvs::{KvsError, Message, Result};
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
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

fn run(kv_store: impl KvsEngine, addr: SocketAddr) -> Result<()> {
    let listener = TcpListener::bind(addr)?;

    for stream in listener.incoming() {
        let mut stream = stream?;
        info!("connection from {:?}", stream.peer_addr()?);

        let mut buffer = [0; 1024];

        let size = stream.read(&mut buffer)?;
        let request: Message = serde_json::from_slice(&buffer[..size])?;
        match request {
            Message::Set { ref key, ref value } => {
                kv_store.set(key.to_owned(), value.to_owned())?;
            }
            Message::Get { ref key } => match kv_store.get(key.to_owned())? {
                Some(value) => {
                    let response = Message::Reply {
                        reply: value.to_owned(),
                    };
                    let response = serde_json::to_vec(&response)?;
                    stream.write_all(&response)?;
                }
                None => {
                    let response = Message::Reply {
                        reply: "Key not found".to_owned(),
                    };
                    let response = serde_json::to_vec(&response)?;
                    stream.write_all(&response)?;
                }
            },
            Message::Remove { ref key } => match kv_store.remove(key.to_owned()) {
                Err(_) => {
                    let response = Message::Err {
                        err: "Key not found".to_owned(),
                    };
                    let response = serde_json::to_vec(&response)?;
                    stream.write_all(&response)?;
                }
                Ok(_) => {
                    let response = Message::Reply {
                        reply: "Ok".to_owned(),
                    };
                    let response = serde_json::to_vec(&response)?;
                    stream.write_all(&response)?;
                }
            },
            _ => {}
        }
    }

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
