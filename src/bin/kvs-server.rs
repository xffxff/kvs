#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
use kvs::KvStore;
use kvs::KvsEngine;
use kvs::Message;
use kvs::Result;
use kvs::SledKVStore;
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::net::TcpListener;
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

    let mut kv_store = get_engine(opt.engine)?;

    let listener = TcpListener::bind(opt.addr)?;

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        info!("connection from {:?}", stream.peer_addr().unwrap());

        let mut buffer = [0; 1024];

        let size = stream.read(&mut buffer).unwrap();
        let request: Message = serde_json::from_slice(&buffer[..size]).unwrap();
        match request {
            Message::Set { ref key, ref value } => {
                kv_store.set(key.to_owned(), value.to_owned())?;
            }
            Message::Get { ref key } => match kv_store.get(key.to_owned())? {
                Some(value) => {
                    let response = Message::Reply {
                        key: value.to_owned(),
                    };
                    let response = serde_json::to_vec(&response)?;
                    stream.write_all(&response)?;
                }
                None => {
                    let response = Message::Reply {
                        key: "Key not found".to_owned(),
                    };
                    let response = serde_json::to_vec(&response)?;
                    stream.write_all(&response)?;
                }
            },
            Message::Remove { ref key } => match kv_store.remove(key.to_owned()) {
                Err(_) => {
                    let response = Message::Err {
                        key: "Key not found".to_owned(),
                    };
                    let response = serde_json::to_vec(&response)?;
                    stream.write_all(&response)?;
                }
                Ok(_) => {
                    let response = Message::Reply {
                        key: "Ok".to_owned(),
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

fn get_engine(possible_engine: Option<Engine>) -> Result<Box<dyn KvsEngine>> {
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
                    return Err(format_err!("mismatch engine"));
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
            let kv_store = KvStore::open("./")?;
            serde_json::to_writer(f, &engine)?;
            Ok(Box::new(kv_store))
        }
        Engine::sled => {
            let kv_store = SledKVStore::open("./")?;
            serde_json::to_writer(f, &engine)?;
            Ok(Box::new(kv_store))
        }
    }
}
