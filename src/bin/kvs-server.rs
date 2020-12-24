#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
#[macro_use]
extern crate failure;
use kvs::thread_pool::{RayonThreadPool, ThreadPool};
use kvs::KvsServer;
use kvs::{KvStore, SledKvStore};
use kvs::{KvsError, Result};
use log::LevelFilter;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::net::SocketAddr;
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

    let ncpu = num_cpus::get();
    let ncpu = ncpu as u32;
    let pool = RayonThreadPool::new(ncpu)?;

    let engine = get_engine(opt.engine)?;
    match engine {
        Engine::kvs => {
            let store = KvStore::open("./").unwrap();
            let server = KvsServer::new(store, pool);
            server.run(opt.addr).unwrap();
        }
        Engine::sled => {
            let store = SledKvStore::open("./").unwrap();
            let server = KvsServer::new(store, pool);
            server.run(opt.addr).unwrap();
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
