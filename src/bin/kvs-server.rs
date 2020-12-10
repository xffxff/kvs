#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
use env_logger::Env;
use kvs::Message;
use kvs::Result;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::net::TcpListener;
use structopt::StructOpt;
use kvs::KvStore;
use kvs::KvsEngine;

arg_enum! {
    #[allow(non_camel_case_types)]
    #[derive(Debug)]
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
    let env = Env::default()
        .filter_or("MY_LOG_LEVEL", "trace")
        .write_style_or("MY_LOG_STYLE", "always");
    env_logger::init_from_env(env);
    let opt = ApplicationArguments::from_args();

    info!("server version: {}", env!("CARGO_PKG_VERSION"));
    info!("IP:PORT {:?}", opt.addr);
    info!("Engine: {:?}", opt.engine);

    let mut kv_store = KvStore::open("./")?;

    let listener = TcpListener::bind(opt.addr)?;

    for stream in listener.incoming() {
        let mut stream = stream.unwrap();
        info!("connection from {:?}", stream.peer_addr().unwrap());

        let mut buffer = [0; 1024];

        let size = stream.read(&mut buffer).unwrap();
        let request: Message = serde_json::from_slice(&buffer[..size]).unwrap();
        match request {
            Message::Set { ref key, ref value} => {
                kv_store.set(key.to_owned(), value.to_owned())?;
            },
            Message::Get { ref key} => {
                match kv_store.get(key.to_owned())? {
                    Some(value) => {
                        let response = Message::Reply { key: value.to_owned() };
                        let response = serde_json::to_vec(&response)?;
                        stream.write_all(&response)?;
                    }
                    None => {
                        let response = Message::Reply { key: "Key not found".to_owned() };
                        let response = serde_json::to_vec(&response)?;
                        stream.write_all(&response)?;
                    }
                }
            },
            Message::Remove { ref key} => {
                match kv_store.remove(key.to_owned()) {
                    Err(_) => {
                        let response = Message::Err { key: "Key not found".to_owned() };
                        let response = serde_json::to_vec(&response)?;
                        stream.write_all(&response)?;
                    },
                    Ok(_) => {
                        let response = Message::Reply { key: "Ok".to_owned() };
                        let response = serde_json::to_vec(&response)?;
                        stream.write_all(&response)?;
                    }
                }
            },
            _ => {}
        }
    }

    Ok(())
}
