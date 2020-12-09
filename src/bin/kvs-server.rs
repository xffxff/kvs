#[macro_use]
extern crate clap;
#[macro_use]
extern crate log;
use env_logger::Env;
use kvs::protocol;
use kvs::Result;
use std::io::prelude::*;
use std::net::SocketAddr;
use std::net::TcpListener;
use std::net::TcpStream;
use structopt::StructOpt;

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

    let listener = TcpListener::bind(opt.addr)?;

    for stream in listener.incoming() {
        let stream = stream.unwrap();
        info!("connection from {:?}", stream.peer_addr().unwrap());

        handle_connection(stream);
    }

    Ok(())
}

fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0; 1024];

    let size = stream.read(&mut buffer).unwrap();
    let request: protocol::Message = serde_json::from_slice(&buffer[..size]).unwrap();
    println!("{:?}", request);
}
