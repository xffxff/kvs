use kvs::{KvStore, Result};
use std::path::Path;
use structopt::StructOpt;
use std::net::SocketAddr;
use std::net::TcpStream;
use kvs::protocol;
use std::io::prelude::*;

#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "set", about = "Stores a key/value pair")]
    Set { 
        key: String, 
        value: String, 
        #[structopt(long = "addr", default_value = "127.0.0.1:4000")]
        addr: SocketAddr
    },
    #[structopt(name = "get", about = "Gets value according to the key")]
    Get { 
        key: String,
        #[structopt(long = "addr", default_value = "127.0.0.1:4000")]
        addr: SocketAddr
    },
    #[structopt(name = "rm", about = "Removes key/value pair according to the key")]
    Remove { 
        key: String,
        #[structopt(long = "addr", default_value = "127.0.0.1:4000")]
        addr: SocketAddr
    },
}

#[derive(Debug, StructOpt)]
pub struct ApplicationArguments {
    #[structopt(subcommand)]
    pub command: Command,
}

fn main() -> Result<()> {
    let opt = ApplicationArguments::from_args();

    match opt.command {
        Command::Set { ref key, ref value, ref addr } => {
            let mut stream = TcpStream::connect(addr).unwrap();
            let request = protocol::Message::Set { key: key.to_owned(), value: value.to_owned() };
            let request = serde_json::to_vec(&request).unwrap();
            stream.write(&request).unwrap();
        }
        Command::Get { ref key, ref addr } => {
            let mut stream = TcpStream::connect(addr).unwrap();
            let request = protocol::Message::Get { key: key.to_owned() };
            let request = serde_json::to_vec(&request).unwrap();
            stream.write(&request).unwrap();
        },
        Command::Remove { ref key, ref addr } => {
            let mut stream = TcpStream::connect(addr).unwrap();
            let request = protocol::Message::Remove { key: key.to_owned() };
            let request = serde_json::to_vec(&request).unwrap();
            stream.write(&request).unwrap();
        }
    }
    Ok(())
}
