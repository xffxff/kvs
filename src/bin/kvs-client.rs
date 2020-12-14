#[macro_use]
extern crate failure;
use kvs::Message;
use kvs::Result;
use std::io::prelude::*;
use std::net::{SocketAddr, TcpStream};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "set", about = "Stores a key/value pair")]
    Set {
        key: String,
        value: String,
        #[structopt(long = "addr", default_value = "127.0.0.1:4000")]
        addr: SocketAddr,
    },
    #[structopt(name = "get", about = "Gets value according to the key")]
    Get {
        key: String,
        #[structopt(long = "addr", default_value = "127.0.0.1:4000")]
        addr: SocketAddr,
    },
    #[structopt(name = "rm", about = "Removes key/value pair according to the key")]
    Remove {
        key: String,
        #[structopt(long = "addr", default_value = "127.0.0.1:4000")]
        addr: SocketAddr,
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
        Command::Set {
            ref key,
            ref value,
            ref addr,
        } => {
            let mut stream = TcpStream::connect(addr).unwrap();
            let request = Message::Set {
                key: key.to_owned(),
                value: value.to_owned(),
            };
            let request = serde_json::to_vec(&request).unwrap();
            stream.write_all(&request).unwrap();
        }
        Command::Get { ref key, ref addr } => {
            let mut stream = TcpStream::connect(addr).unwrap();
            let request = Message::Get {
                key: key.to_owned(),
            };
            let request = serde_json::to_vec(&request).unwrap();
            stream.write_all(&request).unwrap();

            let mut buffer = [0; 1024];
            let size = stream.read(&mut buffer).unwrap();
            let response: Message = serde_json::from_slice(&buffer[..size])?;
            if let Message::Reply { ref reply } = response {
                println!("{}", reply);
            }
        }
        Command::Remove { ref key, ref addr } => {
            let mut stream = TcpStream::connect(addr).unwrap();
            let request = Message::Remove {
                key: key.to_owned(),
            };
            let request = serde_json::to_vec(&request).unwrap();
            stream.write_all(&request).unwrap();

            let mut buffer = [0; 1024];
            let size = stream.read(&mut buffer).unwrap();
            let response: Message = serde_json::from_slice(&buffer[..size])?;
            if let Message::Err { ref err } = response {
                eprintln!("{}", err);
                return Err(format_err!("Key not found"));
            }
        }
    }
    Ok(())
}
