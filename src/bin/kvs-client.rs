#[macro_use]
extern crate failure;
use kvs::KvsClient;
use kvs::KvsError;
use kvs::Response;
use kvs::Result;
use std::net::SocketAddr;
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
            let mut client = KvsClient::new(addr)?;
            client.set(key.to_owned(), value.to_owned())?;
        }
        Command::Get { ref key, ref addr } => {
            let mut client = KvsClient::new(addr)?;
            let response = client.get(key.to_owned())?;
            if let Response::Ok(option) = response {
                match option {
                    Some(value) => println!("{}", value),
                    None => println!("Key not found"),
                }
            }
        }
        Command::Remove { ref key, ref addr } => {
            let mut client = KvsClient::new(addr)?;
            let response = client.remove(key.to_owned())?;
            if let Response::Err(err) = response {
                eprintln!("{}", err);
                return Err(KvsError::KeyNotFound);
            }
        }
    }
    Ok(())
}
