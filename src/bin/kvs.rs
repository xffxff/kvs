use kvs::{KvStore, Result};
use structopt::StructOpt;
use std::path::Path;

#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "set", about = "Stores a key/value pair")]
    Set { key: String, value: String },
    #[structopt(name = "get", about = "Gets value according to the key")]
    Get { key: String },
    #[structopt(name = "rm", about = "Removes key/value pair according to the key")]
    Remove { key: String },
}

#[derive(Debug, StructOpt)]
pub struct ApplicationArguments {
    #[structopt(subcommand)]
    pub command: Command,
}

fn main() -> Result<()> {
    let opt = ApplicationArguments::from_args();

    let path = Path::new("./");
    let mut kvs = KvStore::open(path)?;

    match opt.command {
        Command::Set { ref key, ref value } => {
            kvs.set(key.to_owned(), value.to_owned()).unwrap();
        }
        Command::Get { ref key } => {
            match kvs.get(key.to_owned()).unwrap() {
                Some(value) => println!("{}", value),
                None => println!("Key not found")
            }
        }
        Command::Remove { ref key } => {
            kvs.remove(key.to_owned()).unwrap();
        }
    }
    Ok(())
}
