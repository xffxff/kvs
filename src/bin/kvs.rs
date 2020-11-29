use bson::Document;
use kvs::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use structopt::StructOpt;

// #[derive(Debug, StructOpt, Serialize, Deserialize)]
// pub struct Set {
//     pub key: String,
//     pub value: String,
// }

// #[derive(Debug, StructOpt, Serialize, Deserialize)]
// pub struct Get {
//     pub key: String,
// }

// #[derive(Debug, StructOpt, Serialize, Deserialize)]
// pub struct Remove {
//     pub key: String,
// }

#[derive(Debug, StructOpt, Serialize, Deserialize)]
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
    let mut f = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .append(true)
        .open("log.bson")
        .unwrap();
    let mut index: HashMap<String, u64> = HashMap::new();

    match opt.command {
        Command::Set { ref key, ref value } => {
            let set = Command::Set {
                key: key.to_owned(),
                value: value.to_owned(),
            };
            let serialized = bson::to_bson(&set).unwrap();
            serialized.as_document().unwrap().to_writer(&mut f).unwrap();
        }
        Command::Get { ref key } => {
            let mut last_log_pointer: u64 = 0;
            while let Ok(deserialized) = Document::from_reader(&mut f) {
                let cmd: Command = bson::from_document(deserialized).unwrap();
                match cmd {
                    Command::Set { ref key, value: _ } => {
                        index.insert(key.to_owned(), last_log_pointer);
                    }
                    Command::Remove { ref key } => {
                        index.remove(key);
                    }
                    _ => {
                        println!("Invalid log")
                    }
                }
                last_log_pointer = f.seek(SeekFrom::Current(0)).unwrap();
            }
            match index.get(key) {
                Some(log_pointer) => {
                    f.seek(SeekFrom::Start(log_pointer.to_owned())).unwrap();
                    let deserialized = Document::from_reader(&mut f).unwrap();
                    let cmd: Command = bson::from_document(deserialized).unwrap();
                    match cmd {
                        Command::Set { key: _, ref value } => {
                            println!("{}", value);
                        }
                        _ => println!("Key not found"),
                    }
                }
                None => println!("Key not found"),
            }
        }
        Command::Remove { ref key } => {
            let mut last_log_pointer: u64 = 0;
            while let Ok(deserialized) = Document::from_reader(&mut f) {
                let cmd: Command = bson::from_document(deserialized).unwrap();
                match cmd {
                    Command::Set { ref key, value: _ } => {
                        index.insert(key.to_owned(), last_log_pointer);
                    }
                    Command::Remove { ref key } => {
                        index.remove(key);
                    }
                    _ => {
                        println!("Invalid log")
                    }
                }
                last_log_pointer = f.seek(SeekFrom::Current(0)).unwrap();
            }

            match index.get(key) {
                Some(_) => {
                    let rm = Command::Remove {
                        key: key.to_owned(),
                    };
                    let serialized = bson::to_bson(&rm).unwrap();
                    serialized.as_document().unwrap().to_writer(&mut f).unwrap();
                }
                None => println!("Key not found"),
            }
        }
    }
    Ok(())
}
