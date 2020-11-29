use bson::Document;
use kvs::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::SeekFrom;
use structopt::StructOpt;

#[derive(Debug, StructOpt, Serialize, Deserialize)]
pub struct Set {
    pub key: String,
    pub value: String,
}

#[derive(Debug, StructOpt, Serialize, Deserialize)]
pub struct Get {
    pub key: String,
}

#[derive(Debug, StructOpt, Serialize, Deserialize)]
pub struct Remove {
    pub key: String,
}

#[derive(Debug, StructOpt)]
pub enum Command {
    #[structopt(name = "set", about = "Stores a key/value pair")]
    Set(Set),
    #[structopt(name = "get", about = "Gets value according to the key")]
    Get(Get),
    #[structopt(name = "rm", about = "Removes key/value pair according to the key")]
    Remove(Remove),
}

#[derive(Debug, StructOpt)]
pub struct ApplicationArguments {
    #[structopt(subcommand)]
    pub command: Command,
}

fn main() -> Result<()> {
    let opt = ApplicationArguments::from_args();
    let cmd = opt.command;
    let mut f = OpenOptions::new()
        .write(true)
        .read(true)
        .create(true)
        .append(true)
        .open("log.bson")
        .unwrap();
    let mut index: HashMap<String, u64> = HashMap::new();

    match cmd {
        Command::Set(set) => {
            println!("kvs set {} {}", set.key, set.value);
            let serialized = bson::to_bson(&set).unwrap();
            serialized.as_document().unwrap().to_writer(&mut f).unwrap();
        }
        Command::Get(get) => {
            let mut last_log_pointer: u64 = 0;
            while let Ok(deserialized) = Document::from_reader(&mut f) {
                let set: Set = bson::from_document(deserialized).unwrap();
                index.insert(set.key, last_log_pointer);
                last_log_pointer = f.seek(SeekFrom::Current(0)).unwrap();
            }
            match index.get(&get.key) {
                Some(log_pointer) => {
                    f.seek(SeekFrom::Start(log_pointer.to_owned())).unwrap();
                    let deserialized = Document::from_reader(&mut f).unwrap();
                    let set: Set = bson::from_document(deserialized).unwrap();
                    println!("{}", set.value);
                }
                None => println!("Key not found"),
            }
        }
        Command::Remove(rm) => {
            let mut last_log_pointer: u64 = 0;
            while let Ok(deserialized) = Document::from_reader(&mut f) {
                let set: Set = bson::from_document(deserialized).unwrap();
                index.insert(set.key, last_log_pointer);
                last_log_pointer = f.seek(SeekFrom::Current(0)).unwrap();
            }
            match index.get(&rm.key) {
                Some(_) => {
                    serde_json::to_writer(f, &rm).unwrap();
                }
                None => println!("Key not found"),
            }
        }
    }
    Ok(())
}
