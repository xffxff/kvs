#![deny(missing_docs)]
//! # kvs
//!
//! `kvs` is a simple in-memory key/value store that maps strings
//! to strings.
#[macro_use]
extern crate failure;
use bson::Document;
use failure::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
use std::result;

/// Using Error as error type
pub type Result<T> = result::Result<T, Error>;

/// Using hash map store key/value
pub struct KvStore {
    file: File,
    path: PathBuf,
    index: HashMap<String, u64>,
    log_count: u32,
}

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Get { key: String },
    Remove { key: String },
}

impl KvStore {
    /// Open the KvStore at a given path. Return the KvStore.
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let kvs = KvStore::open("./").unwrap();
    /// ```
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        let mut f = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .append(true)
            .open(&path.join("log.bson"))?;
        let (index, log_count) = KvStore::build_index(&mut f)?;
        Ok(KvStore {
            file: f,
            path,
            index,
            log_count,
        })
    }

    /// Store one key value pair
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut kvs = KvStore::open("./").unwrap();
    ///
    /// kvs.set("key1".to_owned(), "value1".to_owned());
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let log_pointer = self.file.seek(SeekFrom::Current(0))?;
        self.index.insert(key.clone(), log_pointer);
        let set = Command::Set { key, value };
        let serialized = bson::to_document(&set)?;
        serialized.to_writer(&mut self.file)?;
        self.log_count += 1;

        let index_len = self.index.len() as u32;
        if self.log_count > 2 * index_len {
            let mut f = OpenOptions::new()
                .write(true)
                .read(true)
                .create(true)
                .open(self.path.join("tmp.bson"))?;

            let old_index = self.index.clone();
            let mut index: HashMap<String, u64> = HashMap::new();
            self.log_count = 0;
            for (key, _) in old_index {
                if let Some(value) = self.get(key.clone()).unwrap() {
                    let log_pointer = f.seek(SeekFrom::Current(0))?;
                    index.insert(key.clone(), log_pointer);
                    let set = Command::Set { key, value };
                    let serialized = bson::to_document(&set)?;
                    serialized.to_writer(&mut f)?;
                    self.log_count += 1;
                }
            }
            self.file = f;
            self.index = index;
            fs::rename(self.path.join("tmp.bson"), self.path.join("log.bson"))?;
        }
        Ok(())
    }

    /// Get value according to the key
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut kvs = KvStore::open("./").unwrap();
    ///
    /// kvs.get("key1".to_owned());
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            Some(log_pointer) => {
                self.file.seek(SeekFrom::Start(log_pointer.to_owned()))?;
                let deserialized = Document::from_reader(&mut self.file)?;
                let cmd: Command = bson::from_document(deserialized)?;
                match cmd {
                    Command::Set { key: _, ref value } => Ok(Some(value.to_owned())),
                    _ => Err(format_err!("Not valid log")),
                }
            }
            None => Ok(None),
        }
    }

    /// Remove the key value pair according to the key
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut kvs = KvStore::open("./").unwrap();
    ///
    /// kvs.remove("key1".to_owned());
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.index.get(&key) {
            Some(_) => {
                let rm = Command::Remove { key: key.clone() };
                let serialized = bson::to_document(&rm)?;
                serialized.to_writer(&mut self.file)?;
                self.index.remove(&key);
            }
            None => {
                println!("Key not found");
                return Err(format_err!("Key not found"));
            }
        }
        Ok(())
    }

    fn build_index(mut f: &mut File) -> Result<(HashMap<String, u64>, u32)> {
        let mut index: HashMap<String, u64> = HashMap::new();
        let mut log_count: u32 = 0;
        let mut last_log_pointer: u64 = 0;
        while let Ok(deserialized) = Document::from_reader(&mut f) {
            log_count += 1;
            let doc: Command = bson::from_document(deserialized)?;
            match doc {
                Command::Set { ref key, value: _ } => {
                    index.insert(key.to_owned(), last_log_pointer);
                }
                Command::Remove { ref key } => {
                    index.remove(key);
                }
                _ => {
                    return Err(format_err!("Not valid log"));
                }
            }
            last_log_pointer = f.seek(SeekFrom::Current(0))?;
        }
        Ok((index, log_count))
    }
}

/// Define the storage interface
pub trait KvsEngine {
    /// Set the value of a string key to a string
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get the string value of a string key. If the key does not exist, return None
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a given string key
    fn remove(&mut self, key: String) -> Result<()>;
}