use crate::engine::Result;
use crate::network::Message;
use crate::{engine::KvsEngine, error::KvsError};
use bson::Document;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;

/// A simple kv store using hash map store key/value
///
/// # Exmaples
/// ```rust
/// # use kvs::{KvStore, KvsEngine, Result};
/// # use tempfile::TempDir;
/// #
/// # fn main() -> Result<()> {
/// // create a KvStore at a temp dir.
/// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
/// let mut store = KvStore::open(temp_dir.path())?;
///
/// // insert a key/value.
/// store.set("Key1".to_owned(), "Value1".to_owned())?;
///
/// // get the value match the key.
/// match store.get("Key1".to_owned())? {
///     Some(value) => println!("{}", value),
///     None => println!("Key not found")
/// }
///
/// // remove a given string key.
/// store.remove("Key1".to_owned())?;
///
/// // Now "Key1" should not exist.
/// assert_eq!(store.get("Key1".to_owned())?, None);
///
/// Ok(())   
/// # }
/// ```
pub struct KvStore {
    file: File,
    path: PathBuf,
    index: HashMap<String, u64>,
    log_count: u32,
}

impl KvStore {
    /// Create a KvStore at `path`
    /// If no previous persisted log exists, create a new log;
    /// if there is a previous persisted log then create a
    /// KvStore based on the log.
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

    fn build_index(mut f: &mut File) -> Result<(HashMap<String, u64>, u32)> {
        let mut index: HashMap<String, u64> = HashMap::new();
        let mut log_count: u32 = 0;
        let mut last_log_pointer: u64 = 0;
        while let Ok(deserialized) = Document::from_reader(&mut f) {
            log_count += 1;
            let doc: Message = bson::from_document(deserialized)?;
            match doc {
                Message::Set { ref key, value: _ } => {
                    index.insert(key.to_owned(), last_log_pointer);
                }
                Message::Remove { ref key } => {
                    index.remove(key);
                }
                _ => {
                    return Err(KvsError::NotValidLog);
                }
            }
            last_log_pointer = f.seek(SeekFrom::Current(0))?;
        }
        Ok((index, log_count))
    }
}

impl KvsEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        let log_pointer = self.file.seek(SeekFrom::Current(0))?;
        self.index.insert(key.clone(), log_pointer);
        let set = Message::Set { key, value };
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
                    let set = Message::Set { key, value };
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

    fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            Some(log_pointer) => {
                self.file.seek(SeekFrom::Start(log_pointer.to_owned()))?;
                let deserialized = Document::from_reader(&mut self.file)?;
                let msg: Message = bson::from_document(deserialized)?;
                match msg {
                    Message::Set { key: _, ref value } => Ok(Some(value.to_owned())),
                    _ => Err(KvsError::NotValidLog),
                }
            }
            None => Ok(None),
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.index.get(&key) {
            Some(_) => {
                let rm = Message::Remove { key: key.clone() };
                let serialized = bson::to_document(&rm)?;
                serialized.to_writer(&mut self.file)?;
                self.index.remove(&key);
            }
            None => {
                return Err(KvsError::NotValidLog);
            }
        }
        Ok(())
    }
}
