use crate::network::Request;
use crate::Result;
use crate::{KvsEngine, KvsError};
use bson::Document;
use dashmap::DashMap;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::io::{Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,
    index: Arc<DashMap<String, u64>>,
    writer: Arc<Mutex<KvStoreWriter>>,
}

fn new_buf_writer(path: &Path) -> Result<BufWriter<File>> {
    let f = OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(path)?;
    Ok(BufWriter::new(f))
}

fn new_buf_reader(path: &Path) -> Result<BufReader<File>> {
    let f = OpenOptions::new()
        .read(true)
        .create(true)
        .append(true)
        .open(path)?;
    Ok(BufReader::new(f))
}

fn log_path(path: &Path, file_name: &str) -> PathBuf {
    path.join(format!("{}.bson", file_name))
}

impl KvStore {
    /// Create a KvStore at `path`
    /// If no previous persisted log exists, create a new log;
    /// if there is a previous persisted log then create a
    /// KvStore based on the log.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        let (index, log_count) = KvStore::build_index(&path)?;
        let index = Arc::new(index);

        let path = Arc::new(path);
        let kvs_writer = KvStoreWriter::new(path.clone(), index.clone(), log_count)?;
        let kv_store = KvStore {
            path,
            writer: Arc::new(Mutex::new(kvs_writer)),
            index,
        };
        Ok(kv_store)
    }

    fn build_index(path: &PathBuf) -> Result<(DashMap<String, u64>, u64)> {
        let index: DashMap<String, u64> = DashMap::new();
        let mut last_log_pointer = 0;

        let file_path = log_path(&path, "log");
        let mut reader = new_buf_reader(&file_path)?;

        let mut log_count = 0;

        while let Ok(deserialized) = Document::from_reader(&mut reader) {
            let doc: Request = bson::from_document(deserialized)?;
            match doc {
                Request::Set { ref key, value: _ } => {
                    index.insert(key.to_owned(), last_log_pointer);
                    log_count += 1;
                }
                Request::Remove { ref key } => {
                    index.remove(key);
                    log_count += 1;
                }
                _ => {
                    return Err(KvsError::NotValidLog);
                }
            }
            last_log_pointer = reader.seek(SeekFrom::Current(0))?;
        }

        Ok((index, log_count))
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.set(key, value)?;
        writer.compact()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        match self.index.get(&key) {
            Some(log_pointer) => {
                let file_path = log_path(&self.path, "log");
                let mut reader = new_buf_reader(&file_path)?;
                reader.seek(SeekFrom::Start(log_pointer.to_owned()))?;
                let deserialized = Document::from_reader(&mut reader)?;
                let msg: Request = bson::from_document(deserialized)?;
                match msg {
                    Request::Set { key: _, ref value } => Ok(Some(value.to_owned())),
                    _ => Err(KvsError::KeyNotFound),
                }
            }
            None => Ok(None),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.remove(key)?;
        Ok(())
    }
}

struct KvStoreWriter {
    path: Arc<PathBuf>,
    writer: BufWriter<File>,
    index: Arc<DashMap<String, u64>>,
    log_count: u64,
}

impl KvStoreWriter {
    pub fn new(
        path: Arc<PathBuf>,
        index: Arc<DashMap<String, u64>>,
        log_count: u64,
    ) -> Result<Self> {
        let file_path = log_path(&path, "log");
        let writer = new_buf_writer(&file_path)?;
        let kvs_writer = KvStoreWriter {
            path,
            writer,
            index,
            log_count,
        };
        Ok(kvs_writer)
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
        let log_pointer = self.writer.seek(SeekFrom::End(0))?;
        self.index.insert(key.clone(), log_pointer);
        self.log_count += 1;

        let set = Request::Set { key, value };
        let serialized = bson::to_document(&set)?;
        serialized.to_writer(&mut self.writer)?;
        self.writer.flush()?;
        Ok(())
    }

    fn remove(&mut self, key: String) -> Result<()> {
        match self.index.get(&key) {
            Some(_) => {
                let rm = Request::Remove {
                    key: key.to_owned(),
                };
                let serialized = bson::to_document(&rm)?;
                serialized.to_writer(&mut self.writer)?;
                self.writer.flush()?;
                self.log_count -= 1;
            }
            None => return Err(KvsError::NotValidLog),
        }
        self.index.remove(&key);
        Ok(())
    }

    fn compact(&mut self) -> Result<()> {
        if self.log_count - self.index.len() as u64 > 1000 {
            let file_path = log_path(&self.path, "tmp");
            let mut writer = new_buf_writer(&file_path)?;

            let file_path = log_path(&self.path, "log");
            let mut reader = new_buf_reader(&file_path)?;
            let mut log_count: u64 = 0;
            let mut last_log_pointer = 0;
            let mut index = HashMap::new();
            for entry in self.index.iter() {
                reader.seek(SeekFrom::Start(entry.value().to_owned()))?;
                let deserialized = Document::from_reader(&mut reader)?;
                deserialized.to_writer(&mut writer)?;
                writer.flush()?;
                index.insert(entry.key().to_owned(), last_log_pointer);
                last_log_pointer = writer.seek(SeekFrom::Current(0))?;
                log_count += 1;
            }

            self.log_count = log_count;
            fs::rename(self.path.join("tmp.bson"), self.path.join("log.bson"))?;
            let writer = new_buf_writer(&file_path)?;
            self.writer = writer;
            self.index.clear();
            for (key, value) in index {
                self.index.insert(key, value);
            }
        }

        Ok(())
    }
}
