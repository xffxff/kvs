use crate::engine::Result;
use crate::network::Request;
use crate::{engine::KvsEngine, error::KvsError};
use bson::Document;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::PathBuf;
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
    reader: Arc<Mutex<Reader>>,
    index: Arc<Mutex<HashMap<String, u64>>>,
    log_count: Arc<Mutex<u32>>,
}

struct Reader {
    file: File,
    path: PathBuf,
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
            reader: Arc::new(Mutex::new(Reader { file: f, path })),
            index: Arc::new(Mutex::new(index)),
            log_count: Arc::new(Mutex::new(log_count)),
        })
    }

    fn build_index(mut f: &mut File) -> Result<(HashMap<String, u64>, u32)> {
        let mut index: HashMap<String, u64> = HashMap::new();
        let mut log_count: u32 = 0;
        let mut last_log_pointer: u64 = 0;
        while let Ok(deserialized) = Document::from_reader(&mut f) {
            log_count += 1;
            let doc: Request = bson::from_document(deserialized)?;
            match doc {
                Request::Set { ref key, value: _ } => {
                    index.insert(key.to_owned(), last_log_pointer);
                }
                Request::Remove { ref key } => {
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

    fn compact(&self) -> Result<()> {
        let need_compaction = {
            let index = self.index.lock().map_err(|e| e.to_string())?;
            let log_count = self.log_count.lock().map_err(|e| e.to_string())?;
            *log_count > 2 * index.len() as u32
        };
        if need_compaction {
            let mut f = {
                let reader = self.reader.lock().map_err(|e| e.to_string())?;
                OpenOptions::new()
                    .write(true)
                    .read(true)
                    .create(true)
                    .open(reader.path.join("tmp.bson"))?
            };

            let old_index = {
                let index = self.index.lock().map_err(|e| e.to_string())?;
                index.clone()
            };

            let mut new_index: HashMap<String, u64> = HashMap::new();
            let mut new_log_count = 0;
            for (key, _) in old_index {
                if let Some(value) = self.get(key.clone())? {
                    let log_pointer = f.seek(SeekFrom::Current(0))?;
                    new_index.insert(key.clone(), log_pointer);
                    let set = Request::Set { key, value };
                    let serialized = bson::to_document(&set)?;
                    serialized.to_writer(&mut f)?;
                    new_log_count += 1;
                }
            }

            {
                let mut reader = self.reader.lock().map_err(|e| e.to_string())?;
                reader.file = f;
                let mut index = self.index.lock().map_err(|e| e.to_string())?;
                *index = new_index;
                let mut log_count = self.log_count.lock().map_err(|e| e.to_string())?;
                *log_count = new_log_count;
                fs::rename(reader.path.join("tmp.bson"), reader.path.join("log.bson"))?;
            }
        }

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        {
            let mut reader = self.reader.lock().map_err(|e| e.to_string())?;
            let log_pointer = reader.file.seek(SeekFrom::Current(0))?;

            let mut index = self.index.lock().map_err(|e| e.to_string())?;
            index.insert(key.clone(), log_pointer);

            let set = Request::Set { key, value };
            let serialized = bson::to_document(&set)?;
            serialized.to_writer(&mut reader.file)?;

            let mut log_count = self.log_count.lock().map_err(|e| e.to_string())?;
            *log_count += 1;
        }
        self.compact().map_err(|e| e.to_string())?;

        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let index = self.index.lock().map_err(|e| e.to_string())?;
        match index.get(&key) {
            Some(log_pointer) => {
                let mut reader = self.reader.lock().map_err(|e| e.to_string())?;
                reader.file.seek(SeekFrom::Start(log_pointer.to_owned()))?;
                let deserialized = Document::from_reader(&mut reader.file)?;
                let msg: Request = bson::from_document(deserialized)?;
                match msg {
                    Request::Set { key: _, ref value } => Ok(Some(value.to_owned())),
                    _ => Err(KvsError::NotValidLog),
                }
            }
            None => Ok(None),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        let mut index = self.index.lock().map_err(|e| e.to_string())?;
        match index.get(&key) {
            Some(_) => {
                let rm = Request::Remove { key: key.clone() };
                let serialized = bson::to_document(&rm)?;
                let mut reader = self.reader.lock().map_err(|e| e.to_string())?;
                serialized.to_writer(&mut reader.file)?;
                index.remove(&key);
            }
            None => {
                return Err(KvsError::NotValidLog);
            }
        }
        Ok(())
    }
}
