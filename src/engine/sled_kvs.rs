use crate::engine::KvsEngine;
use crate::engine::Result;
use crate::error::KvsError;
use std::path::PathBuf;

/// A kv store using the `sled` library
///
/// # Exmaples
/// ```rust
/// # use kvs::{SledKvStore, KvsEngine, Result};
/// # use tempfile::TempDir;
/// #
/// # fn main() -> Result<()> {
/// // create a KvStore at a temp dir.
/// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
/// let mut store = SledKvStore::open(temp_dir.path())?;
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
pub struct SledKvStore {
    db: sled::Db,
}

impl SledKvStore {
    /// Create a SledKvStore at `path`
    /// If no previous persisted log exists, create a new log;
    /// if there is a previous persisted log then create a
    /// KvStore based on the log.
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvStore> {
        let db = sled::open(path.into())?;
        let sled_kvs = SledKvStore { db };
        Ok(sled_kvs)
    }
}

impl KvsEngine for SledKvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let result = self.db.get(key.as_bytes())?;
        match result {
            Some(value) => {
                let value = String::from_utf8(value.to_vec())?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn remove(&self, key: String) -> Result<()> {
        let result = self.db.remove(key.as_bytes())?;
        self.db.flush()?;
        if result.is_none() {
            return Err(KvsError::KeyNotFound);
        }
        Ok(())
    }
}
