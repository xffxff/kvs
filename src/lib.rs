#![deny(missing_docs)]
//! # kvs
//!
//! `kvs` is a simple in-memory key/value store that maps strings
//! to strings.
use std::collections::HashMap;
use std::path::PathBuf;
use std::result;
use failure::Error;

/// Using Error as error type
pub type Result<T> = result::Result<T, Error>;

/// Using hash map store key/value
pub struct KvStore {
    hash_map: HashMap<String, String>,
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KvStore {
    /// Returns a new key value store
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let kvs = KvStore::new();
    /// ```
    pub fn new() -> Self {
        let hash_map = HashMap::new();
        KvStore { hash_map }
    }
    
    /// Open the KvStore at a given path. Return the KvStore.
    /// 
    /// # Examples
    /// 
    /// ```
    /// use kvs::KvStore;
    /// let kvs = KvStore::open().unwrap();
    /// ```
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        panic!("open failed")
    }

    /// Store one key value pair
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut kvs = KvStore::new();
    ///
    /// kvs.set("key1".to_owned(), "value1".to_owned());
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.hash_map.insert(key, value);
        Ok(())
    }

    /// Get value according to the key
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let kvs = KvStore::new();
    ///
    /// kvs.get("key1".to_owned());
    /// ```
    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.hash_map.get(&key) {
            Some(value) => Ok(Some(value.to_owned())),
            None => Ok(None),
        }
    }

    /// Remove the key value pair according to the key
    ///
    /// # Examples
    ///
    /// ```
    /// use kvs::KvStore;
    /// let mut kvs = KvStore::new();
    ///
    /// kvs.remove("key1".to_owned());
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        self.hash_map.remove(&key);
        Ok(())
    }
}
