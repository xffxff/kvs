use std::collections::HashMap;

pub struct KvStore {
    hash_map: HashMap<String, String>,
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
    pub fn new() -> KvStore {
        let hash_map = HashMap::new();
        KvStore{hash_map}
    }

    /// Store one key value pair
    /// 
    /// # Examples
    /// 
    /// ```
    /// use kvs::KvStore;
    /// let kvs = KvStore::new();
    /// 
    /// kvs.set("key1".to_owned(), "value1".to_owned());
    /// ```
    pub fn set(&mut self, key: String, value: String) {
        self.hash_map.insert(key, value);
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
    pub fn get(&self, key: String) -> Option<String> {
        match self.hash_map.get(&key) {
            Some(value) => {
                Some(value.to_owned())
            }
            None => None
        }
    }

    /// Remove the key value pair according to the key
    /// 
    /// # Examples
    /// 
    /// ```
    /// use kvs::KvStore;
    /// let kvs = KvStore::new();
    /// 
    /// kvs.remove("key1".to_owned());
    /// ```
    pub fn remove(&mut self, key: String) {
        self.hash_map.remove(&key);
    }
}