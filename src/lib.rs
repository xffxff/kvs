use std::collections::HashMap;

pub struct KvStore {
    hash_map: HashMap<String, String>,
}

impl KvStore {
    pub fn new() -> KvStore {
        let hash_map = HashMap::new();
        KvStore{hash_map}
    }

    pub fn set(&mut self, key: String, value: String) {
        self.hash_map.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        match self.hash_map.get(&key) {
            Some(value) => {
                Some(value.to_owned())
            }
            None => None
        }
    }

    pub fn remove(&mut self, key: String) {
        self.hash_map.remove(&key);
    }
}