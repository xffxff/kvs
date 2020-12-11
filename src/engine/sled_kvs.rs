use crate::KvsEngine;
use crate::Result;
use std::path::PathBuf;

pub struct SledKVStore {
    pub db: sled::Db
}

impl SledKVStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKVStore> {
        let db = sled::open(path.into())?;
        let sled_kvs = SledKVStore { db };
        Ok(sled_kvs)
    }
}

impl KvsEngine for SledKVStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        let result = self.db.get(key.as_bytes())?;
        match result {
            Some(value) => {
                let value = String::from_utf8(value.to_vec())?;
                Ok(Some(value))
            },
            None => Ok(None)
        }
    }

    fn remove(&mut self, key: String) -> Result<()> {
        let result = self.db.remove(key.as_bytes())?;
        self.db.flush()?;
        match result {
            Some(_) => {}
            None => {
                return Err(format_err!("Key not found"));
            }
        }
        Ok(())
    }
}

