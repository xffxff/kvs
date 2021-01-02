use crate::network::Request;
use crate::Result;
use crate::{KvsEngine, KvsError};
use bson::Document;
use dashmap::DashMap;
use std::fs::{File, OpenOptions};
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,
    writer: Arc<Mutex<BufWriter<File>>>,
    index: Arc<DashMap<String, u64>>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        let log_path = path.join("log.bson");
        let index = KvStore::build_index(&path)?;

        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(log_path)?;
        let writer = BufWriter::new(f);
        let kv_store = KvStore {
            path: Arc::new(path),
            writer: Arc::new(Mutex::new(writer)),
            index: Arc::new(index),
        };
        Ok(kv_store)
    }

    fn build_index(path: &PathBuf) -> Result<DashMap<String, u64>> {
        let index: DashMap<String, u64> = DashMap::new();
        let mut last_log_pointer = 0;

        let log_path = path.join("log.bson");
        let f = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(log_path.clone())?;
        let mut reader = BufReader::new(f);

        while let Ok(deserialized) = Document::from_reader(&mut reader) {
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
            last_log_pointer = reader.seek(SeekFrom::Current(0))?;
        }

        Ok(index)
    }

    // fn compact(&self) -> Result<()> {

    // }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        let log_pointer = writer.seek(SeekFrom::End(0))?;
        self.index.insert(key.clone(), log_pointer);

        let set = Request::Set { key, value };
        let serialized = bson::to_document(&set)?;
        serialized.to_writer(&mut *writer)?;
        writer.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        // let index = self.index.clone();
        match self.index.get(&key) {
            Some(log_pointer) => {
                let log_path = self.path.join("log.bson");
                let f = OpenOptions::new()
                    .read(true)
                    .create(true)
                    .append(true)
                    .open(log_path.clone())?;
                let mut reader = BufReader::new(f);
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
        match self.index.get(&key) {
            Some(_) => {
                let rm = Request::Remove {
                    key: key.to_owned(),
                };
                let serialized = bson::to_document(&rm)?;
                let mut writer = self.writer.lock().unwrap();
                serialized.to_writer(&mut *writer)?;
                writer.flush()?;
            }
            None => return Err(KvsError::NotValidLog),
        }
        self.index.remove(&key);
        Ok(())
    }
}
