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
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct KvStore {
    path: Arc<PathBuf>,
    writer: Arc<Mutex<BufWriter<File>>>,
    index: Arc<DashMap<String, u64>>,
    log_count: Arc<Mutex<u64>>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        let log_path = path.join("log.bson");
        let (index, log_count) = KvStore::build_index(&path)?;

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
            log_count: Arc::new(Mutex::new(log_count)),
        };
        Ok(kv_store)
    }

    fn build_index(path: &PathBuf) -> Result<(DashMap<String, u64>, u64)> {
        let index: DashMap<String, u64> = DashMap::new();
        let mut last_log_pointer = 0;

        let log_path = path.join("log.bson");
        let f = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(log_path.clone())?;
        let mut reader = BufReader::new(f);

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

    fn compact(&self) -> Result<()> {
        let log_path = self.path.join("tmp.bson");
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(log_path)?;
        let mut writer = BufWriter::new(f);

        let log_path = self.path.join("log.bson");
        let f = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(log_path)?;
        let mut reader = BufReader::new(f);
        let mut log_count: u64 = 0;
        let mut last_log_pointer = 0;
        let mut index = HashMap::new();
        for entry in self.index.iter() {
            reader.seek(SeekFrom::Start(entry.value().to_owned()))?;
            let deserialized = Document::from_reader(&mut reader)?;
            let set: Request = bson::from_document(deserialized)?;

            let serialized = bson::to_document(&set)?;
            serialized.to_writer(&mut writer)?;
            writer.flush()?;
            index.insert(entry.key().to_owned(), last_log_pointer);
            last_log_pointer = writer.seek(SeekFrom::Current(0))?;
            log_count += 1;
        }

        let mut self_log_count = self.log_count.lock().unwrap();
        *self_log_count = log_count;
        fs::rename(self.path.join("tmp.bson"), self.path.join("log.bson"))?;
        let log_path = self.path.join("log.bson");
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(log_path)?;
        let writer = BufWriter::new(f);
        let mut self_writer = self.writer.lock().unwrap();
        *self_writer = writer;
        self.index.clear();
        for (key, value) in index {
            self.index.insert(key, value);
        }

        Ok(())
    }
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        {
            let mut writer = self.writer.lock().unwrap();
            let log_pointer = writer.seek(SeekFrom::End(0))?;
            self.index.insert(key.clone(), log_pointer);
            let mut log_count = self.log_count.lock().unwrap();
            *log_count += 1;

            let set = Request::Set { key, value };
            let serialized = bson::to_document(&set)?;
            serialized.to_writer(&mut *writer)?;
            writer.flush()?;
        }

        let log_count = self.log_count.lock().unwrap().clone();
        if log_count > 2 * self.index.len() as u64 {
            self.compact()?;
        }

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
                let mut log_count = self.log_count.lock().unwrap();
                *log_count -= 1;
            }
            None => return Err(KvsError::NotValidLog),
        }
        self.index.remove(&key);
        Ok(())
    }
}
