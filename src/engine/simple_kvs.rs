use crate::network::Request;
use crate::Result;
use crate::{KvsEngine, KvsError};
use bson::Document;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::SeekFrom;
use std::io::{BufReader, BufWriter};
use std::io::{Seek, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct KvStore(Arc<Mutex<SharedKvStore>>);

pub struct SharedKvStore {
    reader: BufReader<File>,
    writer: BufWriter<File>,
    index: HashMap<String, u64>,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = path.into();
        let log_path = path.join("log.bson");
        let f = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(log_path.clone())?;
        let mut reader = BufReader::new(f);
        let index = KvStore::build_index(&mut reader)?;

        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(log_path)?;
        let writer = BufWriter::new(f);
        let shared_kv_store = SharedKvStore {
            reader,
            writer,
            index,
        };
        Ok(KvStore(Arc::new(Mutex::new(shared_kv_store))))
    }

    fn build_index(reader: &mut BufReader<File>) -> Result<HashMap<String, u64>> {
        let mut index: HashMap<String, u64> = HashMap::new();
        let mut last_log_pointer = 0;

        while let Ok(deserialized) = Document::from_reader(reader) {
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
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut store = self.0.lock().unwrap();
        let log_pointer = store.writer.seek(SeekFrom::End(0))?;
        store.index.insert(key.clone(), log_pointer);

        let set = Request::Set { key, value };
        let serialized = bson::to_document(&set)?;
        serialized.to_writer(&mut store.writer)?;
        store.writer.flush()?;
        Ok(())
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let mut store = self.0.lock().unwrap();
        let index = store.index.clone();
        match index.get(&key) {
            Some(log_pointer) => {
                store.reader.seek(SeekFrom::Start(log_pointer.to_owned()))?;
                let deserialized = Document::from_reader(&mut store.reader)?;
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
        let mut store = self.0.lock().unwrap();
        match store.index.get(&key) {
            Some(_) => {
                let rm = Request::Remove {
                    key: key.to_owned(),
                };
                let serialized = bson::to_document(&rm)?;
                serialized.to_writer(&mut store.writer)?;
                store.writer.flush()?;
                store.index.remove(&key);
            }
            None => return Err(KvsError::NotValidLog),
        }
        Ok(())
    }
}
