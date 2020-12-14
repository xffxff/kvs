use failure::Error;
use std::result;

/// Using failure::Error as error type
pub type Result<T> = result::Result<T, Error>;

/// Define the storage interface
pub trait KvsEngine {
    /// Set the value of a string key to a string
    fn set(&mut self, key: String, value: String) -> Result<()>;

    /// Get the string value of a string key. If the key does not exist, return None
    fn get(&mut self, key: String) -> Result<Option<String>>;

    /// Remove a given string key
    fn remove(&mut self, key: String) -> Result<()>;
}


/// A simple kv store using hash map store key/value
pub mod simple_kvs;

/// A kv store using the `sled` library
pub mod sled_kvs;
