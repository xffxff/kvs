use serde::{Deserialize, Serialize};

/// Network protocol of kvs-client and kvs-server
#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    /// Set the value of a string key to a string
    Set { key: String, value: String },
    /// Get the string value of a string key. If the key does not exist, return None
    Get { key: String },
    /// Remove a given string key
    Remove { key: String },
}

#[derive(Debug, Deserialize, Serialize)]
pub enum Response {
    Ok(Option<String>),
    Err(String),
}
