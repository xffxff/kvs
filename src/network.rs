use serde::{Deserialize, Serialize};

/// Network protocol of kvs-client and kvs-server
#[derive(Debug, Deserialize, Serialize)]
pub enum Message {
    /// Set the value of a string key to a string
    Set {
        /// insert key
        key: String,
        /// insert value
        value: String,
    },

    /// Get the string value of a string key. If the key does not exist, return None
    Get {
        /// key
        key: String,
    },

    /// Remove a given string key
    Remove {
        /// remove key
        key: String,
    },

    /// Reply to the received message
    Reply {
        /// reply string
        reply: String,
    },

    /// Error replies
    Err {
        /// error string
        err: String,
    },
}
