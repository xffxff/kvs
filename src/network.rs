use serde::{Deserialize, Serialize};

pub mod protocol {
    #[derive(Debug, super::Deserialize, super::Serialize)]
    pub enum Message {
        Set { key: String, value: String },
        Get { key: String },
        Remove { key: String },
    }
}
