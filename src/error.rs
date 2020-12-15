use bson;
use sled;
use std::fmt;
use std::io;
use std::string::FromUtf8Error;

/// Possible errors that can arise.
#[derive(Debug)]
pub enum KvsError {
    /// A [`std::io::Error`](https://doc.rust-lang.org/std/io/struct.Error.html) encountered while IO.
    IoError(io::Error),

    /// A [`bson::ser::Error`](https://docs.rs/bson/1.1.0/bson/ser/enum.Error.html) encountered while serilization with bson.
    BsonSerError(bson::ser::Error),

    /// A [`bson::de::Error`](https://docs.rs/bson/1.1.0/bson/de/enum.Error.html) encountered while deserialization with bson.
    BsonDeError(bson::de::Error),

    /// A [`serde_json::Error`](https://docs.serde.rs/serde_json/struct.Error.html) encountered while using serde_json.
    SerdeJsonError(serde_json::Error),

    /// A [`sled::Error`](https://docs.rs/sled/0.16.2/sled/enum.Error.html) encountered while using sled.
    SledError(sled::Error),

    /// A [`std::string::FromUtf8Error`](https://doc.rust-lang.org/std/string/struct.FromUtf8Error.html) encountered
    /// while decoding a UTF-8 String from the input data.
    FromUtf8Error(FromUtf8Error),

    /// Raise when get or remove a not founded key.
    KeyNotFound,

    /// Raise when reading from a not valid log.
    NotValidLog,

    /// Raise when input engine mismatch the previous persisted engine.
    MismatchEngine,
}

impl fmt::Display for KvsError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            KvsError::IoError(ref err) => err.fmt(f),
            KvsError::BsonSerError(ref err) => err.fmt(f),
            KvsError::BsonDeError(ref err) => err.fmt(f),
            KvsError::SerdeJsonError(ref err) => err.fmt(f),
            KvsError::SledError(ref err) => err.fmt(f),
            KvsError::FromUtf8Error(ref err) => err.fmt(f),
            KvsError::KeyNotFound => write!(f, "Key not found"),
            KvsError::NotValidLog => write!(f, "Not valid log"),
            KvsError::MismatchEngine => write!(f, "Mismatch engine"),
        }
    }
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::IoError(err)
    }
}

impl From<bson::ser::Error> for KvsError {
    fn from(err: bson::ser::Error) -> KvsError {
        KvsError::BsonSerError(err)
    }
}

impl From<bson::de::Error> for KvsError {
    fn from(err: bson::de::Error) -> KvsError {
        KvsError::BsonDeError(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::SerdeJsonError(err)
    }
}

impl From<sled::Error> for KvsError {
    fn from(err: sled::Error) -> KvsError {
        KvsError::SledError(err)
    }
}

impl From<FromUtf8Error> for KvsError {
    fn from(err: FromUtf8Error) -> KvsError {
        KvsError::FromUtf8Error(err)
    }
}
