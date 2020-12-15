use bson;
use sled;
use std::fmt;
use std::io;
use std::string::FromUtf8Error;

#[derive(Debug)]
pub enum KvsError {
    IoError(io::Error),

    BsonSerError(bson::ser::Error),

    BsonDeError(bson::de::Error),

    SerdeJsonError(serde_json::Error),

    SledError(sled::Error),

    UtfError(FromUtf8Error),

    KeyNotFound,

    NotValidLog,

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
            KvsError::UtfError(ref err) => err.fmt(f),
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
        KvsError::UtfError(err)
    }
}
