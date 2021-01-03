#![deny(missing_docs)]
//! # kvs
//!
//! `kvs` is a simple in-memory key/value store that maps strings
//! to strings.
#[macro_use]
extern crate failure;
#[macro_use]
extern crate log;
mod client;
mod engine;
mod error;
mod network;
mod server;
mod thread_pool;

pub use crate::client::KvsClient;
pub use crate::engine::simple_kvs::*;
pub use crate::engine::sled_kvs::*;
pub use crate::engine::*;
pub use crate::error::KvsError;
// pub use crate::network::{Request, Response};
pub use crate::server::KvsServer;
