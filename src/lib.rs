#![deny(missing_docs)]
//! # kvs
//!
//! `kvs` is a simple in-memory key/value store that maps strings
//! to strings.
#[macro_use]
extern crate failure;
mod engine;
mod network;

pub use crate::engine::simple_kvs::*;
pub use crate::engine::sled_kvs::*;
pub use crate::network::Message;
