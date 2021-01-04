use crate::network::{Request, Response};
use crate::thread_pool::ThreadPool;
use crate::KvsEngine;
use crate::{engine::Result, KvsError};
use log::info;
use serde::Deserialize;
use std::io;
use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::mpsc;

/// A server to listen to the kvs client.
/// # Examples
/// ```no_run
/// # use tempfile::TempDir;
/// # use kvs::thread_pool::{ThreadPool, SharedQueueThreadPool};
/// # use kvs::KvStore;
/// # use kvs::KvsServer;
/// # use std::sync::mpsc;
/// # use std::net::{IpAddr, Ipv4Addr, SocketAddr};
///
/// # fn main() {
/// let temp_dir = TempDir::new().expect("unable to create temporary working directory");
/// let store = KvStore::open(temp_dir.path()).unwrap();
/// let pool = SharedQueueThreadPool::new(8).unwrap();
///
/// // create a server.
/// let server = KvsServer::new(store, pool, None);
///
/// // listen to the request.
/// let port = 4000;
/// let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
/// server.run(addr).unwrap();
/// # }
pub struct KvsServer<E: KvsEngine, T: ThreadPool> {
    store: E,
    pool: T,
    receiver: Option<mpsc::Receiver<()>>,
}

impl<E: KvsEngine, T: ThreadPool> KvsServer<E, T> {
    /// Create a server.
    ///  
    /// # Arguments
    ///
    /// * `engine` - The storage engine to use, simple kvs or sled.
    /// * `pool` - The thread pool to use, NaiveThreadPool, SharedQueueThreadPool or RayonThreadPool.
    /// * `receiver` - A [`mpsc::Receiver`](https://doc.rust-lang.org/std/sync/mpsc/struct.Receiver.html) to shutdown the server programmatically.
    pub fn new(engine: E, pool: T, receiver: Option<mpsc::Receiver<()>>) -> Self {
        KvsServer {
            store: engine,
            pool,
            receiver,
        }
    }

    /// Create a listener bound to `addr`, and handle the connection received on this listener.
    pub fn run(self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;

        for stream in listener.incoming() {
            if let Some(rx) = self.receiver.as_ref() {
                if rx.try_recv().is_ok() {
                    break;
                }
            };
            match stream {
                Ok(mut s) => {
                    info!("connection from {:?}", s.peer_addr()?);
                    let kv_store = self.store.clone();
                    self.pool.spawn(move || {
                        let request = read_cmd(&mut s).unwrap();
                        let response = process_cmd(kv_store, request).unwrap();
                        respond(&mut s, response).unwrap();
                    })
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => return Err(KvsError::IoError(e)),
            }
        }

        Ok(())
    }
}

fn read_cmd(stream: &mut TcpStream) -> Result<Request> {
    let mut de = serde_json::Deserializer::from_reader(stream);
    let request = Request::deserialize(&mut de)?;
    Ok(request)
}

fn process_cmd(kv_store: impl KvsEngine, msg: Request) -> Result<Response> {
    let response = match msg {
        Request::Set { ref key, ref value } => {
            kv_store.set(key.to_owned(), value.to_owned())?;
            Response::Ok(None)
        }
        Request::Get { ref key } => match kv_store.get(key.to_owned())? {
            Some(value) => Response::Ok(Some(value)),
            None => Response::Ok(None),
        },
        Request::Remove { ref key } => match kv_store.remove(key.to_owned()) {
            Err(_) => Response::Err("Key not found".to_owned()),
            Ok(_) => Response::Ok(None),
        },
    };
    Ok(response)
}

fn respond(mut stream: &mut TcpStream, resp: Response) -> Result<()> {
    serde_json::to_writer(&mut stream, &resp)?;
    stream.flush()?;
    Ok(())
}
