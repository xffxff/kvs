use crate::engine::Result;
use crate::network::{Request, Response};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

/// A client to speak to kvs server.
///
/// # Examples
/// ``` no_run
/// # use kvs::KvsClient;
/// # use kvs::Result;
/// # use std::net::{SocketAddr, Ipv4Addr, IpAddr};
/// #
/// # fn main() -> Result<()> {
/// let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000);
///
/// // insert a key/value.
/// let mut client = KvsClient::new(&addr)?;
/// client.set("Key".to_owned(), "Value".to_owned())?;
///
/// // get the value match the key.
/// let mut client = KvsClient::new(&addr)?;
/// let response = client.get("Key".to_owned())?;
/// assert_eq!(response, Some("Value".to_owned()));
///
/// // remove the given string key.
/// let mut client = KvsClient::new(&addr)?;
/// client.remove("Key".to_owned())?;
///
/// Ok(())
/// # }
/// ```
pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    /// Create a connection to server.
    pub fn new(addr: &SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(KvsClient { stream })
    }

    /// Send to the server to insert a key/value, and wait for the server to respond.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let request = Request::Set { key, value };
        send_and_recv(&mut self.stream, request)?;
        Ok(())
    }

    /// Send to the server to get the value match the key, and wait for the server to respond.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let request = Request::Get { key };
        let response = send_and_recv(&mut self.stream, request)?;
        if let Response::Ok(option) = response {
            return Ok(option);
        }
        Ok(None)
    }

    /// Send to the server to remove the given string key, and wait for the server to respond.
    pub fn remove(&mut self, key: String) -> Result<Option<String>> {
        let request = Request::Remove { key: key.clone() };
        let response = send_and_recv(&mut self.stream, request)?;
        if let Response::Err(_) = response {
            return Ok(None);
        }
        Ok(Some(key))
    }
}

fn send_and_recv(stream: &mut TcpStream, request: Request) -> Result<Response> {
    let request = serde_json::to_vec(&request)?;
    stream.write_all(&request)?;

    let mut buffer = [0; 1024];
    let size = stream.read(&mut buffer)?;
    let response: Response = serde_json::from_slice(&buffer[..size])?;
    Ok(response)
}
