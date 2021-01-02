use crate::engine::Result;
use crate::network::{Request, Response};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};

pub struct KvsClient {
    stream: TcpStream,
}

impl KvsClient {
    pub fn new(addr: &SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr)?;
        Ok(KvsClient { stream })
    }

    pub fn set(&mut self, key: String, value: String) -> Result<Response> {
        let request = Request::Set { key, value };
        send_and_recv(&mut self.stream, request)
    }

    pub fn get(&mut self, key: String) -> Result<Response> {
        let request = Request::Get { key };
        send_and_recv(&mut self.stream, request)
    }

    pub fn remove(&mut self, key: String) -> Result<Response> {
        let request = Request::Remove { key };
        send_and_recv(&mut self.stream, request)
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
