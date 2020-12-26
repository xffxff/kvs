use super::ThreadPool;
use crate::engine::Result;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    RunJob(Job),
    Shutdown,
}

pub struct SharedQueueThreadPool {
    threads: u32,
    sender: Sender<Message>,
}

struct Sentinel<'a> {
    receiver: &'a Arc<Mutex<Receiver<Message>>>,
    active: bool,
}

impl<'a> Sentinel<'a> {
    fn new(receiver: &'a Arc<Mutex<Receiver<Message>>>) -> Self {
        Sentinel { receiver, active: true }
    }

    fn cancel(mut self) {
        self.active = false;
    }
}

impl<'a> Drop for Sentinel<'a> {
    fn drop(&mut self) {
        if self.active {
            spawn_in_pool(self.receiver.clone());
        }
    }
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let (tx, rx) = channel();
        let rx = Arc::new(Mutex::new(rx));

        for _ in 0..threads {
            let rx = Arc::clone(&rx);
            spawn_in_pool(rx);
        }

        Ok(SharedQueueThreadPool { threads, sender: tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(job);
        self.sender.send(Message::RunJob(job)).unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in 0..self.threads {
            self.sender.send(Message::Shutdown).unwrap();
        }
    }
}

fn spawn_in_pool(receiver: Arc<Mutex<Receiver<Message>>>) {
    thread::spawn(move || {
        let sentinel = Sentinel::new(&receiver);
        loop {
            let recv = receiver.lock().unwrap().recv(); 
            match recv {
                Ok(msg) => {
                    match msg {
                        Message::RunJob(job) => job(),
                        Message::Shutdown => break,
                    }
                },
                Err(_) => {
                    break
                }
            }
        }

        sentinel.cancel();
    });
}
