use super::ThreadPool;
use crate::engine::Result;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

type Thunk = Box<dyn FnOnce() + Send + 'static>;

pub struct SharedQueueThreadPool {
    job: Sender<Thunk>,
}

struct Sentinel<'a> {
    job: &'a Arc<Mutex<Receiver<Thunk>>>,
    active: bool,
}

impl<'a> Sentinel<'a> {
    fn new(job: &'a Arc<Mutex<Receiver<Thunk>>>) -> Self {
        Sentinel { job, active: true }
    }

    fn cancel(mut self) {
        self.active = false;
    }
}

impl<'a> Drop for Sentinel<'a> {
    fn drop(&mut self) {
        if self.active {
            spawn_in_pool(self.job.clone());
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

        Ok(SharedQueueThreadPool { job: tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.job.send(Box::new(|| job())).unwrap();
    }
}

fn spawn_in_pool(job: Arc<Mutex<Receiver<Thunk>>>) {
    thread::spawn(move || {
        let sentinel = Sentinel::new(&job);
        loop {
            let message = {
                let lock = job.lock().unwrap();
                lock.recv()
            };

            match message {
                Ok(job) => job(),
                Err(..) => break,
            }
        }

        sentinel.cancel();
    });
}
