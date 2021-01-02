use super::ThreadPool;
use crate::engine::Result;
use std::panic;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;

pub struct SharedQueueThreadPool {
    workers: Vec<Worker>,
    sender: mpsc::Sender<Message>,
}

type Job = Box<dyn FnOnce() + Send + 'static>;

enum Message {
    NewJob(Job),
    Shutdown,
}

impl ThreadPool for SharedQueueThreadPool {
    /// Create a new ThreadPool.
    ///
    /// The size is the number of threads in the pool.
    ///
    /// # Panics
    ///
    /// The `new` function will panic if the size is zero.
    fn new(size: u32) -> Result<Self>
    where
        Self: Sized,
    {
        assert!(size > 0);

        let (sender, receiver) = mpsc::channel();

        let receiver = Arc::new(Mutex::new(receiver));

        let mut workers = Vec::with_capacity(size as usize);

        for id in 0..size {
            workers.push(Worker::new(id, Arc::clone(&receiver)));
        }

        Ok(SharedQueueThreadPool { workers, sender })
    }

    fn spawn<F>(&self, f: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(f);

        self.sender.send(Message::NewJob(job)).unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        info!("Sending Shutdown message to all workers.");

        for _ in &self.workers {
            self.sender.send(Message::Shutdown).unwrap();
        }

        info!("Shutting down all workers.");

        for worker in &mut self.workers {
            info!("Shutting down worker {}", worker.id);

            if let Some(thread) = worker.thread.take() {
                thread.join().unwrap();
            }
        }
    }
}

struct Worker {
    id: u32,
    thread: Option<thread::JoinHandle<()>>,
}

impl Worker {
    fn new(id: u32, receiver: Arc<Mutex<mpsc::Receiver<Message>>>) -> Worker {
        info!("create worker {}", id);
        let thread = thread::spawn(move || loop {
            let message = receiver.lock().unwrap().recv().unwrap();

            match message {
                Message::NewJob(job) => {
                    info!("Worker {} got a job; executing.", id);

                    let result = panic::catch_unwind(panic::AssertUnwindSafe(|| job()));
                    if result.is_err() {
                        info!("Worker {} got an error", id)
                    }
                }
                Message::Shutdown => {
                    info!("Worker {} was told to Shutdown.", id);

                    break;
                }
            }
        });

        Worker {
            id,
            thread: Some(thread),
        }
    }
}
