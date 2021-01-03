use crate::thread_pool::ThreadPool;
use crate::Result;
use std::thread;

/// `NaiveThreadPool` is a `ThreadPool` implementation for this naive approach,
/// where `ThreadPool::spawn` will create a new thread for each spawned job.
pub struct NaiveThreadPool;

impl ThreadPool for NaiveThreadPool {
    fn new(_threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(NaiveThreadPool)
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        thread::spawn(move || job());
    }
}
