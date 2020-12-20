use crate::engine::Result;

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

pub mod naive;
pub mod shared_queue;

pub use crate::thread_pool::naive::NaiveThreadPool;
pub use crate::thread_pool::shared_queue::SharedQueueThreadPool;
