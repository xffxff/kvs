//! This module provides various thread pools.
use crate::engine::Result;

/// A thread pool used to execute functions in parallel.
/// Spawns a specified number of worker threads
pub trait ThreadPool {
    /// Creates a new thread pool capable of executing num_threads number of jobs concurrently.
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    /// Executes the function job on a thread in the pool.
    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

mod naive;
mod rayon_work_stealing;
mod shared_queue;

pub use crate::thread_pool::naive::NaiveThreadPool;
pub use crate::thread_pool::rayon_work_stealing::RayonThreadPool;
pub use crate::thread_pool::shared_queue::SharedQueueThreadPool;
