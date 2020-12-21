use crate::engine::Result;
use crate::thread_pool::ThreadPool;

pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads as usize)
            .build()
            .unwrap();
        Ok(RayonThreadPool { pool })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.install(job);
    }
}
