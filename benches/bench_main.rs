use criterion::criterion_main;

mod benchmarks;

criterion_main!(
    // benchmarks::engine::write,
    // benchmarks::engine::read,
    // benchmarks::thread_pool::write_queued,
    // benchmarks::thread_pool::write_rayon,
    benchmarks::thread_pool::read_queued,
    // benchmarks::thread_pool::read_rayon,
);
