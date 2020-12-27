extern crate rand;
extern crate rand_chacha;
use criterion::{criterion_group, criterion_main, Criterion};
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::KvStore;
use kvs::{KvsClient, KvsServer};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

pub fn write_queued_kvstore(c: &mut Criterion) {
    let inputs = &[1, 2, 4, 8, 16];
    c.bench_function_over_inputs(
        "write queued kvstore",
        |b, &num| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let store = KvStore::open(temp_dir.path()).unwrap();
            let pool = RayonThreadPool::new(*num as u32).unwrap();
            let (tx, rx) = mpsc::channel();
            let server = KvsServer::new(store, pool, Some(rx));
            let port = 4000;
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
            let handle = thread::spawn(move || {
                server.run(addr).unwrap();
            });
            thread::sleep(Duration::from_millis(10));
            {
                b.iter(|| {
                    let client_num = 8;
                    let client_pool = SharedQueueThreadPool::new(client_num).unwrap();
                    for i in 0..client_num {
                        client_pool.spawn(move || {
                            for j in 0..100 {
                                let mut client = KvsClient::new(&addr).unwrap();
                                client
                                    .set(format!("key{}", i * 100 + j), "value1".to_owned())
                                    .unwrap();
                            }
                        });
                    }
                });
            }
            tx.send(()).unwrap();
            handle.join().unwrap();
        },
        inputs,
    );
}

criterion_group!(benches, write_queued_kvstore);
criterion_main!(benches);