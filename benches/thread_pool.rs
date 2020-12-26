extern crate rand;
extern crate rand_chacha;
use criterion::{criterion_group, criterion_main, Criterion};
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsEngine};
use kvs::{KvsClient, KvsServer};
use rand::distributions::Alphanumeric;
use rand::{Rng, SeedableRng};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn random_string(rng: &mut impl rand::RngCore) -> String {
    let size = rng.gen_range(1, 100000);
    let rand_string = rng.sample_iter(&Alphanumeric).take(size).collect();
    return rand_string;
}

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
            let mut rng = rand::thread_rng();
            let port = rng.gen_range(4000, 7000);
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
            thread::spawn(move || {
                server.run(addr).unwrap();
            });
            thread::sleep(Duration::from_millis(5));
            b.iter(|| {
                for i in 0..1000 {
                    let mut client = KvsClient::new(&addr).unwrap();
                    client
                        .set(format!("key{}", i), "value1".to_owned())
                        .unwrap();
                }
            });
            tx.send(()).unwrap();
        },
        inputs,
    );
}

criterion_group!(benches, write_queued_kvstore);
criterion_main!(benches);
