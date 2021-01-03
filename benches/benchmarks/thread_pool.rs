extern crate rand;
extern crate rand_chacha;
use criterion::{criterion_group, Criterion};
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, SledKvStore};
use kvs::{KvsClient, KvsServer};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

static SERVER_THREAD_NUMS: &[i32] = &[1, 2, 4, 8, 16];
static CLIENT_THREAD_NUM: u32 = 64;

fn thread_pool_set(thread_pool: &impl ThreadPool, addr: SocketAddr) {
    let (sender, receiver) = mpsc::channel();
    for i in 0..1000 {
        let sender = mpsc::Sender::clone(&sender);
        thread_pool.spawn(move || {
            let mut client = KvsClient::new(&addr).unwrap();
            client.set(format!("key{}", i), "value".to_owned()).unwrap();
            sender.send(()).unwrap();
        });
    }
    drop(sender);
    for _ in receiver {}
}

fn thread_pool_get(thread_pool: &impl ThreadPool, addr: SocketAddr) {
    let (sender, receiver) = mpsc::channel();
    for i in 0..1000 {
        let sender = mpsc::Sender::clone(&sender);
        thread_pool.spawn(move || {
            let mut client = KvsClient::new(&addr).unwrap();
            let response = client.get(format!("key{}", i)).unwrap();
            if let Response::Ok(option) = response {
                assert_eq!(option, Some("value".to_string()));
            }
            sender.send(()).unwrap();
        });
    }
    drop(sender);
    for _ in receiver {}
}

fn write_queued_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "write queued kvstore",
        |b, &num| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let store = KvStore::open(temp_dir.path()).unwrap();
            let pool = SharedQueueThreadPool::new(*num as u32).unwrap();
            let (tx, rx) = mpsc::channel();
            let server = KvsServer::new(store, pool, Some(rx));
            let port = 4000;
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
            let handle = thread::spawn(move || {
                server.run(addr).unwrap();
            });
            thread::sleep(Duration::from_millis(10));
            {
                let client_pool = SharedQueueThreadPool::new(CLIENT_THREAD_NUM).unwrap();
                b.iter(|| thread_pool_set(&client_pool, addr));
            }
            tx.send(()).unwrap();
            handle.join().unwrap();
        },
        SERVER_THREAD_NUMS,
    );
}

fn read_queued_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "read queued kvstore",
        |b, &num| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let store = KvStore::open(temp_dir.path()).unwrap();
            let pool = SharedQueueThreadPool::new(*num as u32).unwrap();
            let (tx, rx) = mpsc::channel();
            let server = KvsServer::new(store, pool, Some(rx));
            let port = 4000;
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
            let handle = thread::spawn(move || {
                server.run(addr).unwrap();
            });
            thread::sleep(Duration::from_millis(10));
            {
                let client_pool = SharedQueueThreadPool::new(CLIENT_THREAD_NUM).unwrap();
                thread_pool_set(&client_pool, addr);

                b.iter(|| {
                    thread_pool_get(&client_pool, addr);
                });
            }
            tx.send(()).unwrap();
            handle.join().unwrap();
        },
        SERVER_THREAD_NUMS,
    );
}

fn write_rayon_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "write rayon kvstore",
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
                let client_pool = SharedQueueThreadPool::new(CLIENT_THREAD_NUM).unwrap();
                b.iter(|| {
                    thread_pool_set(&client_pool, addr);
                });
            }
            tx.send(()).unwrap();
            handle.join().unwrap();
        },
        SERVER_THREAD_NUMS,
    );
}

fn read_rayon_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "read rayon kvstore",
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
                let client_pool = SharedQueueThreadPool::new(CLIENT_THREAD_NUM).unwrap();

                thread_pool_set(&client_pool, addr);

                b.iter(|| {
                    thread_pool_get(&client_pool, addr);
                });
            }
            tx.send(()).unwrap();
            handle.join().unwrap();
        },
        SERVER_THREAD_NUMS,
    );
}

fn write_queued_sled_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "write queued sled kvstore",
        |b, &num| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let store = SledKvStore::open(temp_dir.path()).unwrap();
            let pool = SharedQueueThreadPool::new(*num as u32).unwrap();
            let (tx, rx) = mpsc::channel();
            let server = KvsServer::new(store, pool, Some(rx));
            let port = 4000;
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
            let handle = thread::spawn(move || {
                server.run(addr).unwrap();
            });
            thread::sleep(Duration::from_millis(10));
            {
                let client_pool = SharedQueueThreadPool::new(CLIENT_THREAD_NUM).unwrap();
                b.iter(|| {
                    thread_pool_set(&client_pool, addr);
                });
            }
            tx.send(()).unwrap();
            handle.join().unwrap();
        },
        SERVER_THREAD_NUMS,
    );
}

fn read_queued_sled_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "read queued sled kvstore",
        |b, &num| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let store = SledKvStore::open(temp_dir.path()).unwrap();
            let pool = SharedQueueThreadPool::new(*num as u32).unwrap();
            let (tx, rx) = mpsc::channel();
            let server = KvsServer::new(store, pool, Some(rx));
            let port = 4000;
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
            let handle = thread::spawn(move || {
                server.run(addr).unwrap();
            });
            thread::sleep(Duration::from_millis(10));
            {
                let client_pool = SharedQueueThreadPool::new(CLIENT_THREAD_NUM).unwrap();

                thread_pool_set(&client_pool, addr);

                b.iter(|| {
                    thread_pool_get(&client_pool, addr);
                });
            }
            tx.send(()).unwrap();
            handle.join().unwrap();
        },
        SERVER_THREAD_NUMS,
    );
}

fn write_rayon_sled_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "write rayon sled kvstore",
        |b, &num| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let store = SledKvStore::open(temp_dir.path()).unwrap();
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
                let client_pool = SharedQueueThreadPool::new(CLIENT_THREAD_NUM).unwrap();
                b.iter(|| {
                    thread_pool_set(&client_pool, addr);
                });
            }
            tx.send(()).unwrap();
            handle.join().unwrap();
        },
        SERVER_THREAD_NUMS,
    );
}

fn read_rayon_sled_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "read rayon sled kvstore",
        |b, &num| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let store = SledKvStore::open(temp_dir.path()).unwrap();
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
                let client_pool = SharedQueueThreadPool::new(CLIENT_THREAD_NUM).unwrap();

                thread_pool_set(&client_pool, addr);

                b.iter(|| {
                    thread_pool_get(&client_pool, addr);
                });
            }
            tx.send(()).unwrap();
            handle.join().unwrap();
        },
        SERVER_THREAD_NUMS,
    );
}

criterion_group!(
    write_queued,
    write_queued_kvstore,
    write_queued_sled_kvstore
);
criterion_group!(write_rayon, write_rayon_kvstore, write_rayon_sled_kvstore);
criterion_group!(read_queued, read_queued_kvstore, read_queued_sled_kvstore);
criterion_group!(read_rayon, read_rayon_kvstore, read_rayon_sled_kvstore);
