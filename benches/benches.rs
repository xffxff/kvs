extern crate rand_chacha;
extern crate rand;
use kvs::{KvStore, KvsEngine, SledKVStore};
use tempfile::TempDir;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{Rng, SeedableRng};
use rand::distributions::Alphanumeric;

pub fn kvs_write(c: &mut Criterion) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = KvStore::open(temp_dir.path()).unwrap();

    c.bench_function("kvs write", |b| {
        b.iter(|| {
            let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(10);
            for _ in 0..100 {
                let key_size = rng.gen_range(1, 10);
                let rand_key: String = rng.clone()
                    .sample_iter(&Alphanumeric)
                    .take(key_size)
                    .collect();
                store.set(rand_key, "value1".to_owned()).unwrap();
            }
        });
    });
}

pub fn sled_write(c: &mut Criterion) {
    let temp_dir = TempDir::new().expect("unable to create temporary working directory");
    let mut store = SledKVStore::open(temp_dir.path()).unwrap();

    c.bench_function("sled write", |b| {
        b.iter(|| {
            let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(10);
            for _ in 0..100 {
                let key_size = rng.gen_range(1, 10);
                let rand_key: String = rng.clone()
                    .sample_iter(&Alphanumeric)
                    .take(key_size)
                    .collect();
                store.set(rand_key, "value1".to_owned()).unwrap();
            }
        });
    });
}

criterion_group!(benches, sled_write, kvs_write);
criterion_main!(benches);