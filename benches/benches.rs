extern crate rand_chacha;
extern crate rand;
use kvs::{KvStore, KvsEngine, SledKVStore};
use tempfile::TempDir;
use criterion::{criterion_group, criterion_main, Criterion};
use rand::{Rng, SeedableRng};
use rand::distributions::Alphanumeric;

fn random_string(rng: &mut impl rand::RngCore) -> String {
    let size = rng.gen_range(1, 100000);
    let rand_string = rng
        .sample_iter(&Alphanumeric)
        .take(size)
        .collect();
    return rand_string;
}

pub fn kvs_write(c: &mut Criterion) {
    c.bench_function("kvs write", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let mut store = KvStore::open(temp_dir.path()).unwrap();

            let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(10);
            for _ in 0..100 {
                let rand_key = random_string(&mut rng);
                let rand_value = random_string(&mut rng);
                store.set(rand_key, rand_value).unwrap();
            }
        });
    });
}

pub fn sled_write(c: &mut Criterion) {
    c.bench_function("sled write", |b| {
        b.iter(|| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let mut store = SledKVStore::open(temp_dir.path()).unwrap();

            let mut rng = rand_chacha::ChaCha8Rng::seed_from_u64(10);
            for _ in 0..100 {
                let rand_key = random_string(&mut rng);
                let rand_value = random_string(&mut rng);
                store.set(rand_key, rand_value).unwrap();
            }
        });
    });
}

criterion_group!(benches, kvs_write, sled_write);
criterion_main!(benches);