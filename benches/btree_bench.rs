use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gatidb::btree::BTree;

fn bench_insert(c: &mut Criterion) {
    c.bench_function("insert 1000 keys", |b| {
        b.iter(|| {
            let mut tree = BTree::new(black_box(4));
            for i in 0..1000 {
                tree.insert(black_box(i), format!("value_{}", i));
            }
        });
    });
}

fn bench_search(c: &mut Criterion) {
    let mut tree = BTree::new(4);
    for i in 0..1000 {
        tree.insert(i, format!("value_{}", i));
    }

    c.bench_function("search hit", |b| {
        b.iter(|| {
            tree.search(black_box(500));
        });
    });

    c.bench_function("search miss", |b| {
        b.iter(|| {
            tree.search(black_box(9999));
        });
    });
}

criterion_group!(benches, bench_insert, bench_search);
criterion_main!(benches);
