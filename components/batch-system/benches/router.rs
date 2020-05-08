// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use batch_system::test_runner::*;
use batch_system::*;
use criterion::*;
use std::time::Duration;

fn bench_send(c: &mut Criterion) {
    let (control_tx, control_fsm) = Runner::new(100000);
    let (router, mut system) =
        batch_system::create_system(2, 2, Duration::from_secs(5), control_tx, control_fsm);
    system.spawn("test".to_owned(), Builder::new());
    let (normal_tx, normal_fsm) = Runner::new(100000);
    let normal_box = BasicMailbox::new(normal_tx, normal_fsm);
    router.register(1, normal_box);

    c.bench_function("router::send", |b| {
        b.iter(|| {
            router.send(1, Message::Loop(0)).unwrap();
        })
    });
    system.shutdown();
}

criterion_group!(benches, bench_send);
criterion_main!(benches);
