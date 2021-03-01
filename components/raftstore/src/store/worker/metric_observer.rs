// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};

use crate::store::{ApplyRouter, RaftRouter};
use engine_traits::{KvEngine, RaftEngine};
use prometheus::*;
use tikv_util::worker::{Runnable, RunnableWithTimer};

lazy_static! {
    pub static ref STORE_QUEUE_LEN_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_store_queue_len",
        "Bucketed histogram of store queue length.",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref APPLY_QUEUE_LEN_HISTOGRAM: Histogram = register_histogram!(
        "tikv_raftstore_apply_queue_len",
        "Bucketed histogram of apply queue length.",
        exponential_buckets(1.0, 2.0, 20).unwrap()
    )
    .unwrap();
}

pub struct Task;

impl Display for Task {
    fn fmt(&self, _: &mut Formatter<'_>) -> fmt::Result {
        Ok(())
    }
}

pub struct Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    store_router: RaftRouter<EK, ER>,
    apply_router: ApplyRouter<EK>,
}

impl<EK, ER> Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(store_router: RaftRouter<EK, ER>, apply_router: ApplyRouter<EK>) -> Runner<EK, ER> {
        Runner {
            store_router,
            apply_router,
        }
    }
}

impl<EK, ER> Runnable for Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    type Task = Task;

    fn run(&mut self, _task: Task) {}
}

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

impl<EK, ER> RunnableWithTimer for Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    fn on_timeout(&mut self) {
        // let store_normals;
        // let apply_normals;
        // {
        //     let store_guard = self.store_router.router.normals.lock().unwrap();
        //     store_normals = store_guard.clone();
        // }
        // {
        //     let apply_guard = self.apply_router.router.normals.lock().unwrap();
        //     apply_normals = apply_guard.clone();
        // }
        // for (_, mailbox) in store_normals {
        //     STORE_QUEUE_LEN_HISTOGRAM.observe(mailbox.len() as f64);
        // }
        // for (_, mailbox) in apply_normals {
        //     APPLY_QUEUE_LEN_HISTOGRAM.observe(mailbox.len() as f64);
        // }
    }
    fn get_interval(&self) -> std::time::Duration {
        std::time::Duration::from_millis(METRICS_FLUSH_INTERVAL)
    }
}
