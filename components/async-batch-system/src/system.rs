use std::{marker::PhantomData, sync::Arc};

use batch_system::{Config, Fsm};
use tokio::{
    runtime::{Builder, Runtime},
    sync::{Mutex, Semaphore},
};

pub trait BatchCollector<N: Fsm, C: Fsm>: Clone {
    fn handle_normal(&self, fsm: &mut N, msg: N::Message);
    fn handle_control(&self, fsm: &mut C, msg: C::Message);

    fn finish_batch(&self);
}

struct System<N: Fsm, C: Fsm, Batch: BatchCollector<N, C>> {
    runtime: Arc<Runtime>,
    batch: Batch,
    control: Arc<Mutex<Box<C>>>,
    semaphore: Arc<Semaphore>,

    _phantom: PhantomData<(N, C)>,
}

impl<N: Fsm, C: Fsm, Batch: BatchCollector<N, C>> System<N, C, Batch> {
    pub fn new(cfg: &Config) {
        let builder = Builder::new_multi_thread()
            .worker_threads(cfg.pool_size)
            .enable_time()
            .build();
    }
}
