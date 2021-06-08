use std::fmt;
use std::time;

use engine_traits::KvEngine;
use tikv_util::timer::Timer;
use tikv_util::worker::{Runnable, RunnableWithTimer};

#[derive(Debug)]
pub struct Task();

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub struct HoldSnapshotRunner<E: KvEngine> {
    snapshots: Vec<E::Snapshot>,
    engine: E,
}

impl<E: KvEngine> HoldSnapshotRunner<E> {
    pub fn new(engine: E) -> Self {
        HoldSnapshotRunner {
            snapshots: vec![],
            engine,
        }
    }

    pub fn new_timer(&self) -> Timer<()> {
        let mut timer = Timer::new(1);
        timer.add_task(time::Duration::from_secs(30), ());
        timer
    }
}

impl<E: KvEngine> Runnable<Task> for HoldSnapshotRunner<E> {
    fn run(&mut self, _: Task) {}
}

impl<E: KvEngine> RunnableWithTimer<Task, ()> for HoldSnapshotRunner<E> {
    fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
        let snapshots = (0..2000).map(|_| self.engine.snapshot()).collect();
        let _ = std::mem::replace(&mut self.snapshots, snapshots);
        timer.add_task(time::Duration::from_secs(30), ());
    }
}
