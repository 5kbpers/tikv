use std::fmt;
use std::time;

use engine_traits::{Iterable, Iterator, KvEngine, SeekKey};
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
    iterators: Vec<<E::Snapshot as Iterable>::Iterator>,
    engine: E,
}

impl<E: KvEngine> HoldSnapshotRunner<E> {
    pub fn new(engine: E) -> Self {
        HoldSnapshotRunner {
            snapshots: vec![],
            iterators: vec![],
            engine,
        }
    }

    pub fn new_timer(&self) -> Timer<()> {
        let mut timer = Timer::new(1);
        timer.add_task(time::Duration::from_secs(10), ());
        timer
    }
}

impl<E: KvEngine> Runnable<Task> for HoldSnapshotRunner<E> {
    fn run(&mut self, _: Task) {}
}

// impl<E: KvEngine> RunnableWithTimer<Task, ()> for HoldSnapshotRunner<E> {
//     fn on_timeout(&mut self, timer: &mut Timer<()>, _: ()) {
//         let snapshots = (0..25000)
//             .map(|_| self.engine.snapshot())
//             .collect::<Vec<_>>();
//         let iterators = snapshots
//             .iter()
//             .map(|s| {
//                 let mut iter = s.iterator().unwrap();
//                 iter.seek(SeekKey::Start).unwrap();
//                 iter
//             })
//             .collect::<Vec<_>>();
//         let _ = std::mem::replace(&mut self.snapshots, snapshots);
//         let _ = std::mem::replace(&mut self.iterators, iterators);
//         timer.add_task(time::Duration::from_secs(40), ());
//     }
// }

impl<E: KvEngine> RunnableWithTimer<Task, ()> for HoldSnapshotRunner<E> {
    fn on_timeout(&mut self, _: &mut Timer<()>, _: ()) {
        loop {
            let snap = self.engine.snapshot();
            let mut iter = snap.iterator().unwrap();
            iter.seek(SeekKey::Start).unwrap();
            if self.snapshots.len() == 3000 {
                let _ = self.snapshots.pop();
                let _ = self.iterators.pop();
            }
            self.snapshots.push(snap);
            self.iterators.push(iter);
            std::thread::sleep(time::Duration::from_millis(1));
        }
    }
}
