use engine_traits::{KvEngine, Snapshot};
use raftstore::store::fsm::{ChangeCmd, ChangeObserve, ObserveID, ObserveRange, StoreMeta};
use raftstore::store::RegionSnapshot;

use crate::cmd::ChangeLog;

pub enum SinkCmd {
    // region id -> resolved ts
    ResolvedTs(Vec<(u64, u64)>),
    // region error
    Error(),
    // ChangeLog
    ChangeLog {
        region_id: u64,
        observe_id: ObserveID,
        logs: Vec<ChangeLog>,
    },
}

pub trait CmdSinker<S: Snapshot>: Send {
    fn sink_cmd(&mut self, cmd: Vec<SinkCmd>, snapshot: RegionSnapshot<S>);

    fn sink_resolved_ts(&mut self);
}
