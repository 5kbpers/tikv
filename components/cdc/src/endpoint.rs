// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::cell::RefCell;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use collections::HashMap;
use concurrency_manager::ConcurrencyManager;
use crossbeam::atomic::AtomicCell;
use engine_rocks::{RocksEngine, RocksSnapshot};
use engine_traits::{KvEngine, Snapshot};
use futures::compat::Future01CompatExt;
use grpcio::{ChannelBuilder, Environment};
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::event::Event as Event_oneof_event;
use kvproto::cdcpb::*;
use kvproto::kvrpcpb::{CheckLeaderRequest, ExtraOp as TxnExtraOp, LeaderInfo};
use kvproto::metapb::{PeerRole, Region};
use kvproto::raft_cmdpb::RaftCmdResponse;
use kvproto::tikvpb::TikvClient;
use pd_client::PdClient;
use raftstore::coprocessor::CmdBatch;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::{ChangeCmd, ObserveId, StoreMeta};
use raftstore::store::msg::{Callback, ReadResponse, SignificantMsg};
use raftstore::store::RegionSnapshot;
use resolved_ts::{ChangeLog, ScanEntry, ScanMode, ScanTask, ScannerPool, SinkCmd};
use security::SecurityManager;
use tikv::config::CdcConfig;
use tikv::storage::mvcc::{DeltaScanner, ScannerBuilder};
use tikv::storage::txn::TxnEntry;
use tikv::storage::txn::TxnEntryScanner;
use tikv::storage::Statistics;
use tikv_util::lru::LruCache;
use tikv_util::time::Instant;
use tikv_util::worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler};
use txn_types::{
    Key, Lock, LockType, MutationType, OldValue, TimeStamp, TxnExtra, TxnExtraScheduler,
};

use crate::delegate::{Delegate, Downstream, DownstreamId, DownstreamState};
use crate::metrics::*;
use crate::service::{CdcEvent, Conn, ConnID, FeatureGate};
use crate::{Error, Result};

pub enum Deregister {
    Downstream {
        region_id: u64,
        downstream_id: DownstreamId,
        conn_id: ConnID,
        err: Option<Error>,
    },
    Region {
        region_id: u64,
        observe_id: ObserveId,
        err: Error,
    },
    Conn(ConnID),
}

impl fmt::Display for Deregister {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl fmt::Debug for Deregister {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("Deregister");
        match self {
            Deregister::Downstream {
                ref region_id,
                ref downstream_id,
                ref conn_id,
                ref err,
            } => de
                .field("deregister", &"downstream")
                .field("region_id", region_id)
                .field("downstream_id", downstream_id)
                .field("conn_id", conn_id)
                .field("err", err)
                .finish(),
            Deregister::Region {
                ref region_id,
                ref observe_id,
                ref err,
            } => de
                .field("deregister", &"region")
                .field("region_id", region_id)
                .field("observe_id", observe_id)
                .field("err", err)
                .finish(),
            Deregister::Conn(ref conn_id) => de
                .field("deregister", &"conn")
                .field("conn_id", conn_id)
                .finish(),
        }
    }
}

type InitCallback = Box<dyn FnOnce() + Send>;

pub struct OldValueCache {
    pub cache: LruCache<Key, (Option<OldValue>, MutationType)>,
    pub miss_count: usize,
    pub access_count: usize,
}

impl OldValueCache {
    pub fn new(size: usize) -> OldValueCache {
        OldValueCache {
            cache: LruCache::with_capacity(size),
            miss_count: 0,
            access_count: 0,
        }
    }
}

pub enum Task<S: Snapshot> {
    RegisterDownstream {
        request: ChangeDataRequest,
        downstream: Downstream,
        conn_id: ConnID,
        version: semver::Version,
    },
    RegisterConn {
        conn: Conn,
    },
    Deregister(Deregister),
    ChangeLog {
        cmds: Vec<SinkCmd>,
        snapshot: RegionSnapshot<S>,
    },
    ResolvedTs {
        regions: Vec<u64>,
        ts: TimeStamp,
    },
    IncrementalScan {
        region_id: u64,
        downstream_id: DownstreamId,
        entry: ScanEntry,
    },
    // The result of ChangeCmd should be returned from CDC Endpoint to ensure
    // the downstream switches to Normal after the previous commands was sunk.
    InitDownstream {
        region_id: u64,
        downstream_id: DownstreamId,
        response: RaftCmdResponse,
    },
    TxnExtra(TxnExtra),
    Validate(u64, Box<dyn FnOnce(Option<&Delegate>) + Send>),
}

impl<S: Snapshot> fmt::Display for Task<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<S: Snapshot> fmt::Debug for Task<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("CdcTask");
        match self {
            Task::RegisterDownstream {
                ref request,
                ref downstream,
                ref conn_id,
                ref version,
                ..
            } => de
                .field("type", &"register")
                .field("register request", request)
                .field("request", request)
                .field("id", &downstream.get_id())
                .field("conn_id", conn_id)
                .field("version", version)
                .finish(),
            Task::RegisterConn { ref conn } => de
                .field("type", &"open_conn")
                .field("conn_id", &conn.get_id())
                .finish(),
            Task::Deregister(deregister) => de
                .field("type", &"deregister")
                .field("deregister", deregister)
                .finish(),
            Task::IncrementalScan {
                ref region_id,
                ref downstream_id,
                ref entries,
            } => de
                .field("type", &"incremental_scan")
                .field("region_id", &region_id)
                .field("downstream", &downstream_id)
                .field("scan_entries", &entries.len())
                .finish(),
            Task::InitDownstream {
                ref downstream_id, ..
            } => de
                .field("type", &"init_downstream")
                .field("downstream", &downstream_id)
                .finish(),
            Task::TxnExtra(_) => de.field("type", &"txn_extra").finish(),
            Task::ChangeLog { cmds, snapshot } => de.field("type", &"change_log").finish(),
            Task::ResolvedTs { regions, ts } => de.field("type", &"resolevd_ts").finish(),
            Task::Validate(region_id, _) => de.field("region_id", &region_id).finish(),
        }
    }
}

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

pub struct Endpoint<T, E: KvEngine> {
    capture_regions: HashMap<u64, Delegate>,
    connections: HashMap<ConnID, Conn>,
    scheduler: Scheduler<Task<E::Snapshot>>,
    raft_router: T,

    store_meta: Arc<Mutex<StoreMeta>>,
    scanner_pool: ScannerPool<T, E>,

    min_resolved_ts: TimeStamp,
    min_ts_region_id: u64,
    old_value_cache: OldValueCache,
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine> Endpoint<T, E> {
    pub fn new(
        cfg: &CdcConfig,
        scheduler: Scheduler<Task<E::Snapshot>>,
        raft_router: T,
        store_meta: Arc<Mutex<StoreMeta>>,
        scanner_pool: ScannerPool<T, E>,
    ) -> Endpoint<T, E> {
        let ep = Endpoint {
            capture_regions: HashMap::default(),
            connections: HashMap::default(),
            scheduler,
            raft_router,
            store_meta,
            scanner_pool,
            min_resolved_ts: TimeStamp::max(),
            min_ts_region_id: 0,
            old_value_cache: OldValueCache::new(cfg.old_value_cache_size),
        };
        ep
    }

    fn on_deregister(&mut self, deregister: Deregister) {
        info!("cdc deregister"; "deregister" => ?deregister);
        fail_point!("cdc_before_handle_deregister", |_| {});
        match deregister {
            Deregister::Downstream {
                region_id,
                downstream_id,
                conn_id,
                err,
            } => {
                // The peer wants to deregister
                let mut is_last = false;
                if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                    is_last = delegate.unsubscribe(downstream_id, err);
                }
                if let Some(conn) = self.connections.get_mut(&conn_id) {
                    if let Some(id) = conn.downstream_id(region_id) {
                        if downstream_id == id {
                            conn.unsubscribe(region_id);
                        }
                    }
                }
                if is_last {
                    self.capture_regions.remove(&region_id).unwrap();
                    if let Some(reader) = self.store_meta.lock().unwrap().readers.get(&region_id) {
                        reader
                            .txn_extra_op
                            .compare_and_swap(TxnExtraOp::ReadOldValue, TxnExtraOp::Noop);
                    }
                }
            }
            Deregister::Region {
                region_id,
                observe_id,
                err,
            } => {
                // Something went wrong, deregister all downstreams of the region.

                // To avoid ABA problem, we must check the unique ObserveID.
                let need_remove = self
                    .capture_regions
                    .get(&region_id)
                    .map_or(false, |d| d.id == observe_id);
                if need_remove {
                    if let Some(mut delegate) = self.capture_regions.remove(&region_id) {
                        delegate.stop(err);
                    }
                    if let Some(reader) = self.store_meta.lock().unwrap().readers.get(&region_id) {
                        reader.txn_extra_op.store(TxnExtraOp::Noop);
                    }
                    self.connections
                        .iter_mut()
                        .for_each(|(_, conn)| conn.unsubscribe(region_id));
                }
            }
            Deregister::Conn(conn_id) => {
                // The connection is closed, deregister all downstreams of the connection.
                if let Some(conn) = self.connections.remove(&conn_id) {
                    conn.take_downstreams()
                        .into_iter()
                        .for_each(|(region_id, downstream_id)| {
                            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                                if delegate.unsubscribe(downstream_id, None) {
                                    self.capture_regions.remove(&region_id).unwrap();
                                }
                            }
                        });
                }
            }
        }
    }

    pub fn on_register(
        &mut self,
        mut request: ChangeDataRequest,
        mut downstream: Downstream,
        conn_id: ConnID,
        version: semver::Version,
    ) {
        let region_id = request.region_id;
        let downstream_id = downstream.get_id();
        let conn = match self.connections.get_mut(&conn_id) {
            Some(conn) => conn,
            None => {
                error!("register for a nonexistent connection";
                    "region_id" => region_id, "conn_id" => ?conn_id);
                return;
            }
        };
        downstream.set_sink(conn.get_sink());

        // TODO: Add a new task to close incompatible features.
        if let Some(e) = conn.check_version_and_set_feature(version) {
            // The downstream has not registered yet, send error right away.
            downstream.sink_compatibility_error(region_id, e);
            return;
        }
        if !conn.subscribe(request.get_region_id(), downstream_id) {
            downstream.sink_duplicate_error(request.get_region_id());
            error!("duplicate register";
                "region_id" => region_id,
                "conn_id" => ?conn_id,
                "req_id" => request.get_request_id(),
                "downstream_id" => ?downstream_id);
            return;
        }

        info!("cdc register region";
            "region_id" => region_id,
            "conn_id" => ?conn.get_id(),
            "req_id" => request.get_request_id(),
            "downstream_id" => ?downstream_id);
        let mut is_new_delegate = false;
        let delegate = self.capture_regions.entry(region_id).or_insert_with(|| {
            let d = Delegate::new(region_id);
            is_new_delegate = true;
            d
        });

        delegate.subscribe(downstream);
        let change_cmd = ChangeCmd {
            region_id,
            change_observe: None,
        };
        let txn_extra_op = request.get_extra_op();
        if txn_extra_op != TxnExtraOp::Noop {
            delegate.txn_extra_op = request.get_extra_op();
            if let Some(reader) = self.store_meta.lock().unwrap().readers.get(&region_id) {
                reader.txn_extra_op.store(txn_extra_op);
            }
        }

        let scan_mode = match delegate.txn_extra_op {
            TxnExtraOp::ReadOldValue => ScanMode::AllWithOldValue,
            _ => ScanMode::All,
        };
        let mut region = Region::default();
        region.set_id(region_id);
        region.set_region_epoch(request.take_region_epoch());
        let downstream_state = downstream.get_state();
        let downstream_state1 = downstream.get_state();
        let checkpoint_ts = request.checkpoint_ts;
        let scheduler = self.scheduler.clone();
        let scheduler_clone = self.scheduler.clone();

        let task = ScanTask {
            id: downstream_id.into_inner(),
            tag: format!(""),
            mode: scan_mode,
            region,
            checkpoint_ts: checkpoint_ts.into(),
            cancelled: Box::new(move || downstream_state.load() == DownstreamState::Stopped),
            send_entries: Box::new(move |entry| {
                scheduler.schedule(Task::IncrementalScan {
                    region_id,
                    downstream_id,
                    entry,
                });
            }),
            before_start: Some(Box::new(move |response| {
                scheduler.schedule(Task::InitDownstream {
                    region_id,
                    downstream_id,
                    response: response.clone(),
                });
            })),
            on_error: Some(Box::new(move |region, error| {
                scheduler.schedule(Task::Deregister(Deregister::Downstream {
                    region_id: region.id,
                    downstream_id,
                    conn_id,
                    err: Some(Error::Request(error.extract_error_header())),
                }));
            })),
        };
        self.scanner_pool.spawn_task(task);
    }

    pub fn init_downstream(
        &mut self,
        region_id: u64,
        downstream_id: DownstreamId,
        response: RaftCmdResponse,
    ) {
        if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
            assert_eq!(response.responses.len(), 1);
            let index = response.responses[0].get_read_index().read_index;
            delegate.init_downstream(downstream_id, index);
            debug!("downstream was initialized"; "downstream_id" => ?downstream_id);
        }
    }

    pub fn on_change_log(&mut self, cmds: Vec<SinkCmd>, snapshot: RegionSnapshot<E::Snapshot>) {
        fail_point!("cdc_before_handle_multi_batch", |_| {});
        for cmd in cmds {
            let region_id = cmd.region_id;
            let mut deregister = None;
            if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
                if delegate.has_failed() {
                    // Skip the batch if the delegate has failed.
                    continue;
                }
                if let Err(e) = delegate.on_sink_cmd(cmd, snapshot, &mut self.old_value_cache) {
                    assert!(delegate.has_failed());
                    // Delegate has error, deregister the corresponding region.
                    deregister = Some(Deregister::Region {
                        region_id,
                        observe_id: delegate.id,
                        err: e,
                    });
                }
            }
            if let Some(deregister) = deregister {
                self.on_deregister(deregister);
            }
        }
    }

    pub fn on_incremental_scan(
        &mut self,
        region_id: u64,
        downstream_id: DownstreamId,
        entry: ScanEntry,
    ) {
        if let Some(delegate) = self.capture_regions.get_mut(&region_id) {
            delegate.on_scan(downstream_id, entry);
        } else {
            warn!("region not found on incremental scan"; "region_id" => region_id);
        }
    }

    fn broadcast_resolved_ts(&self, regions: Vec<u64>) {
        let mut resolved_ts = ResolvedTs::default();
        resolved_ts.regions = regions;
        resolved_ts.ts = self.min_resolved_ts.into_inner();

        let send_cdc_event = |conn: &Conn, event| {
            if let Err(e) = conn.get_sink().try_send(event) {
                match e {
                    crossbeam::TrySendError::Disconnected(_) => {
                        debug!("send event failed, disconnected";
                            "conn_id" => ?conn.get_id(), "downstream" => conn.get_peer());
                    }
                    crossbeam::TrySendError::Full(_) => {
                        info!("send event failed, full";
                            "conn_id" => ?conn.get_id(), "downstream" => conn.get_peer());
                    }
                }
            }
        };
        for conn in self.connections.values() {
            let features = if let Some(features) = conn.get_feature() {
                features
            } else {
                // None means there is no downsteam registered yet.
                continue;
            };

            if features.contains(FeatureGate::BATCH_RESOLVED_TS) {
                send_cdc_event(conn, CdcEvent::ResolvedTs(resolved_ts.clone()));
            } else {
                // Fallback to previous non-batch resolved ts event.
                for region_id in &resolved_ts.regions {
                    self.broadcast_resolved_ts_compact(*region_id, resolved_ts.ts, conn);
                }
            }
        }
    }

    fn broadcast_resolved_ts_compact(&self, region_id: u64, resolved_ts: u64, conn: &Conn) {
        let downstream_id = match conn.downstream_id(region_id) {
            Some(downstream_id) => downstream_id,
            // No such region registers in the connection.
            None => return,
        };
        let delegate = match self.capture_regions.get(&region_id) {
            Some(delegate) => delegate,
            // No such region registers in the endpoint.
            None => return,
        };
        let downstream = match delegate.downstream(downstream_id) {
            Some(downstream) => downstream,
            // No such downstream registers in the delegate.
            None => return,
        };
        let mut event = Event::default();
        event.region_id = region_id;
        event.event = Some(Event_oneof_event::ResolvedTs(resolved_ts));
        downstream.sink_event(event);
    }

    fn on_register_conn(&mut self, conn: Conn) {
        self.connections.insert(conn.get_id(), conn);
    }

    fn flush_all(&self) {
        self.connections.iter().for_each(|(_, conn)| conn.flush());
    }
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine> Runnable for Endpoint<T, E> {
    type Task = Task<E::Snapshot>;

    fn run(&mut self, task: Task<E::Snapshot>) {
        debug!("run cdc task"; "task" => %task);
        match task {
            Task::RegisterDownstream {
                request,
                downstream,
                conn_id,
                version,
            } => self.on_register(request, downstream, conn_id, version),
            Task::RegisterConn { conn } => self.on_register_conn(conn),
            Task::Deregister(deregister) => self.on_deregister(deregister),
            Task::IncrementalScan {
                region_id,
                downstream_id,
                entry,
            } => {
                self.on_incremental_scan(region_id, downstream_id, entry);
            }
            Task::ChangeLog { cmds, snapshot } => self.on_change_log(cmds, snapshot),
            Task::ResolvedTs { regions, ts } => self.on_resolved_ts(regions, ts),
            Task::InitDownstream {
                region_id,
                downstream_id,
                response,
            } => self.init_downstream(region_id, downstream_id, response),
            Task::TxnExtra(txn_extra) => {
                for (k, v) in txn_extra.old_values {
                    self.old_value_cache.cache.insert(k, v);
                }
            }
            Task::Validate(region_id, validate) => {
                validate(self.capture_regions.get(&region_id));
            }
        }
        self.flush_all();
    }
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine> RunnableWithTimer for Endpoint<T, E> {
    fn on_timeout(&mut self) {
        CDC_CAPTURED_REGION_COUNT.set(self.capture_regions.len() as i64);
        if self.min_resolved_ts != TimeStamp::max() {
            CDC_MIN_RESOLVED_TS_REGION.set(self.min_ts_region_id as i64);
            CDC_MIN_RESOLVED_TS.set(self.min_resolved_ts.physical() as i64);
        }
        self.min_resolved_ts = TimeStamp::max();
        self.min_ts_region_id = 0;

        let cache_size: usize = self
            .old_value_cache
            .cache
            .iter()
            .map(|(k, v)| k.as_encoded().len() + v.0.as_ref().map_or(0, |v| v.size()))
            .sum();
        CDC_OLD_VALUE_CACHE_BYTES.set(cache_size as i64);
        CDC_OLD_VALUE_CACHE_ACCESS.add(self.old_value_cache.access_count as i64);
        CDC_OLD_VALUE_CACHE_MISS.add(self.old_value_cache.miss_count as i64);
        self.old_value_cache.access_count = 0;
        self.old_value_cache.miss_count = 0;
    }

    fn get_interval(&self) -> Duration {
        // Currently there is only one timeout for CDC.
        Duration::from_millis(METRICS_FLUSH_INTERVAL)
    }
}

pub struct CdcTxnExtraScheduler<S: Snapshot> {
    scheduler: Scheduler<Task<S>>,
}

impl<S: Snapshot> CdcTxnExtraScheduler<S> {
    pub fn new(scheduler: Scheduler<Task<S>>) -> CdcTxnExtraScheduler<S> {
        CdcTxnExtraScheduler { scheduler }
    }
}

impl<S: Snapshot> TxnExtraScheduler for CdcTxnExtraScheduler<S> {
    fn schedule(&self, txn_extra: TxnExtra) {
        if let Err(e) = self.scheduler.schedule(Task::TxnExtra(txn_extra)) {
            error!("cdc schedule txn extra failed"; "err" => ?e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use collections::HashSet;
    use engine_traits::DATA_CFS;
    #[cfg(feature = "prost-codec")]
    use kvproto::cdcpb::event::Event as Event_oneof_event;
    use kvproto::errorpb::Error as ErrorHeader;
    use raftstore::errors::Error as RaftStoreError;
    use raftstore::store::msg::CasualMessage;
    use std::collections::BTreeMap;
    use std::fmt::Display;
    use std::sync::mpsc::{channel, Receiver, RecvTimeoutError, Sender};
    use tempfile::TempDir;
    use test_raftstore::MockRaftStoreRouter;
    use test_raftstore::TestPdClient;
    use tikv::storage::kv::Engine;
    use tikv::storage::txn::tests::{must_acquire_pessimistic_lock, must_prewrite_put};
    use tikv::storage::TestEngineBuilder;
    use tikv_util::config::ReadableDuration;
    use tikv_util::mpsc::batch;
    use tikv_util::worker::{dummy_scheduler, LazyWorker, ReceiverWrapper};

    struct ReceiverRunnable<T: Display + Send> {
        tx: Sender<T>,
    }

    impl<T: Display + Send + 'static> Runnable for ReceiverRunnable<T> {
        type Task = T;

        fn run(&mut self, task: T) {
            self.tx.send(task).unwrap();
        }
    }

    fn new_receiver_worker<T: Display + Send + 'static>() -> (LazyWorker<T>, Receiver<T>) {
        let (tx, rx) = channel();
        let runnable = ReceiverRunnable { tx };
        let mut worker = LazyWorker::new("test-receiver-worker");
        worker.start(runnable);
        (worker, rx)
    }

    fn mock_initializer() -> (LazyWorker<Task>, Runtime, Initializer, Receiver<Task>) {
        let (receiver_worker, rx) = new_receiver_worker();

        let pool = Builder::new()
            .threaded_scheduler()
            .thread_name("test-initializer-worker")
            .core_threads(4)
            .build()
            .unwrap();
        let downstream_state = Arc::new(AtomicCell::new(DownstreamState::Normal));
        let initializer = Initializer {
            sched: receiver_worker.scheduler(),

            region_id: 1,
            observe_id: ObserveId::new(),
            downstream_id: DownstreamId::new(),
            downstream_state,
            conn_id: ConnID::new(),
            checkpoint_ts: 1.into(),
            txn_extra_op: TxnExtraOp::Noop,
        };

        (receiver_worker, pool, initializer, rx)
    }

    fn mock_endpoint(
        cfg: &CdcConfig,
    ) -> (
        Endpoint<MockRaftStoreRouter>,
        MockRaftStoreRouter,
        ReceiverWrapper<Task>,
    ) {
        let (task_sched, task_rx) = dummy_scheduler();
        let raft_router = MockRaftStoreRouter::new();
        let observer = CdcObserver::new(task_sched.clone());
        let pd_client = Arc::new(TestPdClient::new(0, true));
        let env = Arc::new(Environment::new(1));
        let security_mgr = Arc::new(SecurityManager::default());
        let ep = Endpoint::new(
            cfg,
            pd_client,
            task_sched,
            raft_router.clone(),
            Arc::new(Mutex::new(StoreMeta::new(0))),
            env,
            security_mgr,
        );
        (ep, raft_router, task_rx)
    }

    #[test]
    fn test_initializer_build_resolver() {
        let (mut worker, _pool, mut initializer, rx) = mock_initializer();

        let temp = TempDir::new().unwrap();
        let engine = TestEngineBuilder::new()
            .path(temp.path())
            .cfs(DATA_CFS)
            .build()
            .unwrap();

        let mut expected_locks = BTreeMap::<TimeStamp, HashSet<Vec<u8>>>::new();

        // Pessimistic locks should not be tracked
        for i in 0..10 {
            let k = &[b'k', i];
            let ts = TimeStamp::new(i as _);
            must_acquire_pessimistic_lock(&engine, k, k, ts, ts);
        }

        for i in 10..100 {
            let (k, v) = (&[b'k', i], &[b'v', i]);
            let ts = TimeStamp::new(i as _);
            must_prewrite_put(&engine, k, v, k, ts);
            expected_locks.entry(ts).or_default().insert(k.to_vec());
        }

        let region = Region::default();
        let snap = engine.snapshot(Default::default()).unwrap();

        let check_result = || loop {
            let task = rx.recv().unwrap();
            match task {
                Task::ResolverReady { resolver, .. } => {
                    // assert_eq!(resolver.locks(), &expected_locks);
                    return;
                }
                Task::IncrementalScan { .. } => continue,
                t => panic!("unepxected task {} received", t),
            }
        };

        initializer.async_incremental_scan(snap.clone(), region.clone());
        check_result();
        initializer.batch_size = 1000;
        initializer.async_incremental_scan(snap.clone(), region.clone());
        check_result();

        initializer.batch_size = 10;
        initializer.async_incremental_scan(snap.clone(), region.clone());
        check_result();

        initializer.batch_size = 11;
        initializer.async_incremental_scan(snap.clone(), region.clone());
        check_result();

        initializer.build_resolver = false;
        initializer.async_incremental_scan(snap.clone(), region.clone());

        loop {
            let task = rx.recv_timeout(Duration::from_secs(1));
            match task {
                Ok(Task::IncrementalScan { .. }) => continue,
                Ok(t) => panic!("unepxected task {} received", t),
                Err(RecvTimeoutError::Timeout) => break,
                Err(e) => panic!("unexpected err {:?}", e),
            }
        }

        // Test cancellation.
        initializer.downstream_state.store(DownstreamState::Stopped);
        initializer.async_incremental_scan(snap, region);

        loop {
            let task = rx.recv_timeout(Duration::from_secs(1));
            match task {
                Ok(t) => panic!("unepxected task {} received", t),
                Err(RecvTimeoutError::Timeout) => break,
                Err(e) => panic!("unexpected err {:?}", e),
            }
        }

        worker.stop();
    }

    #[test]
    fn test_raftstore_is_busy() {
        let (tx, _rx) = batch::unbounded(1);
        let (mut ep, raft_router, mut task_rx) = mock_endpoint(&CdcConfig::default());
        // Fill the channel.
        let _raft_rx = raft_router.add_region(1 /* region id */, 1 /* cap */);
        loop {
            if let Err(RaftStoreError::Transport(_)) =
                raft_router.send_casual_msg(1, CasualMessage::ClearRegionSize)
            {
                break;
            }
        }
        // Make sure channel is full.
        raft_router
            .send_casual_msg(1, CasualMessage::ClearRegionSize)
            .unwrap_err();

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch, 0, conn_id);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_regions.len(), 1);

        for _ in 0..5 {
            if let Ok(Some(Task::Deregister(Deregister::Downstream { err, .. }))) =
                task_rx.recv_timeout(Duration::from_secs(1))
            {
                if let Some(Error::Request(err)) = err {
                    assert!(!err.has_server_is_busy());
                }
            }
        }
    }

    #[test]
    fn test_register() {
        let (mut ep, raft_router, _task_rx) = mock_endpoint(&CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        });
        let _raft_rx = raft_router.add_region(1 /* region id */, 100 /* cap */);
        let (tx, rx) = batch::unbounded(1);

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 1, conn_id);
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        assert_eq!(ep.capture_regions.len(), 1);

        // duplicate request error.
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 2, conn_id);
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::Event(mut e) = cdc_event {
            assert_eq!(e.region_id, 1);
            assert_eq!(e.request_id, 2);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_duplicate_request());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        assert_eq!(ep.capture_regions.len(), 1);

        // Compatibility error.
        let downstream = Downstream::new("".to_string(), region_epoch, 3, conn_id);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::Event(mut e) = cdc_event {
            assert_eq!(e.region_id, 1);
            assert_eq!(e.request_id, 3);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::Error(err) => {
                    assert!(err.has_compatibility());
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        assert_eq!(ep.capture_regions.len(), 1);
    }

    #[test]
    fn test_feature_gate() {
        let (mut ep, raft_router, _task_rx) = mock_endpoint(&CdcConfig {
            min_ts_interval: ReadableDuration(Duration::from_secs(60)),
            ..Default::default()
        });
        let _raft_rx = raft_router.add_region(1 /* region id */, 100 /* cap */);

        let (tx, rx) = batch::unbounded(1);
        let mut region = Region::default();
        region.set_id(1);
        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id);
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        let mut resolver = Resolver::new(1);
        resolver.init();
        let observe_id = ep.capture_regions[&1].id;
        ep.on_region_ready(observe_id, resolver, region.clone());
        ep.run(Task::MinTS {
            regions: vec![1],
            min_ts: TimeStamp::from(1),
        });
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::ResolvedTs(r) = cdc_event {
            assert_eq!(r.regions, vec![1]);
            assert_eq!(r.ts, 1);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }

        // Register region 2 to the conn.
        req.set_region_id(2);
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id);
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 6),
        });
        let mut resolver = Resolver::new(2);
        resolver.init();
        region.set_id(2);
        let observe_id = ep.capture_regions[&2].id;
        ep.on_region_ready(observe_id, resolver, region);
        ep.run(Task::MinTS {
            regions: vec![1, 2],
            min_ts: TimeStamp::from(2),
        });
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::ResolvedTs(mut r) = cdc_event {
            r.regions.as_mut_slice().sort_unstable();
            assert_eq!(r.regions, vec![1, 2]);
            assert_eq!(r.ts, 2);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }

        // Register region 3 to another conn which is not support batch resolved ts.
        let (tx, rx2) = batch::unbounded(1);
        let mut region = Region::default();
        region.set_id(3);
        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        req.set_region_id(3);
        let downstream = Downstream::new("".to_string(), region_epoch, 3, conn_id);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(4, 0, 5),
        });
        let mut resolver = Resolver::new(3);
        resolver.init();
        region.set_id(3);
        let observe_id = ep.capture_regions[&3].id;
        ep.on_region_ready(observe_id, resolver, region);
        ep.run(Task::MinTS {
            regions: vec![1, 2, 3],
            min_ts: TimeStamp::from(3),
        });
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::ResolvedTs(mut r) = cdc_event {
            r.regions.as_mut_slice().sort_unstable();
            // Although region 3 is not register in the first conn, batch resolved ts
            // sends all region ids.
            assert_eq!(r.regions, vec![1, 2, 3]);
            assert_eq!(r.ts, 3);
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
        let cdc_event = rx2.recv_timeout(Duration::from_millis(500)).unwrap();
        if let CdcEvent::Event(mut e) = cdc_event {
            assert_eq!(e.region_id, 3);
            assert_eq!(e.request_id, 3);
            let event = e.event.take().unwrap();
            match event {
                Event_oneof_event::ResolvedTs(ts) => {
                    assert_eq!(ts, 3);
                }
                other => panic!("unknown event {:?}", other),
            }
        } else {
            panic!("unknown cdc event {:?}", cdc_event);
        }
    }

    #[test]
    fn test_deregister() {
        let (mut ep, raft_router, _task_rx) = mock_endpoint(&CdcConfig::default());
        let _raft_rx = raft_router.add_region(1 /* region id */, 100 /* cap */);
        let (tx, rx) = batch::unbounded(1);

        let conn = Conn::new(tx, String::new());
        let conn_id = conn.get_id();
        ep.run(Task::OpenConn { conn });
        let mut req_header = Header::default();
        req_header.set_cluster_id(0);
        let mut req = ChangeDataRequest::default();
        req.set_region_id(1);
        let region_epoch = req.get_region_epoch().clone();
        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id);
        let downstream_id = downstream.get_id();
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_regions.len(), 1);

        let mut err_header = ErrorHeader::default();
        err_header.set_not_leader(Default::default());
        let deregister = Deregister::Downstream {
            region_id: 1,
            downstream_id,
            conn_id,
            err: Some(Error::Request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
        loop {
            let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
            if let CdcEvent::Event(mut e) = cdc_event {
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Error(err) => {
                        assert!(err.has_not_leader());
                        break;
                    }
                    other => panic!("unknown event {:?}", other),
                }
            }
        }
        assert_eq!(ep.capture_regions.len(), 0);

        let downstream = Downstream::new("".to_string(), region_epoch.clone(), 0, conn_id);
        let new_downstream_id = downstream.get_id();
        ep.run(Task::Register {
            request: req.clone(),
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_regions.len(), 1);

        let deregister = Deregister::Downstream {
            region_id: 1,
            downstream_id,
            conn_id,
            err: Some(Error::Request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
        assert!(rx.recv_timeout(Duration::from_millis(200)).is_err());
        assert_eq!(ep.capture_regions.len(), 1);

        let deregister = Deregister::Downstream {
            region_id: 1,
            downstream_id: new_downstream_id,
            conn_id,
            err: Some(Error::Request(err_header.clone())),
        };
        ep.run(Task::Deregister(deregister));
        let cdc_event = rx.recv_timeout(Duration::from_millis(500)).unwrap();
        loop {
            if let CdcEvent::Event(mut e) = cdc_event {
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Error(err) => {
                        assert!(err.has_not_leader());
                        break;
                    }
                    other => panic!("unknown event {:?}", other),
                }
            }
        }
        assert_eq!(ep.capture_regions.len(), 0);

        // Stale deregister should be filtered.
        let downstream = Downstream::new("".to_string(), region_epoch, 0, conn_id);
        ep.run(Task::Register {
            request: req,
            downstream,
            conn_id,
            version: semver::Version::new(0, 0, 0),
        });
        assert_eq!(ep.capture_regions.len(), 1);
        let deregister = Deregister::Region {
            region_id: 1,
            // A stale ObserveID (different from the actual one).
            observe_id: ObserveId::new(),
            err: Error::Request(err_header),
        };
        ep.run(Task::Deregister(deregister));
        match rx.recv_timeout(Duration::from_millis(500)) {
            Err(_) => (),
            Ok(other) => panic!("unknown event {:?}", other),
        }
        assert_eq!(ep.capture_regions.len(), 1);
    }
}
