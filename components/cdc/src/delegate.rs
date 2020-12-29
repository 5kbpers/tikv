// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam::atomic::AtomicCell;
use engine_traits::Snapshot;
#[cfg(feature = "prost-codec")]
use kvproto::cdcpb::{
    event::{
        row::OpType as EventRowOpType, Entries as EventEntries, Event as Event_oneof_event,
        LogType as EventLogType, Row as EventRow,
    },
    Compatibility, DuplicateRequest as ErrorDuplicateRequest, Error as EventError, Event,
};
#[cfg(not(feature = "prost-codec"))]
use kvproto::cdcpb::{
    Compatibility, DuplicateRequest as ErrorDuplicateRequest, Error as EventError, Event,
    EventEntries, EventLogType, EventRow, EventRowOpType, Event_oneof_event,
};
use kvproto::errorpb;
use kvproto::kvrpcpb::ExtraOp as TxnExtraOp;
use kvproto::metapb::RegionEpoch;
use raftstore::store::fsm::ObserveId;
use raftstore::store::RegionSnapshot;
use resolved_ts::{ChangeLog, ChangeRow, ScanEntry, SinkCmd};
use tikv::storage::txn::TxnEntry;
use tikv::storage::{Snapshot as EngineSnapshot, Statistics};
use tikv_util::mpsc::batch::Sender as BatchSender;
use tikv_util::time::Instant;
use txn_types::{Key, Lock, LockType, MutationType, TimeStamp, Value, WriteType};

use crate::endpoint::OldValueCache;
use crate::metrics::*;
use crate::reader::OldValueReader;
use crate::service::{CdcEvent, ConnID};
use crate::{Error, Result};

const EVENT_MAX_SIZE: usize = 6 * 1024 * 1024; // 6MB
static DOWNSTREAM_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier of a Downstream.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct DownstreamId(usize);

impl DownstreamId {
    pub fn new() -> DownstreamId {
        DownstreamId(DOWNSTREAM_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }

    pub fn into_inner(&self) -> usize {
        self.0
    }
}

impl From<usize> for DownstreamId {
    fn from(id: usize) -> DownstreamId {
        DownstreamId(id)
    }
}

impl From<&usize> for DownstreamId {
    fn from(id: &usize) -> DownstreamId {
        DownstreamId(*id)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DownstreamState {
    Uninitialized,
    Normal,
    Stopped,
}

impl Default for DownstreamState {
    fn default() -> Self {
        Self::Uninitialized
    }
}

#[derive(Clone)]
pub struct Downstream {
    // TODO: include cdc request.
    /// A unique identifier of the Downstream.
    pub id: DownstreamId,
    pub state: Arc<AtomicCell<DownstreamState>>,
    // The reqeust ID set by CDC to identify events corresponding different requests.
    req_id: u64,
    conn_id: ConnID,
    // The IP address of downstream.
    peer: String,
    region_epoch: RegionEpoch,
    sink: Option<BatchSender<CdcEvent>>,
    start_log_index: u64,
}

impl Downstream {
    /// Create a Downsteam.
    ///
    /// peer is the address of the downstream.
    /// sink sends data to the downstream.
    pub fn new(
        peer: String,
        region_epoch: RegionEpoch,
        req_id: u64,
        conn_id: ConnID,
    ) -> Downstream {
        Downstream {
            id: DownstreamId::new(),
            req_id,
            conn_id,
            peer,
            region_epoch,
            sink: None,
            state: Arc::new(AtomicCell::new(DownstreamState::default())),
            start_log_index: 0,
        }
    }

    /// Sink events to the downstream.
    /// The size of `Error` and `ResolvedTS` are considered zero.
    pub fn sink_event(&self, mut event: Event) {
        event.set_request_id(self.req_id);
        if self.sink.is_none() {
            info!("drop event, no sink";
                "conn_id" => ?self.conn_id, "downstream_id" => ?self.id);
            return;
        }
        let sink = self.sink.as_ref().unwrap();
        if let Err(e) = sink.try_send(CdcEvent::Event(event)) {
            match e {
                crossbeam::TrySendError::Disconnected(_) => {
                    debug!("send event failed, disconnected";
                        "conn_id" => ?self.conn_id, "downstream_id" => ?self.id);
                }
                crossbeam::TrySendError::Full(_) => {
                    info!("send event failed, full";
                        "conn_id" => ?self.conn_id, "downstream_id" => ?self.id);
                }
            }
        }
    }

    pub fn set_sink(&mut self, sink: BatchSender<CdcEvent>) {
        self.sink = Some(sink);
    }

    pub fn get_id(&self) -> DownstreamId {
        self.id
    }

    pub fn get_state(&self) -> Arc<AtomicCell<DownstreamState>> {
        self.state.clone()
    }

    pub fn get_conn_id(&self) -> ConnID {
        self.conn_id
    }

    pub fn sink_duplicate_error(&self, region_id: u64) {
        let mut change_data_event = Event::default();
        let mut cdc_err = EventError::default();
        let mut err = ErrorDuplicateRequest::default();
        err.set_region_id(region_id);
        cdc_err.set_duplicate_request(err);
        change_data_event.event = Some(Event_oneof_event::Error(cdc_err));
        change_data_event.region_id = region_id;
        self.sink_event(change_data_event);
    }

    // TODO: merge it into Delegate::error_event.
    pub fn sink_compatibility_error(&self, region_id: u64, compat: Compatibility) {
        let mut change_data_event = Event::default();
        let mut cdc_err = EventError::default();
        cdc_err.set_compatibility(compat);
        change_data_event.event = Some(Event_oneof_event::Error(cdc_err));
        change_data_event.region_id = region_id;
        self.sink_event(change_data_event);
    }
}

/// A CDC delegate of a raftstore region peer.
///
/// It converts raft commands into CDC events and broadcast to downstreams.
/// It also track trancation on the fly in order to compute resolved ts.
pub struct Delegate {
    pub id: ObserveId,
    pub region_id: u64,
    pub downstreams: Vec<Downstream>,
    failed: bool,
    pub txn_extra_op: TxnExtraOp,
}

impl Delegate {
    /// Create a Delegate the given region.
    pub fn new(region_id: u64) -> Delegate {
        Delegate {
            region_id,
            id: ObserveId::new(),
            downstreams: Vec::new(),
            failed: false,
            txn_extra_op: TxnExtraOp::default(),
        }
    }

    /// Return false if subscribe failed.
    pub fn subscribe(&mut self, downstream: Downstream) {
        self.downstreams.push(downstream);
    }

    pub fn downstream(&self, downstream_id: DownstreamId) -> Option<&Downstream> {
        self.downstreams.iter().find(|d| d.id == downstream_id)
    }

    pub fn downstream_mut(&mut self, downstream_id: DownstreamId) -> Option<&mut Downstream> {
        self.downstreams.iter_mut().find(|d| d.id == downstream_id)
    }

    pub fn init_downstream(&mut self, downstream_id: DownstreamId, index: u64) {
        if let Some(downstream) = self.downstream_mut(downstream_id) {
            let state = downstream.get_state();
            state.compare_and_swap(DownstreamState::Uninitialized, DownstreamState::Normal);
            downstream.start_log_index = index;
        }
    }

    pub fn unsubscribe(&mut self, id: DownstreamId, err: Option<Error>) -> bool {
        let change_data_error = err.map(|err| self.error_event(err));
        self.downstreams.retain(|d| {
            if d.id == id {
                if let Some(change_data_error) = change_data_error.clone() {
                    d.sink_event(change_data_error);
                }
                d.state.store(DownstreamState::Stopped);
            }
            d.id != id
        });
        let is_last = self.downstreams.is_empty();
        is_last
    }

    fn error_event(&self, err: Error) -> Event {
        let mut change_data_event = Event::default();
        let mut cdc_err = EventError::default();
        let mut err = err.extract_error_header();
        if err.has_not_leader() {
            let not_leader = err.take_not_leader();
            cdc_err.set_not_leader(not_leader);
        } else if err.has_epoch_not_match() {
            let epoch_not_match = err.take_epoch_not_match();
            cdc_err.set_epoch_not_match(epoch_not_match);
        } else {
            // TODO: Add more errors to the cdc protocol
            let mut region_not_found = errorpb::RegionNotFound::default();
            region_not_found.set_region_id(self.region_id);
            cdc_err.set_region_not_found(region_not_found);
        }
        change_data_event.event = Some(Event_oneof_event::Error(cdc_err));
        change_data_event.region_id = self.region_id;
        change_data_event
    }

    pub fn mark_failed(&mut self) {
        self.failed = true;
    }

    pub fn has_failed(&self) -> bool {
        self.failed
    }

    /// Stop the delegate
    ///
    /// This means the region has met an unrecoverable error for CDC.
    /// It broadcasts errors to all downstream and stops.
    pub fn stop(&mut self, err: Error) {
        self.mark_failed();

        info!("region met error";
            "region_id" => self.region_id, "error" => ?err);
        for d in &self.downstreams {
            d.state.store(DownstreamState::Stopped);
        }
        self.broadcast_error(err);
    }

    fn broadcast_error(&self, err: Error) {
        assert!(
            !self.downstreams.is_empty(),
            "region {} miss downstream",
            self.region_id
        );
        let change_data_err = self.error_event(err);
        for downstream in &self.downstreams {
            downstream.sink_event(change_data_err);
        }
    }

    fn broadcast_entries(&self, index: u64, event_entries: EventEntries) {
        assert!(
            !self.downstreams.is_empty(),
            "region {} miss downstream",
            self.region_id
        );
        let mut change_data_event = Event::default();
        change_data_event.region_id = self.region_id;
        change_data_event.index = index;
        change_data_event.event = Some(Event_oneof_event::Entries(event_entries));
        for downstream in &self.downstreams {
            if downstream.state.load() != DownstreamState::Normal
                || downstream.start_log_index <= index
            {
                continue;
            }
            downstream.sink_event(change_data_event.clone());
        }
    }

    pub fn on_scan(&mut self, downstream_id: DownstreamId, entry: ScanEntry) {
        let downstream = if let Some(d) = self.downstreams.iter().find(|d| d.id == downstream_id) {
            d
        } else {
            warn!("downstream not found"; "downstream_id" => ?downstream_id, "region_id" => self.region_id);
            return;
        };
        let rows = match entry {
            ScanEntry::TxnEntry(rows) => rows,
            ScanEntry::None => {
                let mut row = EventRow::default();
                // This type means scan has finished.
                set_event_row_type(&mut row, EventLogType::Initialized);
                let mut event_entries = EventEntries::default();
                event_entries.mut_entries().push(row);
                let mut event = Event::default();
                event.region_id = self.region_id;
                event.event = Some(Event_oneof_event::Entries(event_entries));
                downstream.sink_event(event);
                return;
            }
            _ => panic!(),
        };

        let entries_len = rows.len();
        let mut event_rows = vec![Vec::with_capacity(entries_len)];
        let mut current_rows_size: usize = 0;
        for (r, old_value) in rows {
            if let Some((row, _)) = decode_change_row(r) {
                row.old_value = old_value.unwrap_or_default();
                let row_size = row.key.len() + row.value.len() + row.old_value.len();
                if current_rows_size + row_size >= EVENT_MAX_SIZE {
                    event_rows.push(Vec::with_capacity(entries_len));
                    current_rows_size = 0;
                }
                current_rows_size += row_size;
                event_rows.last_mut().unwrap().push(row);
            }
        }

        for rs in event_rows {
            if !rs.is_empty() {
                let mut event_entries = EventEntries::default();
                event_entries.entries = rs.into();
                let mut event = Event::default();
                event.region_id = self.region_id;
                event.event = Some(Event_oneof_event::Entries(event_entries));
                downstream.sink_event(event);
            }
        }
    }

    pub fn on_sink_cmd<S: Snapshot>(
        &mut self,
        cmd: SinkCmd,
        snapshot: RegionSnapshot<S>,
        old_value_cache: &mut OldValueCache,
    ) -> Result<()> {
        let reader = OldValueReader::new(snapshot);
        for log in cmd.logs {
            match log {
                ChangeLog::Error(e) => {
                    self.mark_failed();
                    return Err(Error::Request(e));
                }
                ChangeLog::Rows { index, rows } => {
                    let event_rows: Vec<_> = rows
                        .into_iter()
                        .map(|r| {
                            decode_change_row(r).and_then(|(mut row, for_update_ts)| {
                                if let Some(for_update_ts) = for_update_ts {
                                    if self.txn_extra_op == TxnExtraOp::ReadOldValue {
                                        row.old_value = read_old_value(
                                            &row.key,
                                            row.start_ts.into(),
                                            for_update_ts,
                                            &mut reader,
                                            &mut old_value_cache,
                                        )
                                        .unwrap_or_default();
                                    }
                                }
                                Some(row)
                            })
                        })
                        .filter_map(|v| v)
                        .collect();
                    let mut event_entries = EventEntries::default();
                    event_entries.entries = event_rows.into();
                    self.broadcast_entries(index, event_entries);
                }
            }
        }
        Ok(())
    }
}

fn decode_change_row(r: ChangeRow) -> Option<(EventRow, Option<TimeStamp>)> {
    match r {
        ChangeRow::Prewrite { key, lock, value } => {
            let mut row = EventRow::default();
            row.start_ts = lock.ts.into_inner();
            row.key = key.into_raw().unwrap();
            row.op_type = match lock.lock_type {
                LockType::Put => EventRowOpType::Put,
                LockType::Delete => EventRowOpType::Delete,
                _ => return None,
            };
            let for_update_ts = lock.for_update_ts;
            if let Some(value) = lock.short_value {
                row.value = value;
            } else if let Some(value) = value {
                row.value = value;
            }
            set_event_row_type(&mut row, EventLogType::Prewrite);
            Some((row, Some(for_update_ts)))
        }
        ChangeRow::Commit {
            key,
            write,
            commit_ts,
        } => {
            let mut row = EventRow::default();
            if write.gc_fence.is_some() {
                return None;
            }
            let (op_type, r_type) = match write.write_type {
                WriteType::Put => (EventRowOpType::Put, EventLogType::Commit),
                WriteType::Delete => (EventRowOpType::Delete, EventLogType::Commit),
                WriteType::Rollback => (EventRowOpType::Unknown, EventLogType::Rollback),
                _ => return None,
            };
            row.start_ts = write.start_ts.into_inner();
            row.commit_ts = commit_ts.unwrap_or_else(TimeStamp::zero).into_inner();
            row.key = key.into_raw().unwrap();
            row.op_type = op_type;
            set_event_row_type(&mut row, r_type);
            Some((row, None))
        }
    }
}

fn set_event_row_type(row: &mut EventRow, ty: EventLogType) {
    #[cfg(feature = "prost-codec")]
    {
        row.r#type = ty.into();
    }
    #[cfg(not(feature = "prost-codec"))]
    {
        row.r_type = ty;
    }
}

fn read_old_value<S: EngineSnapshot>(
    key: &[u8],
    start_ts: TimeStamp,
    for_update_ts: TimeStamp,
    reader: &mut OldValueReader<S>,
    old_value_cache: &mut OldValueCache,
) -> Option<Value> {
    let start = Instant::now();
    let mut statistics = Statistics::default();
    let key = Key::from_raw(key).append_ts(start_ts);
    old_value_cache.access_count += 1;
    if let Some((old_value, mutation_type)) = old_value_cache.cache.remove(&key) {
        match mutation_type {
            MutationType::Insert => {
                assert!(old_value.is_none());
                return None;
            }
            MutationType::Put | MutationType::Delete => {
                if let Some(old_value) = old_value {
                    let start_ts = old_value.start_ts;
                    return old_value.short_value.or_else(|| {
                        let prev_key = key.truncate_ts().unwrap().append_ts(start_ts);
                        let start = Instant::now();
                        let value = reader.get_value_default(&prev_key, &mut statistics);
                        CDC_OLD_VALUE_DURATION_HISTOGRAM
                            .with_label_values(&["get"])
                            .observe(start.elapsed().as_secs_f64());
                        value
                    });
                }
            }
            _ => unreachable!(),
        }
    }
    // Cannot get old value from cache, seek for it in engine.
    old_value_cache.miss_count += 1;
    let start = Instant::now();
    let query_ts = std::cmp::max(start_ts, for_update_ts);
    let key = key.truncate_ts().unwrap().append_ts(query_ts);
    let value = reader
        .near_seek_old_value(&key, &mut statistics)
        .unwrap_or_default();
    CDC_OLD_VALUE_DURATION_HISTOGRAM
        .with_label_values(&["seek"])
        .observe(start.elapsed().as_secs_f64());
    CDC_OLD_VALUE_DURATION_HISTOGRAM
        .with_label_values(&["all"])
        .observe(start.elapsed().as_secs_f64());
    value
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::executor::block_on;
    use futures::stream::StreamExt;
    use kvproto::errorpb::Error as ErrorHeader;
    use kvproto::metapb::Region;
    use std::cell::Cell;
    use tikv::storage::mvcc::test_util::*;
    use tikv_util::mpsc::batch::{self, BatchReceiver, VecCollector};

    #[test]
    fn test_error() {
        let region_id = 1;
        let mut region = Region::default();
        region.set_id(region_id);
        region.mut_peers().push(Default::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(2);
        let region_epoch = region.get_region_epoch().clone();

        let (sink, rx) = batch::unbounded(1);
        let rx = BatchReceiver::new(rx, 1, Vec::new, VecCollector);
        let request_id = 123;
        let mut downstream =
            Downstream::new(String::new(), region_epoch, request_id, ConnID::new());
        downstream.set_sink(sink);
        let mut delegate = Delegate::new(region_id);
        delegate.subscribe(downstream);
        let enabled = delegate.enabled();
        assert!(enabled.load(Ordering::SeqCst));
        let mut resolver = Resolver::new(region_id);
        resolver.init();
        for downstream in delegate.on_region_ready(resolver, region) {
            delegate.subscribe(downstream);
        }

        let rx_wrap = Cell::new(Some(rx));
        let receive_error = || {
            let (resps, rx) = block_on(rx_wrap.replace(None).unwrap().into_future());
            rx_wrap.set(Some(rx));
            let mut resps = resps.unwrap();
            assert_eq!(resps.len(), 1);
            for r in &resps {
                if let CdcEvent::Event(e) = r {
                    assert_eq!(e.get_request_id(), request_id);
                }
            }
            let cdc_event = &mut resps[0];
            if let CdcEvent::Event(e) = cdc_event {
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Error(err) => err,
                    other => panic!("unknown event {:?}", other),
                }
            } else {
                panic!("unknown event")
            }
        };

        let mut err_header = ErrorHeader::default();
        err_header.set_not_leader(Default::default());
        delegate.stop(Error::Request(err_header));
        let err = receive_error();
        assert!(err.has_not_leader());
        // Enable is disabled by any error.
        assert!(!enabled.load(Ordering::SeqCst));

        let mut err_header = ErrorHeader::default();
        err_header.set_region_not_found(Default::default());
        delegate.stop(Error::Request(err_header));
        let err = receive_error();
        assert!(err.has_region_not_found());

        let mut err_header = ErrorHeader::default();
        err_header.set_epoch_not_match(Default::default());
        delegate.stop(Error::Request(err_header));
        let err = receive_error();
        assert!(err.has_epoch_not_match());

        // Split
        let mut region = Region::default();
        region.set_id(1);
        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::Split);
        let mut response = AdminResponse::default();
        response.mut_split().set_left(region.clone());
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        err.take_epoch_not_match()
            .current_regions
            .into_iter()
            .find(|r| r.get_id() == 1)
            .unwrap();

        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::BatchSplit);
        let mut response = AdminResponse::default();
        response.mut_splits().set_regions(vec![region].into());
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        err.take_epoch_not_match()
            .current_regions
            .into_iter()
            .find(|r| r.get_id() == 1)
            .unwrap();

        // Merge
        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::PrepareMerge);
        let response = AdminResponse::default();
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_regions.is_empty());

        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::CommitMerge);
        let response = AdminResponse::default();
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_regions.is_empty());

        let mut request = AdminRequest::default();
        request.set_cmd_type(AdminCmdType::RollbackMerge);
        let response = AdminResponse::default();
        let err = delegate.sink_admin(request, response).err().unwrap();
        delegate.stop(err);
        let mut err = receive_error();
        assert!(err.has_epoch_not_match());
        assert!(err.take_epoch_not_match().current_regions.is_empty());
    }

    #[test]
    fn test_scan() {
        let region_id = 1;
        let mut region = Region::default();
        region.set_id(region_id);
        region.mut_peers().push(Default::default());
        region.mut_region_epoch().set_version(2);
        region.mut_region_epoch().set_conf_ver(2);
        let region_epoch = region.get_region_epoch().clone();

        let (sink, rx) = batch::unbounded(1);
        let rx = BatchReceiver::new(rx, 1, Vec::new, VecCollector);
        let request_id = 123;
        let mut downstream =
            Downstream::new(String::new(), region_epoch, request_id, ConnID::new());
        let downstream_id = downstream.get_id();
        downstream.set_sink(sink);
        let mut delegate = Delegate::new(region_id);
        delegate.subscribe(downstream);
        let enabled = delegate.enabled();
        assert!(enabled.load(Ordering::SeqCst));

        let rx_wrap = Cell::new(Some(rx));
        let check_event = |event_rows: Vec<EventRow>| {
            let (resps, rx) = block_on(rx_wrap.replace(None).unwrap().into_future());
            rx_wrap.set(Some(rx));
            let mut resps = resps.unwrap();
            assert_eq!(resps.len(), 1);
            for r in &resps {
                if let CdcEvent::Event(e) = r {
                    assert_eq!(e.get_request_id(), request_id);
                }
            }
            let cdc_event = resps.remove(0);
            if let CdcEvent::Event(mut e) = cdc_event {
                assert_eq!(e.region_id, region_id);
                assert_eq!(e.index, 0);
                let event = e.event.take().unwrap();
                match event {
                    Event_oneof_event::Entries(entries) => {
                        assert_eq!(entries.entries.as_slice(), event_rows.as_slice());
                    }
                    other => panic!("unknown event {:?}", other),
                }
            }
        };

        // Stashed in pending before region ready.
        let entries = vec![
            Some(
                EntryBuilder::default()
                    .key(b"a")
                    .value(b"b")
                    .start_ts(1.into())
                    .commit_ts(0.into())
                    .primary(&[])
                    .for_update_ts(0.into())
                    .build_prewrite(LockType::Put, false),
            ),
            Some(
                EntryBuilder::default()
                    .key(b"a")
                    .value(b"b")
                    .start_ts(1.into())
                    .commit_ts(2.into())
                    .primary(&[])
                    .for_update_ts(0.into())
                    .build_commit(WriteType::Put, false),
            ),
            Some(
                EntryBuilder::default()
                    .key(b"a")
                    .value(b"b")
                    .start_ts(3.into())
                    .commit_ts(0.into())
                    .primary(&[])
                    .for_update_ts(0.into())
                    .build_rollback(),
            ),
            None,
        ];
        delegate.on_scan(downstream_id, entries);
        // Flush all pending entries.
        let mut row1 = EventRow::default();
        row1.start_ts = 1;
        row1.commit_ts = 0;
        row1.key = b"a".to_vec();
        row1.op_type = EventRowOpType::Put;
        set_event_row_type(&mut row1, EventLogType::Prewrite);
        row1.value = b"b".to_vec();
        let mut row2 = EventRow::default();
        row2.start_ts = 1;
        row2.commit_ts = 2;
        row2.key = b"a".to_vec();
        row2.op_type = EventRowOpType::Put;
        set_event_row_type(&mut row2, EventLogType::Committed);
        row2.value = b"b".to_vec();
        let mut row3 = EventRow::default();
        set_event_row_type(&mut row3, EventLogType::Initialized);
        check_event(vec![row1, row2, row3]);

        let mut resolver = Resolver::new(region_id);
        resolver.init();
        delegate.on_region_ready(resolver, region);
    }
}
