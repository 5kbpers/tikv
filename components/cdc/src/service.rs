// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::hash_map::Entry;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use futures::{Future, Sink, Stream};
use futures03::compat::Compat;
use futures03::stream::{self, StreamExt};
use grpcio::Result as GrpcResult;
use grpcio::*;
use kvproto::cdcpb::{ChangeData, ChangeDataEvent, ChangeDataRequest, Event, ResolvedTs};
use protobuf::Message;
use security::{check_common_name, SecurityManager};
use tikv_util::collections::HashMap;
use tikv_util::mpsc::batch::{self, BatchReceiver, Sender as BatchSender, VecCollector};
use tikv_util::worker::*;

use crate::delegate::{Downstream, DownstreamID};
use crate::endpoint::{Deregister, Task};

static CONNECTION_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

const CDC_MSG_NOTIFY_COUNT: usize = 8;
const CDC_MAX_RESP_SIZE: usize = 6 * 1024 * 1024; // 6MB
const CDC_MSG_MAX_BATCH_SIZE: usize = 128;

/// A unique identifier of a Connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnID(usize);

impl ConnID {
    pub fn new() -> ConnID {
        ConnID(CONNECTION_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

pub enum CdcEvent {
    ResolvedTs(ResolvedTs),
    Event(Event),
}

impl CdcEvent {
    pub fn size(&self) -> usize {
        match self {
            CdcEvent::ResolvedTs(r) => r.compute_size() as usize,
            CdcEvent::Event(ref e) => e.compute_size() as usize,
        }
    }
}

pub struct Conn {
    id: ConnID,
    sink: BatchSender<CdcEvent>,
    downstreams: HashMap<u64, DownstreamID>,
}

impl Conn {
    pub fn new(sink: BatchSender<CdcEvent>) -> Conn {
        Conn {
            id: ConnID::new(),
            sink,
            downstreams: HashMap::default(),
        }
    }

    pub fn get_id(&self) -> ConnID {
        self.id
    }

    pub fn take_downstreams(self) -> HashMap<u64, DownstreamID> {
        self.downstreams
    }

    pub fn get_sink(&self) -> BatchSender<CdcEvent> {
        self.sink.clone()
    }

    pub fn subscribe(&mut self, region_id: u64, downstream_id: DownstreamID) -> bool {
        match self.downstreams.entry(region_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert(downstream_id);
                true
            }
        }
    }

    pub fn unsubscribe(&mut self, region_id: u64) {
        self.downstreams.remove(&region_id);
    }

    pub fn downstream_id(&self, region_id: u64) -> Option<DownstreamID> {
        self.downstreams.get(&region_id).copied()
    }

    pub fn flush(&self) {
        if !self.sink.is_empty() {
            if let Some(notifier) = self.sink.get_notifier() {
                notifier.notify();
            }
        }
    }
}

/// Service implements the `ChangeData` service.
///
/// It's a front-end of the CDC service, schedules requests to the `Endpoint`.
#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Task>,
    security_mgr: Arc<SecurityManager>,
}

impl Service {
    /// Create a ChangeData service.
    ///
    /// It requires a scheduler of an `Endpoint` in order to schedule tasks.
    pub fn new(scheduler: Scheduler<Task>, security_mgr: Arc<SecurityManager>) -> Service {
        Service {
            scheduler,
            security_mgr,
        }
    }
}

impl ChangeData for Service {
    fn event_feed(
        &mut self,
        ctx: RpcContext,
        stream: RequestStream<ChangeDataRequest>,
        sink: DuplexSink<ChangeDataEvent>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        // TODO: make it a bounded channel.
        let (tx, rx) = batch::unbounded(CDC_MSG_NOTIFY_COUNT);
        let conn = Conn::new(tx);
        let conn_id = conn.get_id();

        if let Err(status) = self
            .scheduler
            .schedule(Task::OpenConn { conn })
            .map_err(|e| RpcStatus::new(RpcStatusCode::INVALID_ARGUMENT, Some(format!("{:?}", e))))
        {
            error!("cdc connection initiate failed"; "error" => ?status);
            ctx.spawn(sink.fail(status).map_err(|e| {
                error!("cdc failed to send error"; "error" => ?e);
            }));
            return;
        }

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();
        let recv_req = stream.for_each(move |request| {
            let region_epoch = request.get_region_epoch().clone();
            let req_id = request.get_request_id();
            let downstream = Downstream::new(peer.clone(), region_epoch, req_id, conn_id);
            scheduler
                .schedule(Task::Register {
                    request,
                    downstream,
                    conn_id,
                })
                .map_err(|e| {
                    Error::RpcFailure(RpcStatus::new(
                        RpcStatusCode::INVALID_ARGUMENT,
                        Some(format!("{:?}", e)),
                    ))
                })
        });

        let rx = BatchReceiver::new(rx, CDC_MSG_MAX_BATCH_SIZE, Vec::new, VecCollector);
        let rx = rx
            .map(|events| {
                // The size of the response should not exceed CDC_MAX_RESP_SIZE.
                // Split the events into multiple responses by CDC_MAX_RESP_SIZE here.
                let events_len = events.len();
                let mut resp_vecs = Vec::with_capacity(events_len);
                resp_vecs.push(ChangeDataEvent::default());
                let mut current_events_size = 0;
                for event in events {
                    if current_events_size >= CDC_MAX_RESP_SIZE {
                        resp_vecs.push(ChangeDataEvent::default());
                        current_events_size = 0;
                    }
                    let event_size = event.size();
                    match event {
                        CdcEvent::Event(e) => {
                            resp_vecs.last_mut().unwrap().mut_events().push(e);
                            current_events_size += event_size;
                        }
                        CdcEvent::ResolvedTs(r) => {
                            // Set resolved ts as an individual event.
                            let events = resp_vecs.last_mut().unwrap().take_events();
                            let mut change_data_event = ChangeDataEvent::default();
                            change_data_event.set_events(events);

                            resp_vecs.last_mut().unwrap().set_resolved_ts(r);
                            resp_vecs.push(change_data_event);
                        }
                    }
                }
                let resps = resp_vecs
                    .into_iter()
                    .filter(|e| e.has_resolved_ts() || e.events.len() > 0)
                    .map(|resp| (resp, WriteFlags::default()));
                stream::iter(resps)
            })
            .flatten()
            .map(|item| GrpcResult::Ok(item));
        let send_resp = sink.send_all(Compat::new(rx));

        let scheduler = self.scheduler.clone();
        ctx.spawn(recv_req.then(move |res| {
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e);
            }
            match res {
                Ok(_s) => {
                    info!("cdc send half closed");
                }
                Err(e) => {
                    error!("cdc send failed"; "error" => ?e);
                }
            }
            Ok(())
        }));

        let scheduler = self.scheduler.clone();
        ctx.spawn(send_resp.then(move |res| {
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e);
            }
            match res {
                Ok(_s) => {
                    info!("cdc send half closed");
                }
                Err(e) => {
                    error!("cdc send failed"; "error" => ?e);
                }
            }
            Ok(())
        }));
    }
}
