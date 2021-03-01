use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use batch_system::Fsm;
use collections::HashSet;
use crossbeam::{SendError, Sender, TrySendError};
use tikv_util::lru::LruCache;
use tikv_util::Either;

use crate::poll::Message;

pub struct Router<N: Fsm, C: Fsm> {
    normals: Arc<Mutex<HashSet<u64>>>,
    caches: Cell<LruCache<u64, ()>>,
    senders: Vec<Sender<Message<N, C>>>,
    stopped: Arc<AtomicBool>,
}

impl<N: Fsm, C: Fsm> Router<N, C> {
    pub fn new(senders: Vec<Sender<Message<N, C>>>) -> Self {
        Self {
            senders,
            normals: Arc::new(Mutex::new(HashSet::default())),
            caches: Cell::new(LruCache::with_capacity_and_sample(1024, 7)),
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    fn shard_id(&self, addr: u64) -> usize {
        fxhash::hash(&addr) % self.senders.len()
    }

    /// Register a mailbox with given address.
    pub fn register(&self, addr: u64, fsm: N) {
        let mut normals = self.normals.lock().unwrap();
        let caches = unsafe { &mut *self.caches.as_ptr() };
        normals.insert(addr);
        caches.insert(addr, ());
        let shard_id = self.shard_id(addr);
        if self.senders[shard_id]
            .send(Message::RegisterNormal((addr, fsm)))
            .is_err()
        {
            assert!(self.is_shutdown());
        }
    }

    pub fn register_all(&self, addrs: Vec<(u64, N)>) {
        let mut normals = self.normals.lock().unwrap();
        let caches = unsafe { &mut *self.caches.as_ptr() };
        normals.reserve(addrs.len());
        for (addr, fsm) in addrs {
            normals.insert(addr);
            caches.insert(addr, ());
            let shard_id = self.shard_id(addr);
            if self.senders[shard_id]
                .send(Message::RegisterNormal((addr, fsm)))
                .is_err()
            {
                assert!(self.is_shutdown());
            }
        }
    }

    /// Try to send a message to specified address.
    ///
    /// If Either::Left is returned, then the message is sent.
    #[inline]
    pub fn try_send(
        &self,
        addr: u64,
        msg: N::Message,
    ) -> Either<Result<(), TrySendError<N::Message>>, N::Message> {
        let caches = unsafe { &mut *self.caches.as_ptr() };
        if caches.get(&addr).is_none() {
            let normals = self.normals.lock().unwrap();
            if normals.get(&addr).is_none() {
                return Either::Right(msg);
            }
            caches.insert(addr, ());
        }

        let shard_id = self.shard_id(addr);
        let r = self.senders[shard_id]
            .try_send(Message::NormalMsg((addr, msg)))
            .map_err(|e| match e {
                TrySendError::Full(Message::NormalMsg((_, m))) => TrySendError::Full(m),
                TrySendError::Disconnected(Message::NormalMsg((_, m))) => {
                    TrySendError::Disconnected(m)
                }
                _ => panic!(""),
            });
        Either::Left(r)
    }

    /// Send the message to specified address.
    #[inline]
    pub fn send(&self, addr: u64, msg: N::Message) -> Result<(), TrySendError<N::Message>> {
        match self.try_send(addr, msg) {
            Either::Left(res) => res,
            Either::Right(m) => Err(TrySendError::Disconnected(m)),
        }
    }

    /// Force sending message to specified address despite the capacity
    /// limit of mailbox.
    #[inline]
    pub fn force_send(&self, addr: u64, msg: N::Message) -> Result<(), SendError<N::Message>> {
        match self.send(addr, msg) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(m)) => {
                let shard_id = self.shard_id(addr);
                self.senders[shard_id]
                    .send(Message::NormalMsg((addr, m)))
                    .map_err(|e| match e {
                        SendError(Message::NormalMsg((_, m))) => SendError(m),
                        _ => panic!(""),
                    })
            }
            Err(TrySendError::Disconnected(m)) => {
                if self.is_shutdown() {
                    Ok(())
                } else {
                    Err(SendError(m))
                }
            }
        }
    }

    /// Force sending message to control fsm.
    pub fn send_control(&self, msg: C::Message) -> Result<(), TrySendError<C::Message>> {
        match self.senders[0].try_send(Message::ControlMsg(msg)) {
            Ok(()) => Ok(()),
            Err(TrySendError::Full(Message::ControlMsg(msg))) => Err(TrySendError::Full(msg)),
            Err(TrySendError::Disconnected(Message::ControlMsg(msg))) => {
                Err(TrySendError::Disconnected(msg))
            }
            _ => panic!(""),
        }
    }

    /// Try to notify all normal fsm a message.
    pub fn broadcast_normal(&self, mut msg_gen: impl FnMut() -> N::Message) {
        let normals = self.normals.lock().unwrap();
        normals.iter().for_each(|addr| {
            let _ = self.force_send(*addr, msg_gen());
        })
    }

    /// Try to notify all fsm that the cluster is being shutdown.
    pub fn broadcast_shutdown(&self) {
        info!("broadcasting shutdown");
        self.stopped.store(true, Ordering::SeqCst);
    }

    /// Close the mailbox of address.
    pub fn close(&self, addr: u64) {
        info!("[region {}] shutdown mailbox", addr);
        let shard_id = self.shard_id(addr);
        if self.senders[shard_id]
            .send(Message::CloseNormal(addr))
            .is_err()
        {
            assert!(self.is_shutdown());
        }
    }
}

impl<N: Fsm, C: Fsm> Clone for Router<N, C> {
    fn clone(&self) -> Router<N, C> {
        Router {
            normals: self.normals.clone(),
            caches: Cell::new(LruCache::with_capacity_and_sample(1024, 7)),
            senders: self.senders.clone(),
            stopped: self.stopped.clone(),
        }
    }
}
