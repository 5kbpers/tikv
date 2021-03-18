use std::sync::{atomic::AtomicBool, Arc, Mutex as StdMutex};
use std::{cell::Cell, sync::atomic::Ordering};

use batch_system::Fsm;
use collections::HashMap;
use crossbeam::{SendError, TrySendError};
use tikv_util::lru::LruCache;
use tikv_util::Either;
use tokio::{
    runtime::Runtime,
    sync::{Mutex, Semaphore, TryAcquireError},
};

struct FsmState<N> {
    fsm: Arc<Mutex<Box<N>>>,
    is_running: Arc<AtomicBool>,
}

impl<N> Clone for FsmState<N> {
    fn clone(&self) -> Self {
        Self {
            fsm: self.fsm.clone(),
            is_running: self.is_running.clone(),
        }
    }
}

struct Router<N: Fsm, C: Fsm> {
    control: Arc<Mutex<Box<C>>>,
    normals: Arc<StdMutex<HashMap<u64, FsmState<N>>>>,
    caches: Cell<LruCache<u64, FsmState<N>>>,
    runtime: Arc<Runtime>,
    stopped: Arc<AtomicBool>,
    semaphore: Arc<Semaphore>,
}

impl<N: Fsm, C: Fsm> Router<N, C> {
    pub fn new(
        control: Arc<Mutex<Box<C>>>,
        runtime: Arc<Runtime>,
        semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            control,
            runtime,
            semaphore,
            normals: Arc::default(),
            caches: Cell::new(LruCache::with_capacity_and_sample(1024, 7)),
            stopped: Arc::default(),
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    /// Register a mailbox with given address.
    pub fn register(&self, addr: u64, fsm: Box<N>) {
        let mut normals = self.normals.lock().unwrap();
        let caches = unsafe { &mut *self.caches.as_ptr() };
        let state = FsmState {
            fsm: Arc::new(Mutex::new(fsm)),
            is_running: Arc::default(),
        };
        normals.insert(addr, state.clone());
        caches.insert(addr, state);
    }

    pub fn register_all(&self, addrs: Vec<(u64, Box<N>)>) {
        let mut normals = self.normals.lock().unwrap();
        let caches = unsafe { &mut *self.caches.as_ptr() };
        normals.reserve(addrs.len());
        for (addr, fsm) in addrs {
            let state = FsmState {
                fsm: Arc::new(Mutex::new(fsm)),
                is_running: Arc::default(),
            };
            normals.insert(addr, state.clone());
            caches.insert(addr, state);
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
        let state = match caches.get(&addr) {
            Some(state) if state.is_running.load(Ordering::Acquire) => state.clone(),
            _ => {
                let normals = self.normals.lock().unwrap();
                match normals.get(&addr) {
                    Some(state) if state.is_running.load(Ordering::Acquire) => {
                        caches.insert(addr, state.clone());
                        state.clone()
                    }
                    _ => {
                        return Either::Right(msg);
                    }
                }
            }
        };

        let semaphore = self.semaphore.clone();
        let r = match semaphore.try_acquire_owned() {
            Ok(permit) => {
                self.runtime.spawn(async move {
                    let _permit = permit;
                });
                Ok(())
            }
            Err(TryAcquireError::Closed) => Err(TrySendError::Disconnected(msg)),
            Err(TryAcquireError::NoPermits) => Err(TrySendError::Full(msg)),
        };

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
        let caches = unsafe { &mut *self.caches.as_ptr() };
        let state = match caches.get(&addr) {
            Some(state) if state.is_running.load(Ordering::Acquire) => state.clone(),
            _ => {
                let normals = self.normals.lock().unwrap();
                match normals.get(&addr) {
                    Some(state) if state.is_running.load(Ordering::Acquire) => {
                        caches.insert(addr, state.clone());
                        state.clone()
                    }
                    _ => {
                        return Err(SendError(msg));
                    }
                }
            }
        };
        self.runtime.spawn(async move {});

        Ok(())
    }

    /// Force sending message to control fsm.
    pub fn send_control(&self, msg: C::Message) -> Result<(), TrySendError<C::Message>> {
        let control = self.control.clone();

        self.runtime.spawn(async move {});

        Ok(())
    }

    /// Try to notify all normal fsm a message.
    pub fn broadcast_normal(&self, mut msg_gen: impl FnMut() -> N::Message) {
        let normals;
        {
            normals = self.normals.lock().unwrap().clone();
        }
        normals.iter().for_each(|(addr, state)| {
            if state.is_running.load(Ordering::Acquire) {
                let _ = self.force_send(*addr, msg_gen());
            }
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
        let mut normals = self.normals.lock().unwrap();
        if let Some(state) = normals.remove(&addr) {
            state.is_running.store(false, Ordering::Release);
        }
        let caches = unsafe { &mut *self.caches.as_ptr() };
        caches.remove(&addr);
    }
}

impl<N: Fsm, C: Fsm> Clone for Router<N, C> {
    fn clone(&self) -> Router<N, C> {
        Router {
            normals: self.normals.clone(),
            caches: Cell::new(LruCache::with_capacity_and_sample(1024, 7)),
            stopped: self.stopped.clone(),
            control: self.control.clone(),
            runtime: self.runtime.clone(),
            semaphore: self.semaphore.clone(),
        }
    }
}
