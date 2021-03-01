use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use batch_system::{Config, Fsm};
use collections::HashMap;
use crossbeam::channel::{self, Receiver, Sender};
use file_system::{set_io_type, IOType};

use crate::router::Router;

pub trait PollHandler<N: Fsm, C: Fsm> {
    fn begin(&mut self);
    fn handle_normal_msg(&mut self, fsm: &mut N, msg: N::Message);
    fn handle_control_msg(&mut self, fsm: &mut C, msg: C::Message);
    fn end(&mut self, normals: &mut Vec<(u64, N)>);
}

pub enum Message<N: Fsm, C: Fsm> {
    ControlMsg(C::Message),
    NormalMsg((u64, N::Message)),
    RegisterNormal((u64, N)),
    RegisterControl(C),
    CloseNormal(u64),
    Stop,
}

pub struct Poller<N: Fsm, C: Fsm, Handler> {
    router: Router<N, C>,
    control: Option<C>,
    normals: HashMap<u64, N>,
    handler: Handler,
    max_batch_size: usize,
    msg_receiver: Receiver<Message<N, C>>,
}

impl<N: Fsm, C: Fsm, Handler: PollHandler<N, C>> Poller<N, C, Handler> {
    fn recv_batch(&mut self, batch: &mut Vec<Message<N, C>>) -> bool {
        loop {
            if let Ok(msg) = self.msg_receiver.try_recv() {
                batch.push(msg);
                if batch.len() >= self.max_batch_size {
                    return true;
                }
                continue;
            }
            if batch.is_empty() {
                if let Ok(msg) = self.msg_receiver.recv() {
                    batch.push(msg);
                    continue;
                }
            }
            break;
        }

        !batch.is_empty()
    }

    pub fn poll(&mut self) {
        let mut batch = Vec::with_capacity(self.max_batch_size);
        let mut handled_normals = Vec::with_capacity(self.max_batch_size);

        while !self.recv_batch(&mut batch) {
            self.handler.begin();
            for msg in std::mem::take(&mut batch) {
                match msg {
                    Message::ControlMsg(m) => self
                        .handler
                        .handle_control_msg(self.control.as_mut().unwrap(), m),
                    Message::NormalMsg((addr, m)) => {
                        if let Some(mut fsm) = self.normals.remove(&addr) {
                            self.handler.handle_normal_msg(&mut fsm, m);
                            handled_normals.push((addr, fsm));
                        }
                    }
                    Message::RegisterNormal((addr, fsm)) => {
                        self.normals.insert(addr, fsm);
                    }
                    Message::RegisterControl(fsm) => self.control = Some(fsm),
                    Message::CloseNormal(addr) => {
                        self.normals.remove(&addr);
                    }
                    Message::Stop => break,
                }
            }
            self.handler.end(&mut handled_normals);
            for (addr, fsm) in std::mem::take(&mut handled_normals) {
                self.normals.insert(addr, fsm);
            }
        }
    }
}

/// A builder trait that can build up poll handlers.
pub trait HandlerBuilder<N: Fsm, C: Fsm> {
    type Handler: PollHandler<N, C>;

    fn build(&mut self) -> Self::Handler;
}

/// A system that can poll FSMs concurrently and in batch.
///
/// To use the system, two type of FSMs and their PollHandlers need
/// to be defined: Normal and Control. Normal FSM handles the general
/// task while Control FSM creates normal FSM instances.
pub struct System<N: Fsm, C: Fsm> {
    name_prefix: Option<String>,
    router: Router<N, C>,
    pool_size: usize,
    max_batch_size: usize,
    workers: Vec<JoinHandle<()>>,
    senders: Vec<Sender<Message<N, C>>>,
    receivers: Vec<Receiver<Message<N, C>>>,
    control: Option<C>,
}

impl<N, C> System<N, C>
where
    N: Fsm + Send + 'static,
    C: Fsm + Send + 'static,
{
    pub fn router(&self) -> &Router<N, C> {
        &self.router
    }

    /// Start the batch system.
    pub fn spawn<B>(&mut self, name_prefix: String, mut builder: B)
    where
        B: HandlerBuilder<N, C>,
        B::Handler: Send + 'static,
    {
        for i in 0..self.pool_size {
            let handler = builder.build();
            let mut poller = Poller {
                router: self.router.clone(),
                handler,
                max_batch_size: self.max_batch_size,
                msg_receiver: self.receivers[i].clone(),
                normals: HashMap::default(),
                control: None,
            };
            let t = thread::Builder::new()
                .name(thd_name!(format!("{}-{}", name_prefix, i)))
                .spawn(move || {
                    set_io_type(IOType::ForegroundWrite);
                    poller.poll()
                })
                .unwrap();
            self.workers.push(t);
        }
        self.name_prefix = Some(name_prefix);
        let control = self.control.take().unwrap();
        self.senders[0]
            .send(Message::RegisterControl(control))
            .unwrap();
    }

    /// Shutdown the batch system and wait till all background threads exit.
    pub fn shutdown(&mut self) {
        if self.name_prefix.is_none() {
            return;
        }
        let name_prefix = self.name_prefix.take().unwrap();
        info!("shutdown batch system {}", name_prefix);
        self.router.broadcast_shutdown();
        let mut last_error = None;
        for h in self.workers.drain(..) {
            debug!("waiting for {}", h.thread().name().unwrap());
            if let Err(e) = h.join() {
                error!("failed to join worker thread: {:?}", e);
                last_error = Some(e);
            }
        }
        if let Some(e) = last_error {
            if !thread::panicking() {
                panic!("failed to join worker thread: {:?}", e);
            }
        }
        info!("batch system {} is stopped.", name_prefix);
    }
}

/// Create a batch system with the given thread name prefix and pool size.
///
/// `sender` and `controller` should be paired.
pub fn create_system<N: Fsm, C: Fsm>(cfg: &Config, control: C) -> (Router<N, C>, System<N, C>) {
    let (senders, receivers): (Vec<_>, Vec<_>) =
        (0..cfg.pool_size).map(|_| channel::unbounded()).unzip();
    let router = Router::new(senders.clone());
    let system = System {
        name_prefix: None,
        router: router.clone(),
        pool_size: cfg.pool_size,
        max_batch_size: cfg.max_batch_size,
        workers: vec![],
        senders,
        receivers,
        control: Some(control),
    };
    (router, system)
}
