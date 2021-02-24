use std::sync::atomic::{AtomicBool, Ordering};

use batch_system::Fsm;
use collections::HashMap;
use crossbeam::channel::Receiver;

pub trait PollHandler<N: Fsm, C: Fsm> {
    fn begin(&mut self);
    fn handle_normal_msg(&mut self, fsm: N, msg: N::Message);
    fn handle_control_msg(&mut self, fsm: C, msg: C::Message);
    fn end(&mut self);
}

pub enum Message<N: Fsm, C: Fsm> {
    ControlMsg(C::Message),
    NormalMsg(N::Message),
    Insert(N),
}

pub struct Poller<N: Fsm, C: Fsm, Handler> {
    // router
    normals: HashMap<u64, N>,
    handler: Handler,
    max_batch_size: usize,
    msg_receiver: Receiver<Message<N, S>>,
    stopped: AtomicBool,
}

impl<N: Fsm, C: Fsm, Handler: PollHandler> Poller<N, C, Handler> {
    fn recv_batch(&mut self, batch: &mut Vec<Message<N, S>>) -> bool {
        loop {
            if let Ok(msg) = self.msg_receiver.try_recv() {
                batch.push(msg);
                if batch.size() >= self.max_batch_size {
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
        let batch = Vec::with_capacity(self.max_batch_size);

        while !self.stopped.load(Ordering::Acquire) && self.recv_batch(&mut batch) {
            self.handler.begin();
            // for msg in batch.
        }
    }
}
