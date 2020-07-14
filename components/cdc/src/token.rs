// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::box_err;
use tikv_util::mpsc::{self, Receiver, Sender};

use crate::errors::{Error, Result};

#[derive(Clone)]
pub struct Token;

#[derive(Clone)]
pub struct TokenLimiter {
    count: usize,
    rx: Receiver<Token>,
    tx: Sender<Token>,
}

impl TokenLimiter {
    pub fn new(count: usize) -> TokenLimiter {
        let (tx, rx) = mpsc::bounded(count);
        for _ in 0..count {
            tx.send(Token).unwrap();
        }
        TokenLimiter { tx, rx, count }
    }

    pub fn put(&self, token: Token) -> Result<()> {
        self.tx.send(token).map_err(|e| Error::Other(box_err!(e)))
    }

    pub fn get(&self) -> Result<Token> {
        self.rx.recv().map_err(|e| Error::Other(box_err!(e)))
    }
}
