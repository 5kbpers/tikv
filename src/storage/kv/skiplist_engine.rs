use std::default::Default;
use std::fmt::{self, Debug, Display, Formatter};
use std::ops::Deref;

use engine_skiplist::{
    SkiplistEngine as SkiplistDb, SkiplistEngineBuilder as SkiplistDbBuilder,
    SkiplistEngineIterator as SkiplistDbIterator, SkiplistSnapshot,
};
use engine_traits::{
    CfName, IterOptions, Iterable, Iterator as _, KvEngine, Peekable, ReadOptions, SeekKey,
    SyncMutable, ALL_CFS, CF_DEFAULT,
};
use kvproto::kvrpcpb::Context;
use txn_types::{Key, Value};

use crate::storage::kv::{
    Callback as EngineCallback, CbContext, Cursor, Engine, Error as EngineError,
    ErrorInner as EngineErrorInner, Iterator, Modify, Result as EngineResult, ScanMode, Snapshot,
    WriteData,
};
use tikv_util::time::ThreadReadId;

#[derive(Clone)]
pub struct SkiplistEngine {
    engine: SkiplistDb,
}

impl SkiplistEngine {
    pub fn new(cfs: &[CfName]) -> Self {
        let engine = SkiplistDbBuilder::new("skiplist").cf_names(cfs).build();

        Self { engine }
    }
}

impl Default for SkiplistEngine {
    fn default() -> Self {
        Self::new(ALL_CFS)
    }
}

impl Engine for SkiplistEngine {
    type Local = SkiplistDb;
    type Snap = SkiplistEngineSnapshot;

    fn kv_engine(&self) -> SkiplistDb {
        self.engine.clone()
    }

    fn snapshot_on_kv_engine(&self, _: &[u8], _: &[u8]) -> EngineResult<Self::Snap> {
        unimplemented!()
    }

    fn modify_on_kv_engine(&self, modifies: Vec<Modify>) -> EngineResult<()> {
        write_modifies(self, modifies)
    }

    fn async_write(
        &self,
        _ctx: &Context,
        batch: WriteData,
        cb: EngineCallback<()>,
    ) -> EngineResult<()> {
        if batch.modifies.is_empty() {
            return Err(EngineError::from(EngineErrorInner::EmptyRequest));
        }
        cb((CbContext::new(), write_modifies(&self, batch.modifies)));

        Ok(())
    }

    /// warning: It returns a fake snapshot whose content will be affected by the later modifies!
    fn async_snapshot(
        &self,
        _ctx: &Context,
        _: Option<ThreadReadId>,
        cb: EngineCallback<Self::Snap>,
    ) -> EngineResult<()> {
        cb((
            CbContext::new(),
            Ok(SkiplistEngineSnapshot::new(self.clone())),
        ));
        Ok(())
    }
}

impl Display for SkiplistEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SkiplistEngine",)
    }
}

impl Debug for SkiplistEngine {
    // TODO: Provide more debug info.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SkiplistEngine",)
    }
}

pub struct SkiplistEngineIterator {
    iter: SkiplistDbIterator,
}

impl Iterator for SkiplistEngineIterator {
    fn next(&mut self) -> EngineResult<bool> {
        Ok(self.iter.next()?)
    }

    fn prev(&mut self) -> EngineResult<bool> {
        Ok(self.iter.prev()?)
    }

    fn seek(&mut self, key: &Key) -> EngineResult<bool> {
        Ok(self.iter.seek(SeekKey::Key(key.as_encoded()))?)
    }

    fn seek_for_prev(&mut self, key: &Key) -> EngineResult<bool> {
        Ok(self.iter.seek_for_prev(SeekKey::Key(key.as_encoded()))?)
    }

    fn seek_to_first(&mut self) -> EngineResult<bool> {
        Ok(self.iter.seek(SeekKey::Start)?)
    }

    fn seek_to_last(&mut self) -> EngineResult<bool> {
        Ok(self.iter.seek(SeekKey::End)?)
    }

    #[inline]
    fn valid(&self) -> EngineResult<bool> {
        Ok(self.iter.valid()?)
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }
}

#[derive(Debug, Clone)]
pub struct SkiplistEngineSnapshot {
    snap: SkiplistSnapshot,
}

impl SkiplistEngineSnapshot {
    pub fn new(e: SkiplistEngine) -> Self {
        Self {
            snap: e.engine.snapshot(),
        }
    }
}

impl Snapshot for SkiplistEngineSnapshot {
    type Iter = SkiplistEngineIterator;

    fn get(&self, key: &Key) -> EngineResult<Option<Value>> {
        self.get_cf(CF_DEFAULT, key)
    }
    fn get_cf(&self, cf: CfName, key: &Key) -> EngineResult<Option<Value>> {
        Ok(self
            .snap
            .get_value_cf(cf, key.as_encoded())?
            .map(|v| v.deref().to_vec()))
    }
    fn get_cf_opt(&self, _: ReadOptions, cf: CfName, key: &Key) -> EngineResult<Option<Value>> {
        self.get_cf(cf, key)
    }
    fn iter(&self, iter_opt: IterOptions, mode: ScanMode) -> EngineResult<Cursor<Self::Iter>> {
        self.iter_cf(CF_DEFAULT, iter_opt, mode)
    }
    #[inline]
    fn iter_cf(
        &self,
        cf: CfName,
        opts: IterOptions,
        mode: ScanMode,
    ) -> EngineResult<Cursor<Self::Iter>> {
        let iter = SkiplistEngineIterator {
            iter: self.snap.iterator_cf_opt(cf, opts)?,
        };
        Ok(Cursor::new(iter, mode))
    }
}

fn write_modifies(e: &SkiplistEngine, modifies: Vec<Modify>) -> EngineResult<()> {
    for rev in modifies {
        match rev {
            Modify::Delete(cf, k) => {
                e.engine.delete_cf(cf, k.as_encoded())?;
            }
            Modify::Put(cf, k, v) => {
                e.engine.put_cf(cf, k.as_encoded(), v.as_slice())?;
            }
            Modify::DeleteRange(cf, start_key, end_key, _notify_only) => {
                e.engine
                    .delete_range_cf(cf, start_key.as_encoded(), end_key.as_encoded())?;
            }
        };
    }
    Ok(())
}
