// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use crate::engine::SkiplistEngine;
use engine_traits::{DeleteStrategy, MiscExt, Range, Result, SyncMutable};
use std::sync::atomic::Ordering;

impl MiscExt for SkiplistEngine {
    fn flush(&self, sync: bool) -> Result<()> {
        Ok(())
    }

    fn flush_cf(&self, cf: &str, sync: bool) -> Result<()> {
        Ok(())
    }

    fn delete_ranges_cf(&self, cf: &str, strategy: DeleteStrategy, ranges: &[Range]) -> Result<()> {
        for r in ranges {
            self.delete_range_cf(cf, r.start_key, r.end_key)?;
        }
        Ok(())
    }

    fn get_approximate_memtable_stats_cf(&self, cf: &str, range: &Range) -> Result<(u64, u64)> {
        Ok((0, 0))
    }

    fn ingest_maybe_slowdown_writes(&self, cf: &str) -> Result<bool> {
        Ok(false)
    }

    fn get_engine_used_size(&self) -> Result<u64> {
        Ok(self.total_bytes.load(Ordering::Relaxed) as u64)
    }

    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let ranges: Vec<Range> = ranges
            .iter()
            .map(|(start_key, end_key)| Range { start_key, end_key })
            .collect();
        self.delete_all_in_range(DeleteStrategy::DeleteByRange, &ranges)?;
        Ok(())
    }

    fn path(&self) -> &str {
        ""
    }

    fn sync_wal(&self) -> Result<()> {
        Ok(())
    }

    fn exists(path: &str) -> bool {
        true
    }

    fn dump_stats(&self) -> Result<String> {
        Ok("".to_owned())
    }

    fn get_latest_sequence_number(&self) -> u64 {
        std::u64::MIN
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        Some(self.get_latest_sequence_number() + 1)
    }
}
