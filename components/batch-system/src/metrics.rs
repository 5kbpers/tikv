// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref BATCH_SYSTEM_BATCH_SIZE: HistogramVec = register_histogram_vec!(
        "tikv_batch_system_batch_size",
        "Bucketed histogram of batch system batch size",
        &["tag"],
        exponential_buckets(1.0, 2.0, 24).unwrap()
    )
    .unwrap();
}
