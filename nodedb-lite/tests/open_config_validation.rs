// SPDX-License-Identifier: Apache-2.0

//! Integration tests for configuration rejection at open time.
//!
//! `LiteConfig::validate` exists to catch a budget split that cannot be
//! honored. Opening is where that check has to happen — a config accepted here
//! is one the database has committed to running with, so an incoherent one must
//! fail loudly rather than quietly over-allocating.

use nodedb_lite::{LiteConfig, NodeDbLite, PagedbStorageMem};

/// Engine percentages summing past the allowed total leave no headroom, so the
/// open must fail instead of handing back a database with an impossible budget.
#[tokio::test]
async fn open_rejects_incoherent_engine_percentages() {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open storage");
    let config = LiteConfig {
        hnsw_percent: 40,
        csr_percent: 25,
        loro_percent: 25,
        query_percent: 15, // sums to 105
        ..LiteConfig::default()
    };

    // `NodeDbLite` is not `Debug`, so match rather than `expect_err`.
    match NodeDbLite::open_with_config(storage, config).await {
        Ok(_) => panic!("engine percentages summing to 105% must be rejected at open"),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("percentage"),
                "the error must name the offending setting, got: {msg}"
            );
        }
    }
}

/// A single percentage over 100 is rejected on the same path.
#[tokio::test]
async fn open_rejects_percentage_over_one_hundred() {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open storage");
    let config = LiteConfig {
        hnsw_percent: 101,
        csr_percent: 0,
        loro_percent: 0,
        query_percent: 0,
        ..LiteConfig::default()
    };

    match NodeDbLite::open_with_config(storage, config).await {
        Ok(_) => panic!("hnsw_percent of 101 must be rejected at open"),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("hnsw_percent"),
                "the error must name the offending field, got: {msg}"
            );
        }
    }
}

/// A zero-capacity read cache cannot be constructed, and the field documents 1
/// as the effective minimum — so opening with 0 is an error, not a silent
/// promotion to some other value.
#[tokio::test]
async fn open_rejects_zero_kv_cache_capacity() {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open storage");
    let config = LiteConfig {
        kv_cache_capacity: 0,
        ..LiteConfig::default()
    };

    match NodeDbLite::open_with_config(storage, config).await {
        Ok(_) => panic!("kv_cache_capacity of 0 must be rejected at open"),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("kv_cache_capacity"),
                "the error must name the offending field, got: {msg}"
            );
        }
    }
}
