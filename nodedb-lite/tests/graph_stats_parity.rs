// SPDX-License-Identifier: Apache-2.0

//! Integration tests for `NodeDb::graph_stats` on the Lite backend.
//!
//! Verifies per-collection and tenant-wide call shapes, correct count
//! aggregation, and that `as_of` is rejected with the expected error.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, RedbStorage};
use nodedb_types::id::NodeId;

async fn open_test_db() -> NodeDbLite<RedbStorage> {
    let storage = RedbStorage::open_in_memory().unwrap();
    NodeDbLite::open(storage, 1).await.unwrap()
}

const N: usize = 10;
const K: usize = 3;

/// Insert N=10 edges across K=3 labels and return the opened db.
async fn db_with_edges() -> NodeDbLite<RedbStorage> {
    let db = open_test_db().await;
    let labels = ["KNOWS", "OWNS", "FOLLOWS"];
    for i in 0..N {
        let from = NodeId::try_new(format!("n{i}")).expect("test fixture");
        let to = NodeId::try_new(format!("n{}", i + 1)).expect("test fixture");
        let label = labels[i % K];
        db.graph_insert_edge("col", &from, &to, label, None)
            .await
            .expect("insert_edge");
    }
    db
}

#[tokio::test]
async fn graph_stats_per_collection_returns_single_entry() {
    let db = db_with_edges().await;
    let result = db.graph_stats(Some("any-name"), None).await.unwrap();
    assert_eq!(result.len(), 1, "expected exactly one entry");
    let stats = &result[0];
    assert_eq!(stats.collection, "any-name");
    assert_eq!(stats.edge_count, N as u64);
    assert_eq!(stats.distinct_label_count, K as u64);
}

#[tokio::test]
async fn graph_stats_tenant_wide_returns_single_entry() {
    let db = db_with_edges().await;
    let result = db.graph_stats(None, None).await.unwrap();
    assert_eq!(result.len(), 1, "expected exactly one entry");
    let stats = &result[0];
    assert_eq!(stats.edge_count, N as u64);
    assert_eq!(stats.distinct_label_count, K as u64);
}

#[tokio::test]
async fn graph_stats_as_of_returns_storage_error() {
    let db = open_test_db().await;
    let err = db.graph_stats(None, Some(1234)).await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("AS OF SYSTEM TIME is not supported on the Lite backend"),
        "unexpected error message: {msg}",
    );
}
