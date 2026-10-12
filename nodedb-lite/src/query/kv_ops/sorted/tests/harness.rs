// SPDX-License-Identifier: Apache-2.0
//! Shared setup and probes for the sorted-index tests.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::KvOp;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;
use nodedb_types::{DatabaseId, Namespace, QualifiedCollection};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::kv_ops::body::encode_kv_map;
use crate::query::kv_ops::writes::kv_put;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::storage::engine::StorageEngine;
use crate::{NodeDbLite, PagedbStorageMem};

use super::super::{
    entries_prefix, kv_sorted_index_count, kv_sorted_index_rank, kv_sorted_index_top_k,
};

pub(super) type Engine = LiteQueryEngine<PagedbStorageMem>;

pub(super) async fn make_db() -> Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("in-memory storage");
    NodeDbLite::open(storage).await.expect("open db")
}

/// A map body holding `fields`.
pub(super) fn body(fields: &[(&str, Value)]) -> Vec<u8> {
    let map: HashMap<String, Value> = fields
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect();
    encode_kv_map(map).expect("encode body")
}

/// A map body `{score}`.
pub(super) fn scored(score: i64) -> Vec<u8> {
    body(&[("score", Value::Integer(score))])
}

/// A map body `{score, ts}`.
pub(super) fn stamped(score: i64, ts_ms: u64) -> Vec<u8> {
    body(&[
        ("score", Value::Integer(score)),
        ("ts", Value::Integer(ts_ms as i64)),
    ])
}

/// A `RegisterSortedIndex` plan, run through the physical visitor.
pub(super) struct Register<'a> {
    pub collection: &'a str,
    pub name: &'a str,
    pub sort: &'a [(&'a str, &'a str)],
    pub window_type: &'a str,
    pub window_start_ms: u64,
    pub window_end_ms: u64,
}

impl<'a> Register<'a> {
    /// An unwindowed index on `collection` ranking by `sort`.
    pub fn plain(collection: &'a str, name: &'a str, sort: &'a [(&'a str, &'a str)]) -> Self {
        Self {
            collection,
            name,
            sort,
            window_type: "none",
            window_start_ms: 0,
            window_end_ms: 0,
        }
    }

    /// A windowed index over the `ts` column.
    pub fn windowed(
        collection: &'a str,
        name: &'a str,
        sort: &'a [(&'a str, &'a str)],
        window_type: &'a str,
        window_start_ms: u64,
        window_end_ms: u64,
    ) -> Self {
        Self {
            collection,
            name,
            sort,
            window_type,
            window_start_ms,
            window_end_ms,
        }
    }

    pub async fn run<S: StorageEngine>(
        &self,
        engine: &LiteQueryEngine<S>,
    ) -> Result<QueryResult, LiteError> {
        let op = KvOp::RegisterSortedIndex {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, self.collection),
            index_name: self.name.to_string(),
            sort_columns: self
                .sort
                .iter()
                .map(|(c, d)| ((*c).to_string(), (*d).to_string()))
                .collect(),
            key_column: "key".to_string(),
            window_type: self.window_type.to_string(),
            window_timestamp_column: "ts".to_string(),
            window_start_ms: self.window_start_ms,
            window_end_ms: self.window_end_ms,
        };
        LiteDataPlaneVisitor::new(engine).kv(&op)?.await
    }
}

/// Register an unwindowed ascending index `lb` on `collection`.
pub(super) async fn register_asc<S: StorageEngine>(engine: &LiteQueryEngine<S>, collection: &str) {
    Register::plain(collection, "lb", &[("score", "ASC")])
        .run(engine)
        .await
        .expect("register");
}

pub(super) async fn put<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    collection: &str,
    key: &str,
    value: &[u8],
) {
    kv_put(engine, collection, key.as_bytes(), value, 0)
        .await
        .expect("put");
}

/// The keys of every visible entry of `name`, in rank order.
pub(super) async fn ranked<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    name: &str,
) -> Vec<String> {
    let result = kv_sorted_index_top_k(engine, name, u32::MAX)
        .await
        .expect("top_k");
    assert_eq!(result.columns, vec!["rank", "key"]);
    result
        .rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            assert_eq!(row[0], Value::Integer(i as i64 + 1), "ranks are dense");
            match &row[1] {
                Value::Bytes(key) => String::from_utf8(key.clone()).expect("utf-8 key"),
                other => panic!("key column is {other:?}"),
            }
        })
        .collect()
}

pub(super) async fn rank<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    name: &str,
    key: &str,
) -> Value {
    let result = kv_sorted_index_rank(engine, name, key.as_bytes())
        .await
        .expect("rank");
    assert_eq!(result.columns, vec!["rank"]);
    result.rows[0][0].clone()
}

pub(super) async fn count<S: StorageEngine>(engine: &LiteQueryEngine<S>, name: &str) -> i64 {
    let result = kv_sorted_index_count(engine, name).await.expect("count");
    assert_eq!(result.columns, vec!["count"]);
    match result.rows[0][0] {
        Value::Integer(n) => n,
        ref other => panic!("count is {other:?}"),
    }
}

/// Every stored entry key of the index `name`.
pub(super) async fn stored_entries<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    name: &str,
) -> usize {
    engine
        .storage
        .scan_prefix(Namespace::Meta, &entries_prefix(name))
        .await
        .expect("scan")
        .len()
}

/// Whether the definition of `name` is stored.
pub(super) async fn definition_stored<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    name: &str,
) -> bool {
    engine
        .storage
        .get(Namespace::Meta, &crate::index::key::sorted_def_key(name))
        .await
        .expect("get")
        .is_some()
}
