// SPDX-License-Identifier: Apache-2.0
//! Sorted-index lifecycle: register validation and replacement, drop,
//! truncate, collection drop, and reopen.

use std::sync::Arc;

use nodedb_types::value::Value;

use crate::config::LiteConfig;
use crate::error::LiteError;
use crate::query::kv_ops::indexes::kv_register_index;
use crate::query::kv_ops::reads::kv_get;
use crate::query::kv_ops::writes::kv_truncate;
use crate::storage::encryption::Encryption;
use crate::{NodeDbLite, PagedbStorageDefault};

use super::super::{kv_drop_sorted_index, kv_sorted_index_top_k};
use super::harness::{
    Register, definition_stored, make_db, put, rank, ranked, register_asc, scored, stored_entries,
};

#[tokio::test]
async fn register_replaces_an_index_of_the_same_name() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(engine, "players", "p", &scored(1)).await;
    put(engine, "teams", "x", &scored(1)).await;
    put(engine, "teams", "y", &scored(2)).await;

    Register::plain("teams", "lb", &[("score", "DESC")])
        .run(engine)
        .await
        .expect("re-register");

    assert_eq!(ranked(engine, "lb").await, ["y", "x"]);
    put(engine, "players", "q", &scored(5)).await;
    assert_eq!(rank(engine, "lb", "q").await, Value::Null);
    assert_eq!(stored_entries(engine, "lb").await, 4);
}

#[tokio::test]
async fn register_refuses_a_window_column_missing_from_the_sort_columns() {
    let db = make_db().await;
    let err = Register::windowed("players", "lb", &[("score", "ASC")], "daily", 0, 0)
        .run(&db.query_engine)
        .await
        .expect_err("refused");
    assert!(matches!(err, LiteError::BadRequest { .. }), "{err:?}");
}

#[tokio::test]
async fn register_refuses_an_unknown_window_type() {
    let db = make_db().await;
    let err = Register::windowed(
        "players",
        "lb",
        &[("score", "ASC"), ("ts", "ASC")],
        "tumbling",
        0,
        0,
    )
    .run(&db.query_engine)
    .await
    .expect_err("refused");
    assert!(matches!(err, LiteError::BadRequest { .. }), "{err:?}");
}

#[tokio::test]
async fn register_refuses_a_sort_column_beyond_score_and_window() {
    let db = make_db().await;
    let err = Register::plain("players", "lb", &[("score", "ASC"), ("name", "ASC")])
        .run(&db.query_engine)
        .await
        .expect_err("refused");
    assert!(
        matches!(&err, LiteError::Unsupported { detail } if detail.contains("'name'")),
        "{err:?}"
    );
    assert!(db.query_engine.indexes.sorted_named("lb").is_none());
}

#[tokio::test]
async fn drop_removes_the_entries_and_the_definition() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(engine, "players", "a", &scored(1)).await;
    assert!(definition_stored(engine, "lb").await);

    let result = kv_drop_sorted_index(engine, "lb").await.expect("drop");

    assert_eq!(result.rows, vec![vec![Value::String("lb".into())]]);
    assert_eq!(stored_entries(engine, "lb").await, 0);
    assert!(!definition_stored(engine, "lb").await);
    assert!(engine.indexes.sorted_named("lb").is_none());
    put(engine, "players", "b", &scored(2)).await;
    assert_eq!(stored_entries(engine, "lb").await, 0);
}

#[tokio::test]
async fn dropping_an_unknown_index_is_not_found() {
    let db = make_db().await;
    let err = kv_drop_sorted_index(&db.query_engine, "ghost")
        .await
        .expect_err("refused");
    assert!(matches!(err, LiteError::UndefinedObject { .. }), "{err:?}");
}

#[tokio::test]
async fn truncate_clears_the_entries_and_drops_the_registration() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    kv_register_index(engine, "players", "score", true)
        .await
        .expect("secondary index");
    put(engine, "players", "a", &scored(1)).await;
    put(engine, "players", "b", &scored(2)).await;

    kv_truncate(engine, "players").await.expect("truncate");

    assert_eq!(stored_entries(engine, "lb").await, 0);
    assert!(!definition_stored(engine, "lb").await);
    let err = kv_sorted_index_top_k(engine, "lb", 10)
        .await
        .expect_err("registration dropped");
    assert!(matches!(err, LiteError::UndefinedObject { .. }), "{err:?}");
    assert!(
        engine
            .indexes
            .def_named(&crate::index::default_index_name("players", "score"))
            .is_some(),
        "truncate keeps the secondary index"
    );
}

#[tokio::test]
async fn dropping_a_kv_collection_removes_its_rows_and_sorted_index() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(engine, "players", "a", &scored(1)).await;
    put(engine, "others", "o", &scored(1)).await;

    engine
        .handle_drop_document("players")
        .await
        .expect("drop collection");

    let row = kv_get(engine, "players", b"a", None).await.expect("get");
    assert!(row.rows.is_empty(), "the row survives the drop");
    assert_eq!(stored_entries(engine, "lb").await, 0);
    assert!(!definition_stored(engine, "lb").await);
    assert!(engine.indexes.sorted_named("lb").is_none());
    let other = kv_get(engine, "others", b"o", None).await.expect("get");
    assert_eq!(other.rows.len(), 1, "another collection is untouched");
}

#[tokio::test]
async fn the_public_collection_drop_removes_buffered_and_stored_kv_rows() {
    let db = make_db().await;
    db.kv_put("players", "stored", &scored(1))
        .await
        .expect("put");
    db.kv_flush().await.expect("flush");
    db.kv_put("players", "buffered", &scored(2))
        .await
        .expect("put");

    db.drop_collection("players").await.expect("drop");
    db.kv_flush().await.expect("flush");

    let engine = &db.query_engine;
    for key in [&b"stored"[..], b"buffered"] {
        let row = kv_get(engine, "players", key, None).await.expect("get");
        assert!(row.rows.is_empty(), "{key:?} survives the drop");
    }
}

/// Open the store at `path` with automatic flushes off.
async fn open_at(path: &std::path::Path) -> Arc<NodeDbLite<PagedbStorageDefault>> {
    let storage = PagedbStorageDefault::open(path, Encryption::Plaintext)
        .await
        .expect("open storage");
    let config = LiteConfig {
        auto_flush_ms: 0,
        ..LiteConfig::default()
    };
    NodeDbLite::open_with_config(storage, config)
        .await
        .expect("open db")
}

#[tokio::test]
async fn a_sorted_index_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("sorted.pagedb");

    {
        let db = open_at(&path).await;
        let engine = &db.query_engine;
        Register::plain("players", "lb", &[("score", "DESC")])
            .run(engine)
            .await
            .expect("register");
        put(engine, "players", "a", &scored(1)).await;
        put(engine, "players", "b", &scored(2)).await;
        db.shutdown().await;
    }

    let db = open_at(&path).await;
    let engine = &db.query_engine;
    let def = engine
        .indexes
        .sorted_named("lb")
        .expect("definition loaded");
    assert_eq!(def.collection, "players");
    assert!(def.score_desc);
    assert_eq!(stored_entries(engine, "lb").await, 4);
    assert_eq!(rank(engine, "lb", "b").await, Value::Integer(1));
    put(engine, "players", "c", &scored(3)).await;
    assert_eq!(ranked(engine, "lb").await, ["c", "b", "a"]);
}
