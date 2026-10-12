// SPDX-License-Identifier: Apache-2.0
//! Every KV write path keeps the sorted index in step with the rows.

use std::time::Duration;

use nodedb_physical::physical_plan::KvCounterShape;
use nodedb_query::msgpack_scan::kv_row_msgpack;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::index::IndexEngine;
use crate::query::document_ops::indexes::{CreateIndexRequest, declare_index};
use crate::query::kv_ops::writes::{
    kv_batch_put, kv_delete, kv_expire, kv_field_set, kv_incr, kv_persist, kv_put, kv_transfer,
};

use super::harness::{Register, body, count, make_db, put, rank, ranked, register_asc, scored};

#[tokio::test]
async fn register_backfills_rows_written_before_it() {
    let db = make_db().await;
    let engine = &db.query_engine;
    put(engine, "players", "a", &scored(30)).await;
    put(engine, "players", "b", &scored(10)).await;
    put(engine, "players", "c", &scored(20)).await;

    let result = Register::plain("players", "lb", &[("score", "ASC")])
        .run(engine)
        .await
        .expect("register");

    assert_eq!(result.columns, vec!["index", "backfilled"]);
    assert_eq!(
        result.rows,
        vec![vec![Value::String("lb".into()), Value::Integer(3)]]
    );
    assert_eq!(ranked(engine, "lb").await, ["b", "c", "a"]);
}

#[tokio::test]
async fn the_plan_collection_is_the_one_ranked() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;

    put(engine, "players", "p", &scored(1)).await;
    put(engine, "others", "o", &scored(2)).await;

    assert_eq!(ranked(engine, "lb").await, ["p"]);
    assert_eq!(rank(engine, "lb", "o").await, Value::Null);
}

#[tokio::test]
async fn a_put_after_register_is_ranked() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(engine, "players", "a", &scored(5)).await;
    put(engine, "players", "b", &scored(2)).await;

    assert_eq!(rank(engine, "lb", "a").await, Value::Integer(2));
    assert_eq!(rank(engine, "lb", "b").await, Value::Integer(1));
}

#[tokio::test]
async fn an_update_moves_the_entry_and_keeps_the_count() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(engine, "players", "a", &scored(1)).await;
    put(engine, "players", "b", &scored(2)).await;

    put(engine, "players", "a", &scored(3)).await;

    assert_eq!(ranked(engine, "lb").await, ["b", "a"]);
    assert_eq!(count(engine, "lb").await, 2);
}

#[tokio::test]
async fn a_delete_removes_the_entry_and_shifts_ranks() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    for (key, score) in [("a", 1), ("b", 2), ("c", 3)] {
        put(engine, "players", key, &scored(score)).await;
    }

    kv_delete(engine, "players", &[b"a".to_vec()])
        .await
        .expect("delete");

    assert_eq!(rank(engine, "lb", "a").await, Value::Null);
    assert_eq!(rank(engine, "lb", "b").await, Value::Integer(1));
    assert_eq!(count(engine, "lb").await, 2);
}

#[tokio::test]
async fn an_increment_reorders_the_row() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(engine, "players", "a", &scored(1)).await;
    put(engine, "players", "b", &scored(5)).await;

    kv_incr(engine, "players", b"a", 10, 0, &KvCounterShape::Raw)
        .await
        .expect("incr");

    assert_eq!(ranked(engine, "lb").await, ["b", "a"]);
}

#[tokio::test]
async fn a_field_set_reorders_the_row() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(engine, "players", "a", &scored(1)).await;
    put(engine, "players", "b", &scored(5)).await;

    let update = nodedb_types::value_to_msgpack(&Value::Integer(9)).expect("encode");
    kv_field_set(
        engine,
        "players",
        b"a",
        &[("score".to_string(), update)],
        true,
    )
    .await
    .expect("field set");

    assert_eq!(ranked(engine, "lb").await, ["b", "a"]);
}

#[tokio::test]
async fn a_transfer_rescores_both_rows() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(engine, "players", "a", &scored(10)).await;
    put(engine, "players", "b", &scored(4)).await;

    kv_transfer(engine, "players", b"a", b"b", "score", 8.0)
        .await
        .expect("transfer");

    assert_eq!(ranked(engine, "lb").await, ["a", "b"]);
    let score = super::super::kv_sorted_index_score(engine, "lb", b"b")
        .await
        .expect("score");
    assert_eq!(score.rows, vec![vec![Value::Float(12.0)]]);
}

#[tokio::test]
async fn a_batch_put_indexes_every_row() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;

    let rows = vec![
        (b"a".to_vec(), scored(3)),
        (b"b".to_vec(), scored(1)),
        (b"c".to_vec(), scored(2)),
    ];
    kv_batch_put(engine, "players", &rows, 0)
        .await
        .expect("batch put");

    assert_eq!(ranked(engine, "lb").await, ["b", "c", "a"]);
}

#[tokio::test]
async fn a_row_written_twice_in_one_batch_holds_one_entry() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(engine, "players", "a", &scored(7)).await;

    let rows = vec![(b"a".to_vec(), scored(1)), (b"a".to_vec(), scored(5))];
    kv_batch_put(engine, "players", &rows, 0)
        .await
        .expect("batch put");

    assert_eq!(ranked(engine, "lb").await, ["a"]);
    assert_eq!(super::harness::stored_entries(engine, "lb").await, 2);
    let score = super::super::kv_sorted_index_score(engine, "lb", b"a")
        .await
        .expect("score");
    assert_eq!(score.rows, vec![vec![Value::Float(5.0)]]);
}

#[tokio::test]
async fn a_raw_body_row_is_not_ranked() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(engine, "players", "raw", b"12").await;
    put(engine, "players", "a", &scored(1)).await;

    assert_eq!(ranked(engine, "lb").await, ["a"]);
}

#[tokio::test]
async fn a_row_missing_the_score_column_is_not_ranked() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(
        engine,
        "players",
        "x",
        &body(&[("other", Value::Integer(1))]),
    )
    .await;
    put(
        engine,
        "players",
        "y",
        &body(&[("score", Value::String("high".into()))]),
    )
    .await;
    put(engine, "players", "a", &scored(1)).await;

    assert_eq!(ranked(engine, "lb").await, ["a"]);
}

#[tokio::test]
async fn an_expired_row_is_not_visible() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    kv_put(engine, "players", b"gone", &scored(1), 1)
        .await
        .expect("put with ttl");
    put(engine, "players", "kept", &scored(2)).await;

    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(ranked(engine, "lb").await, ["kept"]);
    assert_eq!(rank(engine, "lb", "gone").await, Value::Null);
    let score = super::super::kv_sorted_index_score(engine, "lb", b"gone")
        .await
        .expect("score");
    assert_eq!(score.rows, vec![vec![Value::Null]]);
}

#[tokio::test]
async fn expire_moves_the_entry_deadline() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    put(engine, "players", "a", &scored(1)).await;

    kv_expire(engine, "players", b"a", 1).await.expect("expire");
    tokio::time::sleep(Duration::from_millis(20)).await;

    assert_eq!(rank(engine, "lb", "a").await, Value::Null);
    assert_eq!(count(engine, "lb").await, 0);
}

#[tokio::test]
async fn persist_keeps_the_row_visible() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    kv_put(engine, "players", b"a", &scored(1), 30)
        .await
        .expect("put with ttl");

    kv_persist(engine, "players", b"a").await.expect("persist");
    tokio::time::sleep(Duration::from_millis(60)).await;

    assert_eq!(rank(engine, "lb", "a").await, Value::Integer(1));
}

#[tokio::test]
async fn a_unique_refusal_writes_no_sorted_entry() {
    let db = make_db().await;
    let engine = &db.query_engine;
    declare_index(
        engine,
        CreateIndexRequest {
            name: Some("uq_email"),
            collection: "players",
            field: "email",
            unique: true,
            case_insensitive: false,
            predicate: None,
            if_not_exists: false,
        },
        IndexEngine::KeyValue,
    )
    .await
    .expect("unique index");
    register_asc(engine, "players").await;
    let row = |score: i64| {
        body(&[
            ("email", Value::String("x@y".into())),
            ("score", Value::Integer(score)),
        ])
    };
    put(engine, "players", "a", &row(1)).await;

    let err = kv_put(engine, "players", b"b", &row(2), 0)
        .await
        .expect_err("unique index refuses");

    assert!(matches!(err, LiteError::UniqueViolation { .. }), "{err:?}");
    assert_eq!(ranked(engine, "lb").await, ["a"]);
    assert_eq!(super::harness::stored_entries(engine, "lb").await, 2);
}

#[tokio::test]
async fn a_buffered_public_put_is_visible_without_a_flush() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;

    db.kv_put("players", "a", &scored(4)).await.expect("kv_put");
    db.kv_put("players", "b", &scored(2)).await.expect("kv_put");

    assert_eq!(ranked(engine, "lb").await, ["b", "a"]);
}

#[tokio::test]
async fn an_inbound_sync_put_is_ranked() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;

    let row = kv_row_msgpack("a", &scored(3));
    db.kv_apply_remote_row("players", "a", &row, false)
        .await
        .expect("apply");

    assert_eq!(rank(engine, "lb", "a").await, Value::Integer(1));

    db.kv_apply_remote_row("players", "a", &[], true)
        .await
        .expect("apply delete");

    assert_eq!(count(engine, "lb").await, 0);
}
