// SPDX-License-Identifier: Apache-2.0
//! Sorted-index reads: order, ties, ranges, and unknown indexes.

use nodedb_types::value::Value;

use crate::error::LiteError;

use super::super::keys::f64_to_sort_bytes;
use super::super::{
    kv_sorted_index_count, kv_sorted_index_range, kv_sorted_index_rank, kv_sorted_index_score,
    kv_sorted_index_top_k,
};
use super::harness::{Engine, Register, make_db, put, ranked, register_asc, scored};

async fn register_desc(engine: &Engine) {
    Register::plain("players", "lb", &[("score", "DESC")])
        .run(engine)
        .await
        .expect("register");
}

/// `[rank, key]` rows of a range read over `[min, max]`.
async fn range(engine: &Engine, min: f64, max: f64) -> Vec<(i64, String)> {
    let result = kv_sorted_index_range(
        engine,
        "lb",
        Some(&f64_to_sort_bytes(min)),
        Some(&f64_to_sort_bytes(max)),
    )
    .await
    .expect("range");
    assert_eq!(result.columns, vec!["rank", "key"]);
    result
        .rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Integer(rank), Value::Bytes(key)) => {
                (*rank, String::from_utf8(key.clone()).expect("utf-8 key"))
            }
            other => panic!("range row is {other:?}"),
        })
        .collect()
}

async fn seed_four(engine: &Engine) {
    for (key, score) in [("a", 10), ("b", 20), ("c", 30), ("d", 40)] {
        put(engine, "players", key, &scored(score)).await;
    }
}

#[tokio::test]
async fn a_descending_index_ranks_the_highest_score_first() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_desc(engine).await;
    for (key, score) in [("a", 1), ("b", 3), ("c", 2)] {
        put(engine, "players", key, &scored(score)).await;
    }

    assert_eq!(ranked(engine, "lb").await, ["b", "c", "a"]);
    let score = kv_sorted_index_score(engine, "lb", b"b")
        .await
        .expect("score");
    assert_eq!(score.columns, vec!["score"]);
    assert_eq!(score.rows, vec![vec![Value::Float(3.0)]]);
}

#[tokio::test]
async fn tied_scores_rank_by_key_in_both_directions() {
    for desc in [false, true] {
        let db = make_db().await;
        let engine = &db.query_engine;
        if desc {
            register_desc(engine).await;
        } else {
            register_asc(engine, "players").await;
        }
        put(engine, "players", "b", &scored(5)).await;
        put(engine, "players", "a", &scored(5)).await;

        assert_eq!(ranked(engine, "lb").await, ["a", "b"], "desc = {desc}");
    }
}

#[tokio::test]
async fn top_k_returns_the_first_k_entries() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    seed_four(engine).await;

    let result = kv_sorted_index_top_k(engine, "lb", 2).await.expect("top_k");

    assert_eq!(
        result.rows,
        vec![
            vec![Value::Integer(1), Value::Bytes(b"a".to_vec())],
            vec![Value::Integer(2), Value::Bytes(b"b".to_vec())],
        ]
    );
}

#[tokio::test]
async fn range_bounds_are_inclusive_ascending() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    seed_four(engine).await;

    assert_eq!(
        range(engine, 20.0, 30.0).await,
        [(2, "b".to_string()), (3, "c".to_string())]
    );
}

#[tokio::test]
async fn range_bounds_are_inclusive_descending() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_desc(engine).await;
    seed_four(engine).await;

    assert_eq!(
        range(engine, 20.0, 30.0).await,
        [(2, "c".to_string()), (3, "b".to_string())]
    );
}

#[tokio::test]
async fn an_open_range_bound_admits_every_score_on_that_side() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;
    seed_four(engine).await;

    let result = kv_sorted_index_range(engine, "lb", Some(&f64_to_sort_bytes(30.0)), None)
        .await
        .expect("range");

    assert_eq!(result.rows.len(), 2);
}

#[tokio::test]
async fn a_range_bound_that_is_not_eight_bytes_is_a_bad_request() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register_asc(engine, "players").await;

    let err = kv_sorted_index_range(engine, "lb", Some(b"short"), None)
        .await
        .expect_err("refused");

    assert!(matches!(err, LiteError::BadRequest { .. }), "{err:?}");
}

#[tokio::test]
async fn an_unknown_index_is_not_found_for_set_reads_and_null_for_point_reads() {
    let db = make_db().await;
    let engine = &db.query_engine;

    for err in [
        kv_sorted_index_top_k(engine, "ghost", 3)
            .await
            .expect_err("top_k"),
        kv_sorted_index_range(engine, "ghost", None, None)
            .await
            .expect_err("range"),
        kv_sorted_index_count(engine, "ghost")
            .await
            .expect_err("count"),
    ] {
        assert!(matches!(err, LiteError::UndefinedObject { .. }), "{err:?}");
    }

    let rank = kv_sorted_index_rank(engine, "ghost", b"a")
        .await
        .expect("rank");
    assert_eq!(rank.rows, vec![vec![Value::Null]]);
    let score = kv_sorted_index_score(engine, "ghost", b"a")
        .await
        .expect("score");
    assert_eq!(score.rows, vec![vec![Value::Null]]);
}
