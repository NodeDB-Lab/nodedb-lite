// SPDX-License-Identifier: Apache-2.0
//! Windowed sorted indexes: reads see only entries stamped in the current
//! window.

use nodedb_types::value::Value;

use super::harness::{Engine, Register, count, make_db, put, rank, ranked, stamped};

const MS_PER_DAY: u64 = 86_400_000;
const SORT: &[(&str, &str)] = &[("score", "ASC"), ("ts", "ASC")];

async fn register(engine: &Engine, window_type: &str, start_ms: u64, end_ms: u64) {
    Register::windowed("players", "lb", SORT, window_type, start_ms, end_ms)
        .run(engine)
        .await
        .expect("register");
}

fn now() -> u64 {
    crate::runtime::now_millis()
}

#[tokio::test]
async fn a_daily_window_hides_rows_stamped_before_today() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register(engine, "daily", 0, 0).await;
    let today = now();
    let midnight = (today / MS_PER_DAY) * MS_PER_DAY;
    put(engine, "players", "old", &stamped(1, midnight - 1)).await;
    put(engine, "players", "new", &stamped(2, today)).await;

    assert_eq!(ranked(engine, "lb").await, ["new"]);
}

#[tokio::test]
async fn a_custom_window_hides_rows_stamped_before_its_start() {
    let db = make_db().await;
    let engine = &db.query_engine;
    let start = now() - 60_000;
    register(engine, "custom", start, now() + 3_600_000).await;
    put(engine, "players", "early", &stamped(1, start - 1)).await;
    put(engine, "players", "inside", &stamped(2, start)).await;

    assert_eq!(ranked(engine, "lb").await, ["inside"]);
}

#[tokio::test]
async fn a_custom_window_shows_nothing_while_now_is_outside_it() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register(engine, "custom", 1_000, 2_000).await;
    put(engine, "players", "a", &stamped(1, 1_500)).await;

    assert_eq!(count(engine, "lb").await, 0);
    assert_eq!(rank(engine, "lb", "a").await, Value::Null);
}

#[tokio::test]
async fn a_windowed_rank_counts_only_entries_in_the_window() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register(engine, "daily", 0, 0).await;
    let today = now();
    put(
        engine,
        "players",
        "yesterday",
        &stamped(1, today - MS_PER_DAY),
    )
    .await;
    put(engine, "players", "x", &stamped(5, today)).await;
    put(engine, "players", "y", &stamped(9, today)).await;

    assert_eq!(rank(engine, "lb", "x").await, Value::Integer(1));
    assert_eq!(rank(engine, "lb", "y").await, Value::Integer(2));
    assert_eq!(rank(engine, "lb", "yesterday").await, Value::Null);
    assert_eq!(count(engine, "lb").await, 2);
}

#[tokio::test]
async fn a_windowed_row_without_a_timestamp_is_not_ranked() {
    let db = make_db().await;
    let engine = &db.query_engine;
    register(engine, "weekly", 0, 0).await;
    put(engine, "players", "bare", &super::harness::scored(1)).await;
    put(engine, "players", "x", &stamped(2, now())).await;

    assert_eq!(ranked(engine, "lb").await, ["x"]);
    assert_eq!(super::harness::stored_entries(engine, "lb").await, 2);
}
