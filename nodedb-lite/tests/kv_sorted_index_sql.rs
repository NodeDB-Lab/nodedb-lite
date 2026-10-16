// SPDX-License-Identifier: Apache-2.0

//! Sorted indexes (leaderboards) through the SQL surface.
//!
//! `CREATE SORTED INDEX` ranks the rows of a KV collection by a score
//! column, and SQL writes keep it current. `RANK`, `TOPK`, `RANGE` and
//! `SORTED_COUNT` read it. `DROP SORTED INDEX` removes it.
//!
//! Run with:
//!   cargo nextest run -p nodedb-lite --test kv_sorted_index_sql

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use nodedb_client::NodeDb;
use nodedb_lite::storage::engine::StorageEngine;
use nodedb_lite::{Encryption, NodeDbLite, PagedbStorageDefault, PagedbStorageMem};
use nodedb_types::error::{ErrorCode, NodeDbError};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

const PLAYERS: &str = "CREATE COLLECTION players \
     (key TEXT PRIMARY KEY, score BIGINT, ts BIGINT) WITH (engine='kv')";

async fn open_db() -> Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    NodeDbLite::open(storage).await.expect("NodeDbLite::open")
}

async fn exec<S: StorageEngine>(db: &NodeDbLite<S>, sql: &str) -> QueryResult {
    db.execute_sql(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
}

async fn exec_err<S: StorageEngine>(db: &NodeDbLite<S>, sql: &str) -> NodeDbError {
    match db.execute_sql(sql, &[]).await {
        Ok(result) => panic!("{sql}: expected an error, got {result:?}"),
        Err(error) => error,
    }
}

/// The `[rank, key]` rows of a TOPK or RANGE result.
async fn ranked<S: StorageEngine>(db: &NodeDbLite<S>, sql: &str) -> Vec<(i64, String)> {
    let result = exec(db, sql).await;
    assert_eq!(result.columns, vec!["rank", "key"], "{sql}");
    result
        .rows
        .iter()
        .map(|row| match (&row[0], &row[1]) {
            (Value::Integer(rank), Value::String(key)) => (*rank, key.clone()),
            other => panic!("{sql}: row is {other:?}"),
        })
        .collect()
}

async fn single<S: StorageEngine>(db: &NodeDbLite<S>, sql: &str, column: &str) -> Value {
    let result = exec(db, sql).await;
    assert_eq!(result.columns, vec![column], "{sql}");
    assert_eq!(result.rows.len(), 1, "{sql}");
    result.rows[0][0].clone()
}

fn now_ms() -> i64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch");
    i64::try_from(elapsed.as_millis()).expect("ms fit i64")
}

fn keys(rows: &[(i64, String)]) -> Vec<&str> {
    rows.iter().map(|(_, key)| key.as_str()).collect()
}

#[tokio::test]
async fn create_sorted_index_backfills_existing_rows_and_returns_the_count() {
    let db = open_db().await;
    exec(&db, PLAYERS).await;
    exec(
        &db,
        "INSERT INTO players (key, score) VALUES ('a', 10), ('b', 30), ('c', 20)",
    )
    .await;
    // A row with no score is not ranked.
    exec(&db, "INSERT INTO players (key, ts) VALUES ('unscored', 5)").await;

    let result = exec(&db, "CREATE SORTED INDEX lb ON players (score ASC) KEY key").await;
    assert_eq!(result.columns, vec!["index", "backfilled"]);
    assert_eq!(
        result.rows,
        vec![vec![Value::String("lb".into()), Value::Integer(3)]]
    );
    assert_eq!(
        keys(&ranked(&db, "SELECT * FROM TOPK(lb, 10)").await),
        vec!["a", "c", "b"]
    );
}

#[tokio::test]
async fn rank_topk_range_and_sorted_count_follow_sql_writes() {
    let db = open_db().await;
    exec(&db, PLAYERS).await;
    exec(&db, "CREATE SORTED INDEX lb ON players (score) KEY key").await;
    exec(
        &db,
        "INSERT INTO players (key, score) VALUES ('a', 10), ('b', 30), ('c', 20), ('d', -5)",
    )
    .await;
    exec(&db, "UPSERT INTO players (key, score) VALUES ('a', 40)").await;

    assert_eq!(
        ranked(&db, "SELECT TOPK(lb, 3)").await,
        vec![(1, "d".into()), (2, "c".into()), (3, "b".into())]
    );
    assert_eq!(
        single(&db, "SELECT RANK(lb, 'a')", "rank").await,
        Value::Integer(4)
    );
    assert_eq!(
        single(&db, "SELECT RANK(lb, 'nobody')", "rank").await,
        Value::Null
    );
    assert_eq!(
        ranked(&db, "SELECT * FROM RANGE(lb, 0, 30);").await,
        vec![(2, "c".into()), (3, "b".into())]
    );
    assert_eq!(
        ranked(&db, "SELECT * FROM RANGE(lb, '25', NULL)").await,
        vec![(3, "b".into()), (4, "a".into())]
    );
    assert_eq!(
        single(&db, "SELECT SORTED_COUNT(lb)", "sorted_count").await,
        Value::Integer(4)
    );

    exec(&db, "DELETE FROM players WHERE key = 'c'").await;
    assert_eq!(
        single(&db, "SELECT SORTED_COUNT('lb')", "sorted_count").await,
        Value::Integer(3)
    );
    assert_eq!(
        keys(&ranked(&db, "SELECT * FROM TOPK(lb, 10)").await),
        vec!["d", "b", "a"]
    );
}

#[tokio::test]
async fn descending_index_ranks_the_highest_score_first() {
    let db = open_db().await;
    exec(&db, PLAYERS).await;
    exec(
        &db,
        "CREATE SORTED INDEX lb ON players (score DESC) KEY key",
    )
    .await;
    exec(
        &db,
        "INSERT INTO players (key, score) VALUES ('a', 30), ('b', 20), ('c', 10), ('d', 5)",
    )
    .await;

    assert_eq!(
        keys(&ranked(&db, "SELECT * FROM TOPK(lb, 10)").await),
        vec!["a", "b", "c", "d"]
    );
    assert_eq!(
        single(&db, "SELECT RANK(lb, 'c')", "rank").await,
        Value::Integer(3)
    );
    // The bounds stay `min, max` in score terms; the index order applies.
    assert_eq!(
        ranked(&db, "SELECT * FROM RANGE(lb, 10, 20)").await,
        vec![(2, "b".into()), (3, "c".into())]
    );
}

#[tokio::test]
async fn windowed_index_hides_rows_stamped_before_the_window() {
    let db = open_db().await;
    exec(&db, PLAYERS).await;
    // Stamped an hour ahead, the recent row stays inside the current day's
    // window even when the test runs across midnight.
    let recent = now_ms() + 3_600_000;
    exec(
        &db,
        &format!(
            "INSERT INTO players (key, score, ts) VALUES ('old', 99, 1000), ('new', 1, {recent})"
        ),
    )
    .await;

    let created = exec(
        &db,
        "CREATE SORTED INDEX daily ON players (score DESC, ts DESC) KEY key WINDOW DAILY ON ts",
    )
    .await;
    assert_eq!(created.rows[0][1], Value::Integer(2), "every row is stored");
    assert_eq!(
        keys(&ranked(&db, "SELECT * FROM TOPK(daily, 10)").await),
        vec!["new"]
    );
    assert_eq!(
        single(&db, "SELECT SORTED_COUNT(daily)", "sorted_count").await,
        Value::Integer(1)
    );
    assert_eq!(
        single(&db, "SELECT RANK(daily, 'old')", "rank").await,
        Value::Null
    );

    let now = now_ms();
    exec(
        &db,
        &format!(
            "CREATE SORTED INDEX season ON players (score, ts) KEY key \
             WINDOW CUSTOM START '{}' END '{}' ON ts",
            now - 3_600_000,
            now + 86_400_000
        ),
    )
    .await;
    assert_eq!(
        keys(&ranked(&db, "SELECT * FROM TOPK(season, 10)").await),
        vec!["new"]
    );
}

#[tokio::test]
async fn drop_sorted_index_removes_the_index() {
    let db = open_db().await;
    exec(&db, PLAYERS).await;
    exec(&db, "CREATE SORTED INDEX lb ON players (score) KEY key").await;
    exec(&db, "INSERT INTO players (key, score) VALUES ('a', 1)").await;

    let dropped = exec(&db, "DROP SORTED INDEX lb;").await;
    assert_eq!(dropped.columns, vec!["dropped"]);
    assert_eq!(dropped.rows, vec![vec![Value::String("lb".into())]]);

    let error = exec_err(&db, "SELECT * FROM TOPK(lb, 10)").await;
    assert_eq!(error.code(), ErrorCode::UNDEFINED_OBJECT, "{error}");
    let error = exec_err(&db, "DROP SORTED INDEX lb").await;
    assert_eq!(error.code(), ErrorCode::UNDEFINED_OBJECT, "{error}");
    // The collection keeps its rows.
    assert!(db.kv_get("players", "a").await.expect("get").is_some());
}

#[tokio::test]
async fn reads_of_an_unknown_index_are_undefined_objects() {
    let db = open_db().await;
    exec(&db, PLAYERS).await;
    for sql in [
        "SELECT RANK(missing, 'a')",
        "SELECT TOPK(missing, 1)",
        "SELECT * FROM RANGE(missing, 0, 1)",
        "SELECT SORTED_COUNT(missing)",
        "DROP SORTED INDEX missing",
    ] {
        let error = exec_err(&db, sql).await;
        assert_eq!(error.code(), ErrorCode::UNDEFINED_OBJECT, "{sql}: {error}");
        assert!(error.to_string().contains("missing"), "{sql}: {error}");
    }
}

#[tokio::test]
async fn malformed_statements_return_parse_errors_in_origin_wording() {
    let db = open_db().await;
    exec(&db, PLAYERS).await;
    for (sql, wording) in [
        (
            "CREATE SORTED INDEX lb ON players (score)",
            "missing KEY clause",
        ),
        (
            "CREATE SORTED INDEX lb players (score) KEY key",
            "missing ON clause",
        ),
        (
            "CREATE SORTED INDEX lb ON players (score DOWN) KEY key",
            "invalid sort direction 'DOWN', expected ASC or DESC",
        ),
        (
            "CREATE SORTED INDEX lb ON players () KEY key",
            "at least one sort column required",
        ),
        (
            "CREATE SORTED INDEX lb ON players (score) KEY key WINDOW HOURLY",
            "unknown WINDOW type 'HOURLY'",
        ),
        (
            "CREATE SORTED INDEX lb ON players (score) KEY key WINDOW CUSTOM START 5",
            "WINDOW CUSTOM requires START and END",
        ),
        (
            "SELECT RANK(lb)",
            "RANK requires 2 arguments: (index_name, key_value)",
        ),
        (
            "SELECT TOPK(lb, many)",
            "TOPK: k must be a positive integer, got 'many'",
        ),
        (
            "SELECT * FROM RANGE(lb, 1)",
            "RANGE requires 3 arguments: (index_name, score_min, score_max)",
        ),
        (
            "SELECT SORTED_COUNT()",
            "SORTED_COUNT requires 1 argument: (index_name)",
        ),
    ] {
        let error = exec_err(&db, sql).await;
        assert_eq!(error.code(), ErrorCode::PLAN_ERROR, "{sql}: {error}");
        assert!(error.to_string().contains(wording), "{sql}: {error}");
    }
}

#[tokio::test]
async fn unsupported_sort_shapes_are_refused_naming_the_construct() {
    let db = open_db().await;
    exec(&db, PLAYERS).await;

    let error = exec_err(
        &db,
        "CREATE SORTED INDEX lb ON players (score DESC, key ASC) KEY key",
    )
    .await;
    assert_eq!(error.code(), ErrorCode::SQL_NOT_ENABLED, "{error}");
    assert!(error.to_string().contains("sort column 'key'"), "{error}");

    exec(&db, "CREATE SORTED INDEX lb ON players (score) KEY key").await;
    let error = exec_err(&db, "SELECT * FROM RANGE(lb, 'gold', NULL)").await;
    assert_eq!(error.code(), ErrorCode::SQL_NOT_ENABLED, "{error}");
    assert!(error.to_string().contains("gold"), "{error}");
}

#[tokio::test]
async fn window_validation_and_collection_checks_refuse_the_index() {
    let db = open_db().await;
    exec(&db, PLAYERS).await;

    let error = exec_err(
        &db,
        "CREATE SORTED INDEX lb ON players (score DESC) KEY key WINDOW DAILY ON ts",
    )
    .await;
    assert_eq!(error.code(), ErrorCode::BAD_REQUEST, "{error}");
    assert!(
        error
            .to_string()
            .contains("must be included in sort columns"),
        "{error}"
    );

    let error = exec_err(&db, "CREATE SORTED INDEX lb ON nowhere (score) KEY key").await;
    assert_eq!(error.code(), ErrorCode::COLLECTION_NOT_FOUND, "{error}");

    exec(&db, "CREATE COLLECTION notes").await;
    let error = exec_err(&db, "CREATE SORTED INDEX lb ON notes (score) KEY id").await;
    assert_eq!(error.code(), ErrorCode::COLLECTION_NOT_FOUND, "{error}");
}

#[tokio::test]
async fn sorted_index_survives_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("sorted_reopen.pagedb");

    {
        let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
            .await
            .expect("open storage");
        let db = NodeDbLite::open(storage).await.expect("open db");
        exec(&db, PLAYERS).await;
        exec(
            &db,
            "CREATE SORTED INDEX lb ON players (score DESC) KEY key",
        )
        .await;
        exec(
            &db,
            "INSERT INTO players (key, score) VALUES ('a', 10), ('b', 20)",
        )
        .await;
        db.flush().await.expect("flush");
        db.shutdown().await;
    }

    let storage = PagedbStorageDefault::open(&path, Encryption::Plaintext)
        .await
        .expect("reopen storage");
    let db = NodeDbLite::open(storage).await.expect("reopen db");
    assert_eq!(
        keys(&ranked(&db, "SELECT * FROM TOPK(lb, 10)").await),
        vec!["b", "a"]
    );
    // Writes after the reopen keep the index current.
    exec(&db, "INSERT INTO players (key, score) VALUES ('c', 15)").await;
    assert_eq!(
        keys(&ranked(&db, "SELECT * FROM TOPK(lb, 10)").await),
        vec!["b", "c", "a"]
    );
    assert_eq!(
        single(&db, "SELECT SORTED_COUNT(lb)", "sorted_count").await,
        Value::Integer(3)
    );
}
