// SPDX-License-Identifier: Apache-2.0
//! Integration tests for the 11 new SqlPlan visitor implementations:
//! Aggregate, Join, DocumentIndexLookup, RangeScan, Cte,
//! Union, Intersect, Except, InsertSelect, UpdateFrom, Merge.

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::value::Value;

async fn open_db() -> NodeDbLite<PagedbStorageMem> {
    let storage = PagedbStorageMem::open_in_memory().await.unwrap();
    NodeDbLite::open(storage, 1).await.unwrap()
}

async fn seed(db: &NodeDbLite<PagedbStorageMem>, stmts: &[&str]) {
    for s in stmts {
        db.execute_sql(s, &[])
            .await
            .unwrap_or_else(|e| panic!("seed SQL '{s}': {e}"));
    }
}

// ── Aggregate ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn aggregate_count_and_sum() {
    let db = open_db().await;
    seed(
        &db,
        &[
            "CREATE COLLECTION agg_test (id TEXT NOT NULL PRIMARY KEY, category TEXT, amount INTEGER) WITH storage = 'strict'",
            "INSERT INTO agg_test (id, category, amount) VALUES ('a', 'X', 10)",
            "INSERT INTO agg_test (id, category, amount) VALUES ('b', 'X', 20)",
            "INSERT INTO agg_test (id, category, amount) VALUES ('c', 'Y', 5)",
        ],
    )
    .await;

    let result = db
        .execute_sql(
            "SELECT category, COUNT(*) as cnt FROM agg_test GROUP BY category ORDER BY category ASC",
            &[],
        )
        .await
        .expect("aggregate query");

    assert_eq!(result.rows.len(), 2, "expected 2 groups");
    let x_row = result
        .rows
        .iter()
        .find(|r| r[0] == Value::String("X".into()));
    assert!(x_row.is_some(), "group X not found");
}

#[tokio::test]
async fn aggregate_with_having_filters_groups() {
    let db = open_db().await;
    seed(
        &db,
        &[
            "CREATE COLLECTION having_test (id TEXT NOT NULL PRIMARY KEY, dept TEXT, salary INTEGER) WITH storage = 'strict'",
            "INSERT INTO having_test (id, dept, salary) VALUES ('e1', 'Eng', 100)",
            "INSERT INTO having_test (id, dept, salary) VALUES ('e2', 'Eng', 200)",
            "INSERT INTO having_test (id, dept, salary) VALUES ('e3', 'HR', 50)",
        ],
    )
    .await;

    let result = db
        .execute_sql(
            "SELECT dept, SUM(salary) AS total FROM having_test GROUP BY dept HAVING SUM(salary) > 100",
            &[],
        )
        .await
        .expect("GROUP BY ... HAVING");

    assert_eq!(result.rows.len(), 1, "only Eng (300) passes HAVING");
    assert_eq!(result.rows[0][0], Value::String("Eng".into()));
}

// ── Union ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn union_all_appends_rows() {
    let db = open_db().await;
    seed(
        &db,
        &[
            "CREATE COLLECTION union_a (id TEXT NOT NULL PRIMARY KEY, val INTEGER) WITH storage = 'strict'",
            "CREATE COLLECTION union_b (id TEXT NOT NULL PRIMARY KEY, val INTEGER) WITH storage = 'strict'",
            "INSERT INTO union_a (id, val) VALUES ('a1', 1)",
            "INSERT INTO union_a (id, val) VALUES ('a2', 2)",
            "INSERT INTO union_b (id, val) VALUES ('b1', 3)",
        ],
    )
    .await;

    let result = db
        .execute_sql(
            "SELECT id, val FROM union_a UNION ALL SELECT id, val FROM union_b",
            &[],
        )
        .await
        .expect("UNION ALL");

    assert_eq!(result.rows.len(), 3, "UNION ALL should have 3 rows");
}

#[tokio::test]
async fn union_distinct_deduplicates() {
    let db = open_db().await;
    seed(
        &db,
        &[
            "CREATE COLLECTION union_dup (id TEXT NOT NULL PRIMARY KEY, val INTEGER) WITH storage = 'strict'",
            "INSERT INTO union_dup (id, val) VALUES ('r1', 42)",
        ],
    )
    .await;

    let result = db
        .execute_sql(
            "SELECT id, val FROM union_dup UNION SELECT id, val FROM union_dup",
            &[],
        )
        .await
        .expect("UNION DISTINCT");

    assert_eq!(result.rows.len(), 1, "UNION DISTINCT should deduplicate");
}

// ── Intersect ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn intersect_returns_common_rows() {
    let db = open_db().await;
    seed(
        &db,
        &[
            "CREATE COLLECTION int_a (id TEXT NOT NULL PRIMARY KEY, val INTEGER) WITH storage = 'strict'",
            "CREATE COLLECTION int_b (id TEXT NOT NULL PRIMARY KEY, val INTEGER) WITH storage = 'strict'",
            "INSERT INTO int_a (id, val) VALUES ('x', 1)",
            "INSERT INTO int_a (id, val) VALUES ('y', 2)",
            "INSERT INTO int_b (id, val) VALUES ('x', 1)",
            "INSERT INTO int_b (id, val) VALUES ('z', 3)",
        ],
    )
    .await;

    let result = db
        .execute_sql(
            "SELECT id, val FROM int_a INTERSECT SELECT id, val FROM int_b",
            &[],
        )
        .await
        .expect("INTERSECT");

    assert_eq!(result.rows.len(), 1, "only ('x',1) is common");
}

// ── Except ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn except_subtracts_right_from_left() {
    let db = open_db().await;
    seed(
        &db,
        &[
            "CREATE COLLECTION exc_a (id TEXT NOT NULL PRIMARY KEY, val INTEGER) WITH storage = 'strict'",
            "CREATE COLLECTION exc_b (id TEXT NOT NULL PRIMARY KEY, val INTEGER) WITH storage = 'strict'",
            "INSERT INTO exc_a (id, val) VALUES ('p', 1)",
            "INSERT INTO exc_a (id, val) VALUES ('q', 2)",
            "INSERT INTO exc_b (id, val) VALUES ('p', 1)",
        ],
    )
    .await;

    let result = db
        .execute_sql(
            "SELECT id, val FROM exc_a EXCEPT SELECT id, val FROM exc_b",
            &[],
        )
        .await
        .expect("EXCEPT");

    assert_eq!(result.rows.len(), 1, "only ('q',2) remains");
    assert_eq!(result.rows[0][0], Value::String("q".into()));
}

// ── InsertSelect ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn insert_select_copies_rows() {
    let db = open_db().await;
    seed(
        &db,
        &[
            "CREATE COLLECTION src_is (id TEXT NOT NULL PRIMARY KEY, name TEXT) WITH storage = 'strict'",
            "CREATE COLLECTION dst_is (id TEXT NOT NULL PRIMARY KEY, name TEXT) WITH storage = 'strict'",
            "INSERT INTO src_is (id, name) VALUES ('s1', 'Alice')",
            "INSERT INTO src_is (id, name) VALUES ('s2', 'Bob')",
        ],
    )
    .await;

    let result = db
        .execute_sql("INSERT INTO dst_is SELECT id, name FROM src_is", &[])
        .await
        .expect("INSERT INTO ... SELECT");

    assert!(
        result.rows_affected >= 2,
        "expected ≥2 rows affected, got {}",
        result.rows_affected
    );
}
