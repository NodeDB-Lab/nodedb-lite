//! SQL `SET` assignments with a computed right-hand side.
//!
//! - An assignment evaluates against the row's pre-image: `n = n + 1`
//!   increments, and `SET a = b, b = a` swaps.
//! - A function call (`LOWER(name)`) evaluates per row.
//! - Strict and schemaless collections behave the same.
//! - An assignment that fails on one row leaves every row unchanged.
//! - `MERGE ... WHEN MATCHED THEN UPDATE` and `UPDATE ... FROM` read target
//!   and source columns in one expression.
//!
//! Run with:
//!   cargo nextest run -p nodedb-lite --test sql_update_expressions

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::value::Value;

async fn open_test_db() -> Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    NodeDbLite::open(storage).await.expect("NodeDbLite::open")
}

async fn sql(db: &NodeDbLite<PagedbStorageMem>, statement: &str) {
    db.execute_sql(statement, &[])
        .await
        .unwrap_or_else(|e| panic!("SQL {statement:?}: {e}"));
}

/// The single value a one-row, one-column query returns.
async fn scalar(db: &NodeDbLite<PagedbStorageMem>, statement: &str) -> Value {
    let result = db
        .execute_sql(statement, &[])
        .await
        .unwrap_or_else(|e| panic!("SQL {statement:?}: {e}"));
    assert_eq!(result.rows.len(), 1, "{statement:?} returned {result:?}");
    result.rows[0]
        .first()
        .cloned()
        .unwrap_or_else(|| panic!("{statement:?} returned an empty row"))
}

async fn seed_strict(db: &NodeDbLite<PagedbStorageMem>) {
    sql(
        db,
        "CREATE COLLECTION counters (
            id TEXT NOT NULL PRIMARY KEY,
            n BIGINT,
            d BIGINT,
            a BIGINT,
            b BIGINT,
            name TEXT
        ) WITH storage = 'strict'",
    )
    .await;
    sql(
        db,
        "INSERT INTO counters (id, n, d, a, b, name) VALUES ('r1', 1, 2, 10, 20, 'Alice')",
    )
    .await;
    sql(
        db,
        "INSERT INTO counters (id, n, d, a, b, name) VALUES ('r2', 5, 0, 30, 40, 'BOB')",
    )
    .await;
}

async fn seed_schemaless(db: &NodeDbLite<PagedbStorageMem>) {
    sql(db, "CREATE COLLECTION tallies").await;
    sql(db, "INSERT INTO tallies (id, n, d) VALUES ('t1', 1, 2)").await;
    sql(db, "INSERT INTO tallies (id, n, d) VALUES ('t2', 5, 0)").await;
}

#[tokio::test]
async fn strict_update_increments_from_the_stored_value() {
    let db = open_test_db().await;
    seed_strict(&db).await;
    sql(&db, "UPDATE counters SET n = n + 1 WHERE id = 'r1'").await;
    assert_eq!(
        scalar(&db, "SELECT n FROM counters WHERE id = 'r1'").await,
        Value::Integer(2)
    );
    assert_eq!(
        scalar(&db, "SELECT n FROM counters WHERE id = 'r2'").await,
        Value::Integer(5),
        "a row the WHERE does not match keeps its value"
    );
}

#[tokio::test]
async fn strict_update_evaluates_a_function_per_row() {
    let db = open_test_db().await;
    seed_strict(&db).await;
    sql(&db, "UPDATE counters SET name = LOWER(name)").await;
    assert_eq!(
        scalar(&db, "SELECT name FROM counters WHERE id = 'r1'").await,
        Value::String("alice".into())
    );
    assert_eq!(
        scalar(&db, "SELECT name FROM counters WHERE id = 'r2'").await,
        Value::String("bob".into())
    );
}

#[tokio::test]
async fn strict_assignments_read_the_pre_image_so_a_pair_swaps() {
    let db = open_test_db().await;
    seed_strict(&db).await;
    sql(&db, "UPDATE counters SET a = b, b = a WHERE id = 'r1'").await;
    assert_eq!(
        scalar(&db, "SELECT a FROM counters WHERE id = 'r1'").await,
        Value::Integer(20)
    );
    assert_eq!(
        scalar(&db, "SELECT b FROM counters WHERE id = 'r1'").await,
        Value::Integer(10)
    );
}

#[tokio::test]
async fn schemaless_update_increments_from_the_stored_value() {
    let db = open_test_db().await;
    seed_schemaless(&db).await;
    sql(&db, "UPDATE tallies SET n = n + 1 WHERE id = 't1'").await;
    assert_eq!(
        scalar(&db, "SELECT n FROM tallies WHERE id = 't1'").await,
        Value::Integer(2)
    );
    assert_eq!(
        scalar(&db, "SELECT n FROM tallies WHERE id = 't2'").await,
        Value::Integer(5)
    );
}

#[tokio::test]
async fn strict_update_failing_on_one_row_leaves_every_row_unchanged() {
    let db = open_test_db().await;
    seed_strict(&db).await;
    let err = db
        .execute_sql("UPDATE counters SET n = 10 / d", &[])
        .await
        .expect_err("row r2 divides by zero");
    assert!(
        err.to_string().to_lowercase().contains("zero"),
        "the error names the division by zero: {err}"
    );
    assert_eq!(
        scalar(&db, "SELECT n FROM counters WHERE id = 'r1'").await,
        Value::Integer(1)
    );
    assert_eq!(
        scalar(&db, "SELECT n FROM counters WHERE id = 'r2'").await,
        Value::Integer(5)
    );
}

#[tokio::test]
async fn schemaless_update_failing_on_one_row_leaves_every_row_unchanged() {
    let db = open_test_db().await;
    seed_schemaless(&db).await;
    db.execute_sql("UPDATE tallies SET n = 10 / d", &[])
        .await
        .expect_err("row t2 divides by zero");
    assert_eq!(
        scalar(&db, "SELECT n FROM tallies WHERE id = 't1'").await,
        Value::Integer(1)
    );
    assert_eq!(
        scalar(&db, "SELECT n FROM tallies WHERE id = 't2'").await,
        Value::Integer(5)
    );
}

#[tokio::test]
async fn merge_matched_update_adds_source_to_target() {
    let db = open_test_db().await;
    sql(&db, "CREATE COLLECTION stock").await;
    sql(&db, "CREATE COLLECTION delivery").await;
    sql(&db, "INSERT INTO stock (id, qty) VALUES ('p1', 3)").await;
    sql(&db, "INSERT INTO delivery (id, qty) VALUES ('p1', 4)").await;
    sql(
        &db,
        "MERGE INTO stock AS t \
         USING (SELECT id, qty FROM delivery) AS s \
         ON t.id = s.id \
         WHEN MATCHED THEN UPDATE SET qty = t.qty + s.qty",
    )
    .await;
    assert_eq!(
        scalar(&db, "SELECT qty FROM stock WHERE id = 'p1'").await,
        Value::Integer(7)
    );
}

#[tokio::test]
async fn update_from_evaluates_an_expression_over_the_source_row() {
    let db = open_test_db().await;
    sql(&db, "CREATE COLLECTION orders").await;
    sql(&db, "CREATE COLLECTION supply").await;
    sql(&db, "INSERT INTO orders (id, total) VALUES ('o1', 0)").await;
    sql(&db, "INSERT INTO orders (id, total) VALUES ('o2', 0)").await;
    sql(&db, "INSERT INTO supply (id, qty) VALUES ('o1', 6)").await;
    sql(
        &db,
        "UPDATE orders SET total = s.qty * 2 FROM supply AS s WHERE orders.id = s.id",
    )
    .await;
    assert_eq!(
        scalar(&db, "SELECT total FROM orders WHERE id = 'o1'").await,
        Value::Integer(12)
    );
    assert_eq!(
        scalar(&db, "SELECT total FROM orders WHERE id = 'o2'").await,
        Value::Integer(0),
        "a target row with no source match keeps its value"
    );
}
