//! SQL parity tests: strict document collections.
//!
//! Strict collections use Binary Tuple storage with a schema. DDL syntax differs:
//!   Lite   → CREATE COLLECTION <name> (...) WITH storage = 'strict'
//!   Origin → CREATE COLLECTION <name> (...) WITH (engine='document_strict')
//!
//! Parity contract for strict:
//!   CREATE  — both sides create successfully
//!   INSERT  — rows_affected matches
//!   SELECT  — column names match; row count matches; field values match
//!   DROP    — both sides drop successfully

use nodedb_client::NodeDb;

use crate::common::origin::OriginServer;
use crate::common::sql::{OriginPgwire, open_lite};

const CREATE_LITE: &str = "CREATE COLLECTION strict_parity (
    id   BIGINT NOT NULL PRIMARY KEY,
    name TEXT NOT NULL,
    score FLOAT64
) WITH storage = 'strict'";

const CREATE_ORIGIN: &str = "CREATE COLLECTION strict_parity (
    id   BIGINT NOT NULL,
    name TEXT NOT NULL,
    score FLOAT64
) WITH (engine='document_strict')";

#[tokio::test]
async fn strict_create_and_drop() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    // Both CREATE statements must succeed without error.
    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[])
        .await
        .expect("Lite CREATE strict_parity");

    // Both DROP statements must succeed.
    pg.execute("DROP COLLECTION strict_parity").await;
    db.execute_sql("DROP COLLECTION strict_parity", &[])
        .await
        .expect("Lite DROP strict_parity");
}

#[tokio::test]
async fn strict_insert_returns_affected() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[]).await.expect("Lite CREATE");

    // Origin INSERT via pgwire.
    pg.execute("INSERT INTO strict_parity (id, name, score) VALUES (1, 'Alice', 9.5)")
        .await;
    pg.execute("INSERT INTO strict_parity (id, name, score) VALUES (2, 'Bob', 8.0)")
        .await;

    // Lite INSERT.
    let r1 = db
        .execute_sql(
            "INSERT INTO strict_parity (id, name, score) VALUES (1, 'Alice', 9.5)",
            &[],
        )
        .await
        .expect("Lite INSERT 1");
    let r2 = db
        .execute_sql(
            "INSERT INTO strict_parity (id, name, score) VALUES (2, 'Bob', 8.0)",
            &[],
        )
        .await
        .expect("Lite INSERT 2");

    // Both inserts must acknowledge at least one affected row.
    assert!(
        r1.rows_affected >= 1,
        "Lite INSERT 1 must affect >= 1 row, got {}",
        r1.rows_affected
    );
    assert!(
        r2.rows_affected >= 1,
        "Lite INSERT 2 must affect >= 1 row, got {}",
        r2.rows_affected
    );
}

#[tokio::test]
async fn strict_select_all_rows() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[]).await.expect("Lite CREATE");

    // Insert 3 rows on both sides.
    for (id, name, score) in [(1i64, "Alice", 9.5f64), (2, "Bob", 8.0), (3, "Carol", 7.5)] {
        let sql =
            format!("INSERT INTO strict_parity (id, name, score) VALUES ({id}, '{name}', {score})");
        pg.execute(&sql).await;
        db.execute_sql(&sql, &[]).await.expect("Lite INSERT");
    }

    let origin_rows = pg.query("SELECT id, name, score FROM strict_parity").await;
    let lite_result = db
        .execute_sql("SELECT id, name, score FROM strict_parity", &[])
        .await
        .expect("Lite SELECT *");

    // Both sides must return 3 rows.
    assert_eq!(origin_rows.len(), 3, "Origin must return 3 rows");
    assert_eq!(
        lite_result.rows.len(),
        3,
        "Lite strict SELECT must return 3 rows after insert"
    );

    // Column names must include all schema columns.
    assert!(
        lite_result.columns.contains(&"id".to_string()),
        "Lite SELECT must include 'id' column, got: {:?}",
        lite_result.columns
    );
    assert!(
        lite_result.columns.contains(&"name".to_string()),
        "Lite SELECT must include 'name' column"
    );
    assert!(
        lite_result.columns.contains(&"score".to_string()),
        "Lite SELECT must include 'score' column"
    );

    // Row values must round-trip correctly (order-independent comparison).
    let mut lite_ids: Vec<String> = lite_result
        .rows
        .iter()
        .map(|r| {
            crate::common::sql::normalise_lite_row(&lite_result, 0)
                .get("id")
                .cloned()
                .unwrap_or_default();
            // Extract id from this row using column index.
            let id_idx = lite_result
                .columns
                .iter()
                .position(|c| c == "id")
                .unwrap_or(0);
            format!("{:?}", r[id_idx])
        })
        .collect();
    lite_ids.sort();
    assert_eq!(
        lite_ids,
        vec!["Integer(1)", "Integer(2)", "Integer(3)"],
        "Lite rows must contain id values 1, 2, 3"
    );
}

#[tokio::test]
async fn strict_update_returns_affected() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[]).await.expect("Lite CREATE");

    // Seed a row on both sides.
    pg.execute("INSERT INTO strict_parity (id, name, score) VALUES (1, 'Alice', 9.5)")
        .await;
    db.execute_sql(
        "INSERT INTO strict_parity (id, name, score) VALUES (1, 'Alice', 9.5)",
        &[],
    )
    .await
    .expect("Lite INSERT");

    // Update on Origin via pgwire.
    pg.execute("UPDATE strict_parity SET score = 8.0 WHERE id = 1")
        .await;

    // Update on Lite — must return at least 1 affected row, not an error.
    let r = db
        .execute_sql("UPDATE strict_parity SET score = 8.0 WHERE id = 1", &[])
        .await
        .expect("Lite UPDATE strict_parity");

    assert!(
        r.rows_affected >= 1,
        "Lite UPDATE must affect >= 1 row, got {}",
        r.rows_affected
    );
}

#[tokio::test]
async fn strict_delete_returns_affected() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[]).await.expect("Lite CREATE");

    // Seed a row on both sides.
    pg.execute("INSERT INTO strict_parity (id, name, score) VALUES (1, 'Alice', 9.5)")
        .await;
    db.execute_sql(
        "INSERT INTO strict_parity (id, name, score) VALUES (1, 'Alice', 9.5)",
        &[],
    )
    .await
    .expect("Lite INSERT");

    // Delete on Origin.
    pg.execute("DELETE FROM strict_parity WHERE id = 1").await;

    // Delete on Lite — must return at least 1 affected row, not an error.
    let r = db
        .execute_sql("DELETE FROM strict_parity WHERE id = 1", &[])
        .await
        .expect("Lite DELETE strict_parity");

    assert!(
        r.rows_affected >= 1,
        "Lite DELETE must affect >= 1 row, got {}",
        r.rows_affected
    );
}

#[tokio::test]
async fn strict_point_get_by_primary_key() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[]).await.expect("Lite CREATE");

    pg.execute("INSERT INTO strict_parity (id, name, score) VALUES (42, 'Eve', 7.0)")
        .await;
    db.execute_sql(
        "INSERT INTO strict_parity (id, name, score) VALUES (42, 'Eve', 7.0)",
        &[],
    )
    .await
    .expect("Lite INSERT");

    // Origin PointGet must return exactly 1 row.
    let origin_rows = pg
        .query("SELECT id, name, score FROM strict_parity WHERE id = 42")
        .await;
    assert_eq!(origin_rows.len(), 1, "Origin PointGet must return 1 row");

    // Lite PointGet must return exactly 1 row with matching values.
    let lite_result = db
        .execute_sql(
            "SELECT id, name, score FROM strict_parity WHERE id = 42",
            &[],
        )
        .await
        .expect("Lite PointGet strict_parity must not error");

    assert_eq!(
        lite_result.rows.len(),
        1,
        "Lite PointGet must return 1 row, got {}",
        lite_result.rows.len()
    );

    // Verify the id value round-trips correctly.
    let row = crate::common::sql::normalise_lite_row(&lite_result, 0);
    assert_eq!(
        row.get("id").map(|s| s.as_str()),
        Some("42"),
        "Lite PointGet id must be 42, got row: {row:?}"
    );
    assert_eq!(
        row.get("name").map(|s| s.as_str()),
        Some("Eve"),
        "Lite PointGet name must be 'Eve', got row: {row:?}"
    );
}
