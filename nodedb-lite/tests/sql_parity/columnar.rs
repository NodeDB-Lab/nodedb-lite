//! SQL parity tests: columnar analytics collections.
//!
//! DDL syntax differs between Lite and Origin:
//!   Lite   → CREATE COLLECTION <name> (...) WITH storage = 'columnar'
//!   Origin → CREATE COLLECTION <name> (...) WITH (engine='columnar')
//!
//! Parity contract for columnar:
//!   CREATE/DROP — both sides execute without error
//!   INSERT      — acknowledged on both sides (rows_affected >= 1)
//!   SELECT      — both sides return the same rows (count + id values)

use nodedb_client::NodeDb;

use crate::common::origin::OriginServer;
use crate::common::sql::{OriginPgwire, open_lite};

const CREATE_LITE: &str = "CREATE COLLECTION col_parity (
    id    BIGINT NOT NULL PRIMARY KEY,
    ts    TIMESTAMP NOT NULL,
    value FLOAT64
) WITH storage = 'columnar'";

const CREATE_ORIGIN: &str = "CREATE COLLECTION col_parity (
    id    BIGINT NOT NULL,
    ts    TIMESTAMP NOT NULL,
    value FLOAT64
) WITH (engine='columnar')";

#[tokio::test]
async fn columnar_create_and_drop() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[])
        .await
        .expect("Lite CREATE columnar col_parity");

    pg.execute("DROP COLLECTION col_parity").await;
    db.execute_sql("DROP COLLECTION col_parity", &[])
        .await
        .expect("Lite DROP columnar col_parity");
}

#[tokio::test]
async fn columnar_insert_acknowledged() {
    // Both sides must acknowledge rows_affected >= 1 for a columnar INSERT.
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[]).await.expect("Lite CREATE");

    pg.execute("INSERT INTO col_parity (id, ts, value) VALUES (1, '2024-01-01 00:00:00', 3.14)")
        .await;

    let r = db
        .execute_sql(
            "INSERT INTO col_parity (id, ts, value) VALUES (1, '2024-01-01 00:00:00', 3.14)",
            &[],
        )
        .await
        .expect("Lite columnar INSERT");

    assert!(
        r.rows_affected >= 1,
        "Lite columnar INSERT must acknowledge >= 1 affected row, got {}",
        r.rows_affected
    );
}

#[tokio::test]
async fn columnar_select_all_rows() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[]).await.expect("Lite CREATE");

    // Insert 3 rows on both sides.
    for (id, val) in [(1i64, 1.11f64), (2, 2.22), (3, 3.33)] {
        let sql = format!(
            "INSERT INTO col_parity (id, ts, value) VALUES ({id}, '2024-01-01 00:00:00', {val})"
        );
        pg.execute(&sql).await;
        db.execute_sql(&sql, &[]).await.expect("Lite INSERT");
    }

    let origin_rows = pg.query("SELECT id, value FROM col_parity").await;
    let lite_result = db
        .execute_sql("SELECT * FROM col_parity", &[])
        .await
        .expect("Lite SELECT *");

    assert_eq!(origin_rows.len(), 3, "Origin must return 3 columnar rows");
    assert_eq!(
        lite_result.rows.len(),
        3,
        "Lite columnar SELECT must return 3 rows after insert, got {}",
        lite_result.rows.len()
    );

    // Column names must include all schema columns.
    assert!(
        lite_result.columns.contains(&"id".to_string()),
        "Lite SELECT must include 'id' column, got: {:?}",
        lite_result.columns
    );
    assert!(
        lite_result.columns.contains(&"value".to_string()),
        "Lite SELECT must include 'value' column"
    );

    // Row id values must round-trip correctly.
    let id_idx = lite_result
        .columns
        .iter()
        .position(|c| c == "id")
        .expect("'id' column present");
    let mut lite_ids: Vec<i64> = lite_result
        .rows
        .iter()
        .filter_map(|r| {
            if let nodedb_types::value::Value::Integer(i) = r[id_idx] {
                Some(i)
            } else {
                None
            }
        })
        .collect();
    lite_ids.sort_unstable();
    assert_eq!(
        lite_ids,
        vec![1, 2, 3],
        "Lite columnar rows must contain id values 1, 2, 3"
    );
}
