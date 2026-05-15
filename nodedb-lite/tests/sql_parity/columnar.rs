//! SQL parity tests: columnar analytics collections.
//!
//! DDL syntax differs between Lite and Origin:
//!   Lite   → CREATE COLLECTION <name> (...) WITH storage = 'columnar'
//!   Origin → CREATE COLLECTION <name> (...) WITH (engine='columnar')
//!
//! Parity contract for columnar:
//!   CREATE/DROP — both sides execute without error
//!   INSERT      — acknowledged on both sides (rows_affected >= 1)
//!   SELECT      — both sides return the same number of rows
//!
//! Note: Lite columnar INSERT goes through execute_insert → CRDT store
//! (the execute_plan arm for non-schemaless engine types currently returns
//! QueryResult::empty). This is a known parity gap recorded in
//! lite-sql-support.md: columnar INSERT/SELECT is not yet wired in Lite beta.

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
    // Columnar INSERT on Lite goes to the CRDT layer (not the columnar engine)
    // in beta — the call succeeds and acknowledges rows_affected = 1.
    // Origin's columnar INSERT writes to the columnar segment store.
    // Both sides must not return an error.
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
async fn columnar_select_gap_documented() {
    // Known parity gap: Lite columnar SELECT returns an empty result because
    // execute_scan for non-schemaless/non-strict engines falls through to
    // `Ok(QueryResult::empty())`. Origin returns the inserted rows.
    // This test documents the gap; it passes by asserting known behavior.
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[]).await.expect("Lite CREATE");

    pg.execute("INSERT INTO col_parity (id, ts, value) VALUES (1, '2024-01-01 00:00:00', 2.71)")
        .await;
    db.execute_sql(
        "INSERT INTO col_parity (id, ts, value) VALUES (1, '2024-01-01 00:00:00', 2.71)",
        &[],
    )
    .await
    .expect("Lite INSERT");

    let origin_rows = pg.query("SELECT id, value FROM col_parity").await;
    let lite_result = db
        .execute_sql("SELECT id, value FROM col_parity", &[])
        .await
        .expect("Lite SELECT");

    assert_eq!(origin_rows.len(), 1, "Origin must return 1 columnar row");
    assert_eq!(
        lite_result.rows.len(),
        0,
        "KNOWN GAP: Lite columnar SELECT returns 0 rows in beta"
    );
}
