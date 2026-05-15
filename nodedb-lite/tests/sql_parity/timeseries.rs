//! SQL parity tests: timeseries collections.
//!
//! DDL syntax differs between Lite and Origin:
//!   Lite   → CREATE TIMESERIES COLLECTION <name> (...) [PARTITION BY TIME(<interval>)]
//!   Origin → CREATE COLLECTION <name> (...) WITH (engine='timeseries')
//!
//! Parity contract for timeseries:
//!   CREATE/DROP — both sides execute without error
//!   INSERT      — acknowledged on both sides
//!   SELECT      — Origin returns rows; Lite returns empty result (known gap)
//!
//! The timeseries engine in Lite uses the columnar engine under the hood with
//! a Timeseries profile. DML routing to the timeseries engine is not yet wired
//! in execute_plan for the beta. Documented in lite-sql-support.md.

use nodedb_client::NodeDb;

use crate::common::origin::OriginServer;
use crate::common::sql::{OriginPgwire, open_lite};

const CREATE_LITE: &str = "CREATE TIMESERIES COLLECTION ts_parity (
    time  TIMESTAMP NOT NULL,
    host  TEXT,
    cpu   FLOAT64
) PARTITION BY TIME(1h)";

const CREATE_ORIGIN: &str = "CREATE COLLECTION ts_parity (
    time  TIMESTAMP NOT NULL,
    host  TEXT,
    cpu   FLOAT64
) WITH (engine='timeseries')";

#[tokio::test]
async fn timeseries_create_and_drop() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[])
        .await
        .expect("Lite CREATE timeseries ts_parity");

    pg.execute("DROP COLLECTION ts_parity").await;
    db.execute_sql("DROP COLLECTION ts_parity", &[])
        .await
        .expect("Lite DROP timeseries ts_parity");
}

#[tokio::test]
async fn timeseries_insert_acknowledged() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[]).await.expect("Lite CREATE");

    let sql =
        "INSERT INTO ts_parity (time, host, cpu) VALUES ('2024-06-01 12:00:00', 'web01', 0.45)";

    pg.execute(sql).await;

    let r = db
        .execute_sql(sql, &[])
        .await
        .expect("Lite timeseries INSERT");

    assert!(
        r.rows_affected >= 1,
        "Lite timeseries INSERT must acknowledge >= 1 affected row, got {}",
        r.rows_affected
    );
}

#[tokio::test]
async fn timeseries_select_gap_documented() {
    // Known parity gap: Lite timeseries SELECT returns empty result.
    // Origin returns the inserted rows. Documented in lite-sql-support.md.
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;
    let db = open_lite().await;

    pg.execute(CREATE_ORIGIN).await;
    db.execute_sql(CREATE_LITE, &[]).await.expect("Lite CREATE");

    let insert =
        "INSERT INTO ts_parity (time, host, cpu) VALUES ('2024-06-01 12:00:00', 'web01', 0.45)";
    pg.execute(insert).await;
    db.execute_sql(insert, &[]).await.expect("Lite INSERT");

    let origin_rows = pg.query("SELECT time, host, cpu FROM ts_parity").await;
    let lite_result = db
        .execute_sql("SELECT time, host, cpu FROM ts_parity", &[])
        .await
        .expect("Lite SELECT");

    assert_eq!(origin_rows.len(), 1, "Origin must return 1 timeseries row");
    assert_eq!(
        lite_result.rows.len(),
        0,
        "KNOWN GAP: Lite timeseries SELECT returns 0 rows in beta"
    );
}

#[tokio::test]
async fn timeseries_default_columns() {
    // CREATE TIMESERIES without explicit columns uses defaults (time, value).
    // This is a Lite-only DDL path; no matching test against Origin needed
    // since the default columns aren't Origin syntax. The test verifies the
    // DDL succeeds and subsequent INSERT is acknowledged.
    let db = open_lite().await;

    db.execute_sql("CREATE TIMESERIES COLLECTION ts_defaults", &[])
        .await
        .expect("Lite CREATE TIMESERIES defaults");

    let r = db
        .execute_sql(
            "INSERT INTO ts_defaults (time, value) VALUES ('2024-01-01 00:00:00', 1.0)",
            &[],
        )
        .await
        .expect("Lite INSERT into ts_defaults");

    assert!(
        r.rows_affected >= 1,
        "INSERT into default-column timeseries must acknowledge >= 1 row"
    );
}
