// SPDX-License-Identifier: Apache-2.0

//! `TRUNCATE` on every Lite engine.
//!
//! Each test seeds three rows, truncates, checks the bare `TRUNCATE` tag,
//! checks every read path of that engine comes back empty, then writes one
//! row and reads back exactly that row.

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::sequence::LiteSequenceDef;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::BoundingBox;
use nodedb_types::document::Document;
use nodedb_types::geometry::Geometry;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

async fn open_db() -> Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    NodeDbLite::open(storage).await.expect("NodeDbLite::open")
}

async fn run(db: &NodeDbLite<PagedbStorageMem>, sql: &str) -> QueryResult {
    db.execute_sql(sql, &[])
        .await
        .unwrap_or_else(|e| panic!("SQL {sql:?}: {e}"))
}

async fn rows(db: &NodeDbLite<PagedbStorageMem>, sql: &str) -> Vec<Vec<Value>> {
    run(db, sql).await.rows
}

fn assert_bare_truncate_tag(result: &QueryResult) {
    assert_eq!(result.command.as_deref(), Some("TRUNCATE"));
    assert_eq!(result.rows_affected, 0);
    assert!(result.columns.is_empty(), "{:?}", result.columns);
    assert!(result.rows.is_empty(), "{:?}", result.rows);
}

// ── Document (schemaless) ────────────────────────────────────────────────────

#[tokio::test]
async fn truncate_schemaless_document_collection() {
    let db = open_db().await;
    run(&db, "CREATE COLLECTION doc_t").await;
    for i in 1..=3 {
        run(
            &db,
            &format!("INSERT INTO doc_t (id, n) VALUES ('d{i}', {i})"),
        )
        .await;
    }
    assert_eq!(rows(&db, "SELECT id FROM doc_t").await.len(), 3);

    let result = run(&db, "TRUNCATE doc_t").await;
    assert_bare_truncate_tag(&result);
    assert!(rows(&db, "SELECT id FROM doc_t").await.is_empty());
    assert!(
        db.document_get("doc_t", "d1")
            .await
            .expect("document_get")
            .is_none()
    );

    run(&db, "INSERT INTO doc_t (id, n) VALUES ('d9', 9)").await;
    let after = rows(&db, "SELECT id FROM doc_t").await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0][0], Value::String("d9".into()));
}

// ── Document (strict) ────────────────────────────────────────────────────────

#[tokio::test]
async fn truncate_strict_collection() {
    let db = open_db().await;
    run(
        &db,
        "CREATE COLLECTION strict_t (id TEXT NOT NULL PRIMARY KEY, n INTEGER) \
         WITH storage = 'strict'",
    )
    .await;
    for i in 1..=3 {
        run(
            &db,
            &format!("INSERT INTO strict_t (id, n) VALUES ('s{i}', {i})"),
        )
        .await;
    }
    assert_eq!(rows(&db, "SELECT id, n FROM strict_t").await.len(), 3);

    let result = run(&db, "TRUNCATE strict_t").await;
    assert_bare_truncate_tag(&result);
    assert!(rows(&db, "SELECT id, n FROM strict_t").await.is_empty());
    assert!(
        rows(&db, "SELECT id FROM strict_t WHERE id = 's1'")
            .await
            .is_empty()
    );

    run(&db, "INSERT INTO strict_t (id, n) VALUES ('s9', 9)").await;
    let after = rows(&db, "SELECT id, n FROM strict_t").await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0][0], Value::String("s9".into()));
    assert_eq!(after[0][1], Value::Integer(9));
}

// ── Key-Value ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn truncate_kv_collection_clears_rows_buffer_and_cache() {
    let db = open_db().await;
    run(
        &db,
        "CREATE COLLECTION kv_t (k TEXT NOT NULL PRIMARY KEY, v TEXT) WITH storage = 'kv'",
    )
    .await;
    for i in 1..=3 {
        db.kv_put("kv_t", &format!("k{i}"), format!("v{i}").as_bytes())
            .await
            .expect("kv_put");
    }
    db.kv_flush().await.expect("kv_flush");
    // A read before the truncate fills the read cache.
    assert_eq!(
        db.kv_get("kv_t", "k1").await.expect("kv_get").as_deref(),
        Some(b"v1".as_slice())
    );
    // An unflushed put sits in the write buffer.
    db.kv_put("kv_t", "k4", b"v4").await.expect("kv_put");
    db.kv_put("other_kv", "x", b"keep").await.expect("kv_put");

    let result = run(&db, "TRUNCATE kv_t").await;
    assert_bare_truncate_tag(&result);
    for k in ["k1", "k2", "k3", "k4"] {
        assert!(
            db.kv_get("kv_t", k).await.expect("kv_get").is_none(),
            "{k} survives truncate"
        );
    }
    db.kv_flush().await.expect("kv_flush");
    for k in ["k1", "k4"] {
        assert!(
            db.kv_get("kv_t", k).await.expect("kv_get").is_none(),
            "{k} resurrects on flush"
        );
    }
    assert!(db.kv_keys("kv_t").await.expect("kv_keys").is_empty());
    assert_eq!(
        db.kv_get("other_kv", "x").await.expect("kv_get").as_deref(),
        Some(b"keep".as_slice())
    );

    db.kv_put("kv_t", "k9", b"v9").await.expect("kv_put");
    assert_eq!(
        db.kv_get("kv_t", "k9").await.expect("kv_get").as_deref(),
        Some(b"v9".as_slice())
    );
    assert_eq!(db.kv_keys("kv_t").await.expect("kv_keys"), vec!["k9"]);
}

// ── Columnar ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn truncate_columnar_collection() {
    let db = open_db().await;
    run(
        &db,
        "CREATE COLLECTION col_t (id BIGINT NOT NULL PRIMARY KEY, v FLOAT64) \
         WITH storage = 'columnar'",
    )
    .await;
    for i in 1..=3 {
        run(
            &db,
            &format!("INSERT INTO col_t (id, v) VALUES ({i}, {i}.5)"),
        )
        .await;
    }
    assert_eq!(rows(&db, "SELECT id, v FROM col_t").await.len(), 3);

    let result = run(&db, "TRUNCATE col_t").await;
    assert_bare_truncate_tag(&result);
    assert!(rows(&db, "SELECT id, v FROM col_t").await.is_empty());

    run(&db, "INSERT INTO col_t (id, v) VALUES (9, 9.5)").await;
    let after = rows(&db, "SELECT id, v FROM col_t").await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0][0], Value::Integer(9));
}

// ── Timeseries ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn truncate_timeseries_collection() {
    let db = open_db().await;
    run(
        &db,
        "CREATE TIMESERIES COLLECTION ts_t (time TIMESTAMP NOT NULL, host TEXT, cpu FLOAT64) \
         PARTITION BY TIME(1h)",
    )
    .await;
    for (i, host) in ["web01", "web02", "web03"].iter().enumerate() {
        run(
            &db,
            &format!(
                "INSERT INTO ts_t (time, host, cpu) VALUES ('2024-06-01 12:0{i}:00', '{host}', 0.{i}5)"
            ),
        )
        .await;
    }
    assert_eq!(rows(&db, "SELECT host FROM ts_t").await.len(), 3);

    let result = run(&db, "TRUNCATE ts_t").await;
    assert_bare_truncate_tag(&result);
    assert!(rows(&db, "SELECT host FROM ts_t").await.is_empty());

    run(
        &db,
        "INSERT INTO ts_t (time, host, cpu) VALUES ('2024-06-01 13:00:00', 'web09', 0.9)",
    )
    .await;
    let after = rows(&db, "SELECT host FROM ts_t").await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0][0], Value::String("web09".into()));
}

// ── Spatial ──────────────────────────────────────────────────────────────────

const PLACES: &str = "places_t";
const LOCATION: &str = "location";
const NEAR_LONDON: &str = "{\"type\":\"Point\",\"coordinates\":[-0.1,51.5]}";

async fn put_place(db: &NodeDbLite<PagedbStorageMem>, id: &str, lng: f64, lat: f64) {
    let geom = Geometry::point(lng, lat);
    let mut doc = Document::new(id);
    doc.set(LOCATION, Value::Geometry(geom.clone()));
    db.document_put(PLACES, doc).await.expect("document_put");
    db.spatial_insert(PLACES, LOCATION, id, &geom);
}

fn dwithin_sql() -> String {
    format!("SELECT id FROM {PLACES} WHERE ST_DWithin({LOCATION}, '{NEAR_LONDON}', 50000)")
}

#[tokio::test]
async fn truncate_spatial_collection_empties_the_rtree() {
    let db = open_db().await;
    put_place(&db, "p1", -0.1278, 51.5074).await;
    put_place(&db, "p2", -0.09, 51.51).await;
    put_place(&db, "p3", -0.11, 51.49).await;
    assert_eq!(rows(&db, &dwithin_sql()).await.len(), 3);

    let result = run(&db, &format!("TRUNCATE {PLACES}")).await;
    assert_bare_truncate_tag(&result);
    assert!(rows(&db, &dwithin_sql()).await.is_empty());
    let world = BoundingBox::new(-180.0, -90.0, 180.0, 90.0);
    assert!(db.spatial_search_bbox(PLACES, LOCATION, &world).is_empty());

    put_place(&db, "p9", -0.12, 51.5).await;
    let after = rows(&db, &dwithin_sql()).await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0][0], Value::String("p9".into()));
    assert_eq!(db.spatial_search_bbox(PLACES, LOCATION, &world).len(), 1);
}

// ── Vector ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn truncate_vector_collection_empties_the_index() {
    let db = open_db().await;
    for (i, id) in ["v1", "v2", "v3"].iter().enumerate() {
        let mut meta = Document::new(*id);
        meta.set("n", Value::Integer(i as i64));
        db.vector_insert("vec_t", id, &[i as f32, 1.0, 0.0], Some(meta))
            .await
            .expect("vector_insert");
    }
    let query = [0.0_f32, 1.0, 0.0];
    assert_eq!(
        db.vector_search("vec_t", &query, 10, None, None)
            .await
            .expect("vector_search")
            .len(),
        3
    );

    let result = run(&db, "TRUNCATE vec_t").await;
    assert_bare_truncate_tag(&result);
    assert!(
        db.vector_search("vec_t", &query, 10, None, None)
            .await
            .expect("vector_search")
            .is_empty()
    );
    assert!(
        db.document_get("vec_t", "v1")
            .await
            .expect("document_get")
            .is_none()
    );

    db.vector_insert("vec_t", "v9", &[0.0, 1.0, 0.0], None)
        .await
        .expect("vector_insert");
    let after = db
        .vector_search("vec_t", &query, 10, None, None)
        .await
        .expect("vector_search");
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].id, "v9");
}

// ── Array ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn truncate_array_is_refused_naming_drop_array() {
    let db = open_db().await;
    run(
        &db,
        "CREATE ARRAY grid_t DIMS (x INT64 [0..63]) ATTRS (v INT64) TILE_EXTENTS (8)",
    )
    .await;
    let err = db
        .execute_sql("TRUNCATE grid_t", &[])
        .await
        .expect_err("TRUNCATE on an array is refused");
    let msg = err.to_string();
    assert!(msg.contains("DROP ARRAY"), "{msg}");
    assert!(msg.contains("DELETE FROM ARRAY"), "{msg}");
}

// ── RESTART IDENTITY ─────────────────────────────────────────────────────────

#[tokio::test]
async fn truncate_restart_identity_restarts_serial() {
    let db = open_db().await;
    run(&db, "CREATE COLLECTION seq_t").await;
    db.sequences().register(LiteSequenceDef {
        name: "seq_t_id_seq".into(),
        start_value: 1,
        increment: 1,
        min_value: 1,
        max_value: i64::MAX,
        cycle: false,
    });
    db.sequences().register(LiteSequenceDef {
        name: "unrelated_id_seq".into(),
        start_value: 1,
        increment: 1,
        min_value: 1,
        max_value: i64::MAX,
        cycle: false,
    });
    for i in 1..=3 {
        let id = db.sequences().nextval("seq_t_id_seq").expect("nextval");
        db.sequences().nextval("unrelated_id_seq").expect("nextval");
        run(
            &db,
            &format!("INSERT INTO seq_t (id, n) VALUES ('r{id}', {i})"),
        )
        .await;
    }

    let plain = run(&db, "TRUNCATE seq_t").await;
    assert_bare_truncate_tag(&plain);
    assert_eq!(
        db.sequences().nextval("seq_t_id_seq").expect("nextval"),
        4,
        "a plain TRUNCATE leaves the sequence alone"
    );

    let restarted = run(&db, "TRUNCATE seq_t RESTART IDENTITY").await;
    assert_bare_truncate_tag(&restarted);
    assert_eq!(
        db.sequences().nextval("seq_t_id_seq").expect("nextval"),
        1,
        "RESTART IDENTITY makes the start value the next value"
    );
    assert_eq!(
        db.sequences().nextval("unrelated_id_seq").expect("nextval"),
        4,
        "another collection's sequence is untouched"
    );
}

// ── Multiple targets ─────────────────────────────────────────────────────────

#[tokio::test]
async fn truncate_several_collections_in_one_statement() {
    let db = open_db().await;
    run(&db, "CREATE COLLECTION multi_a").await;
    run(&db, "CREATE COLLECTION multi_b").await;
    run(&db, "INSERT INTO multi_a (id) VALUES ('a1')").await;
    run(&db, "INSERT INTO multi_b (id) VALUES ('b1')").await;

    let result = run(&db, "TRUNCATE multi_a, multi_b").await;
    assert_bare_truncate_tag(&result);
    assert!(rows(&db, "SELECT id FROM multi_a").await.is_empty());
    assert!(rows(&db, "SELECT id FROM multi_b").await.is_empty());
}
