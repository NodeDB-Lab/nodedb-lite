//! Declared column types on a strict collection are enforced by the SQL
//! planner: integer width, `REAL` range, `DECIMAL(p,s)` fitting, `UUID` text,
//! `TIMESTAMPTZ` instants and numeric `VECTOR(dim)` elements.
//!
//! Run with:
//!   cargo nextest run -p nodedb-lite --test sql_declared_types

pub mod common;

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::error::ErrorCode;
use nodedb_types::value::Value;

use crate::common::sql::open_lite;

const CREATE: &str = "CREATE COLLECTION declared_types (
    id BIGINT NOT NULL PRIMARY KEY,
    small SMALLINT,
    medium INT,
    single REAL,
    price DECIMAL(5,2),
    ref_id UUID,
    seen_at TIMESTAMPTZ
) WITH storage = 'strict'";

async fn open_declared() -> Arc<NodeDbLite<PagedbStorageMem>> {
    let db = open_lite().await;
    db.execute_sql(CREATE, &[])
        .await
        .expect("create declared_types");
    db
}

async fn insert_err(db: &Arc<NodeDbLite<PagedbStorageMem>>, column: &str, literal: &str) -> String {
    let sql = format!("INSERT INTO declared_types (id, {column}) VALUES (1, {literal})");
    db.execute_sql(&sql, &[]).await.expect_err(&sql).to_string()
}

async fn read_cell(db: &Arc<NodeDbLite<PagedbStorageMem>>, column: &str) -> Value {
    let sql = format!("SELECT {column} FROM declared_types WHERE id = 1");
    let result = db.execute_sql(&sql, &[]).await.expect(&sql);
    assert_eq!(result.rows.len(), 1, "{sql}: one row");
    result.rows[0][0].clone()
}

fn text_of(value: &Value) -> String {
    match value {
        Value::Decimal(d) => d.to_string(),
        Value::String(s) | Value::Uuid(s) => s.clone(),
        Value::DateTime(at) | Value::NaiveDateTime(at) => at.to_iso8601(),
        Value::Float(f) => f.to_string(),
        Value::Integer(i) => i.to_string(),
        other => panic!("unexpected cell {other:?}"),
    }
}

#[tokio::test]
async fn smallint_column_refuses_a_value_past_sixteen_bits() {
    let db = open_declared().await;
    let message = insert_err(&db, "small", "100000").await;
    assert!(message.contains("out of range"), "{message}");
    db.execute_sql(
        "INSERT INTO declared_types (id, small) VALUES (1, 32767)",
        &[],
    )
    .await
    .expect("32767 fits SMALLINT");
    assert_eq!(read_cell(&db, "small").await, Value::Integer(32767));
}

#[tokio::test]
async fn int_column_refuses_two_to_the_fortieth() {
    let db = open_declared().await;
    let message = insert_err(&db, "medium", "1099511627776").await;
    assert!(message.contains("out of range"), "{message}");
    db.execute_sql(
        "INSERT INTO declared_types (id, medium) VALUES (1, 2147483647)",
        &[],
    )
    .await
    .expect("i32::MAX fits INT");
    assert_eq!(
        read_cell(&db, "medium").await,
        Value::Integer(2_147_483_647)
    );
}

#[tokio::test]
async fn real_column_stores_values_within_f32_bounds_and_refuses_past_them() {
    let db = open_declared().await;
    let message = insert_err(&db, "single", "1e39").await;
    assert!(message.contains("out of range"), "{message}");
    db.execute_sql(
        "INSERT INTO declared_types (id, single) VALUES (1, 3.0e38)",
        &[],
    )
    .await
    .expect("3.0e38 fits REAL");
    assert_eq!(read_cell(&db, "single").await, Value::Float(3.0e38));
}

#[tokio::test]
async fn decimal_column_rounds_to_its_scale() {
    let db = open_declared().await;
    db.execute_sql(
        "INSERT INTO declared_types (id, price) VALUES (1, 1.005)",
        &[],
    )
    .await
    .expect("insert 1.005");
    assert_eq!(text_of(&read_cell(&db, "price").await), "1.01");
}

#[tokio::test]
async fn decimal_column_refuses_an_integer_part_past_its_precision() {
    let db = open_declared().await;
    let message = insert_err(&db, "price", "1000.00").await;
    assert!(
        message.contains("DECIMAL") || message.contains("decimal"),
        "{message}"
    );
    let message = insert_err(&db, "price", "123456.789").await;
    assert!(
        message.contains("DECIMAL") || message.contains("decimal"),
        "{message}"
    );
}

#[tokio::test]
async fn uuid_column_accepts_a_valid_uuid_and_refuses_an_invalid_one() {
    let db = open_declared().await;
    let uuid = "67e55044-10b1-426f-9247-bb680e5fe0c8";
    db.execute_sql(
        &format!("INSERT INTO declared_types (id, ref_id) VALUES (1, '{uuid}')"),
        &[],
    )
    .await
    .expect("valid uuid");
    assert_eq!(text_of(&read_cell(&db, "ref_id").await), uuid);

    let db = open_declared().await;
    insert_err(&db, "ref_id", "'not-a-uuid'").await;
}

#[tokio::test]
async fn timestamptz_column_round_trips_an_instant() {
    let db = open_declared().await;
    db.execute_sql(
        "INSERT INTO declared_types (id, seen_at) VALUES (1, '2024-01-01T00:00:00Z')",
        &[],
    )
    .await
    .expect("insert timestamptz");
    let text = text_of(&read_cell(&db, "seen_at").await);
    assert!(text.starts_with("2024-01-01T00:00:00"), "{text}");
}

#[tokio::test]
async fn a_text_element_in_a_vector_literal_is_a_type_mismatch() {
    let db = open_lite().await;
    db.execute_sql(
        "CREATE COLLECTION declared_vectors (
            id BIGINT NOT NULL PRIMARY KEY,
            embedding VECTOR(3)
        ) WITH storage = 'strict'",
        &[],
    )
    .await
    .expect("create declared_vectors");
    let err = db
        .execute_sql(
            "INSERT INTO declared_vectors (id, embedding) VALUES (1, ARRAY['x', 0.0, 1.0])",
            &[],
        )
        .await
        .expect_err("a text element is not a VECTOR element");
    assert_eq!(err.code(), ErrorCode::TYPE_MISMATCH, "{err}");
}
