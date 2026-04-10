//! Tests for DDL parsing and related utilities.

use nodedb_types::columnar::{ColumnType, ColumnTypeParseError};

use super::parser::{parse_column_def, parse_strict_create_sql};

// -- DDL golden tests --

#[test]
fn parse_strict_create_basic() {
    let sql = "CREATE COLLECTION customers (
        id BIGINT NOT NULL PRIMARY KEY,
        name TEXT NOT NULL,
        email TEXT,
        balance DECIMAL NOT NULL DEFAULT 0
    ) WITH storage = 'strict'";

    let (name, schema) = parse_strict_create_sql(sql).expect("parse");
    assert_eq!(name, "customers");
    assert_eq!(schema.columns.len(), 4);
    assert_eq!(schema.columns[0].name, "id");
    assert!(schema.columns[0].primary_key);
    assert!(!schema.columns[0].nullable);
    assert_eq!(schema.columns[1].name, "name");
    assert!(!schema.columns[1].nullable);
    assert_eq!(schema.columns[2].name, "email");
    assert!(schema.columns[2].nullable);
    assert_eq!(schema.columns[3].name, "balance");
    assert!(schema.columns[3].default.is_some());
    assert_eq!(schema.version, 1);
}

#[test]
fn parse_columnar_create() {
    let sql = "CREATE COLLECTION buildings (
        id BIGINT NOT NULL PRIMARY KEY,
        name TEXT,
        height FLOAT64
    ) WITH storage = 'columnar'";

    let (name, schema) = parse_strict_create_sql(sql).expect("parse");
    assert_eq!(name, "buildings");
    assert_eq!(schema.columns.len(), 3);
    assert!(schema.columns[0].primary_key);
}

#[test]
fn parse_vector_column() {
    let sql = "CREATE COLLECTION embeddings (
        id BIGINT NOT NULL PRIMARY KEY,
        emb VECTOR(768) NOT NULL
    ) WITH storage = 'strict'";

    let (_, schema) = parse_strict_create_sql(sql).expect("parse");
    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[1].column_type, ColumnType::Vector(768));
}

#[test]
fn parse_auto_rowid_when_no_pk() {
    let sql = "CREATE COLLECTION items (
        name TEXT NOT NULL,
        value FLOAT64
    ) WITH storage = 'strict'";

    let (_, schema) = parse_strict_create_sql(sql).expect("parse");
    // Should auto-generate _rowid PK.
    assert_eq!(schema.columns[0].name, "_rowid");
    assert!(schema.columns[0].primary_key);
    assert_eq!(schema.columns[0].column_type, ColumnType::Int64);
    assert_eq!(schema.columns.len(), 3); // _rowid + name + value
}

#[test]
fn parse_empty_columns_rejected() {
    let sql = "CREATE COLLECTION empty () WITH storage = 'strict'";
    assert!(parse_strict_create_sql(sql).is_err());
}

// -- Type vocabulary enforcement tests --

#[test]
fn type_vocabulary_canonical() {
    // All canonical types parse correctly.
    assert_eq!("BIGINT".parse::<ColumnType>().unwrap(), ColumnType::Int64);
    assert_eq!(
        "FLOAT64".parse::<ColumnType>().unwrap(),
        ColumnType::Float64
    );
    assert_eq!("TEXT".parse::<ColumnType>().unwrap(), ColumnType::String);
    assert_eq!("BOOL".parse::<ColumnType>().unwrap(), ColumnType::Bool);
    assert_eq!("BYTES".parse::<ColumnType>().unwrap(), ColumnType::Bytes);
    assert_eq!(
        "TIMESTAMP".parse::<ColumnType>().unwrap(),
        ColumnType::Timestamp
    );
    assert_eq!(
        "DECIMAL".parse::<ColumnType>().unwrap(),
        ColumnType::Decimal
    );
    assert_eq!(
        "GEOMETRY".parse::<ColumnType>().unwrap(),
        ColumnType::Geometry
    );
    assert_eq!("UUID".parse::<ColumnType>().unwrap(), ColumnType::Uuid);
    assert_eq!(
        "VECTOR(128)".parse::<ColumnType>().unwrap(),
        ColumnType::Vector(128)
    );
}

#[test]
fn type_vocabulary_datetime_rejected() {
    assert!(matches!(
        "DATETIME".parse::<ColumnType>(),
        Err(ColumnTypeParseError::UseTimestamp)
    ));
}

#[test]
fn type_vocabulary_unknown_rejected() {
    assert!(matches!(
        "FOOBAR".parse::<ColumnType>(),
        Err(ColumnTypeParseError::Unknown(_))
    ));
}

#[test]
fn type_vocabulary_aliases() {
    // Common aliases are accepted.
    assert_eq!("INTEGER".parse::<ColumnType>().unwrap(), ColumnType::Int64);
    assert_eq!("INT".parse::<ColumnType>().unwrap(), ColumnType::Int64);
    assert_eq!("VARCHAR".parse::<ColumnType>().unwrap(), ColumnType::String);
    assert_eq!("BOOLEAN".parse::<ColumnType>().unwrap(), ColumnType::Bool);
    assert_eq!(
        "NUMERIC".parse::<ColumnType>().unwrap(),
        ColumnType::Decimal
    );
}

// -- Column def parser tests --

#[test]
fn parse_column_required() {
    let col = parse_column_def("id BIGINT NOT NULL PRIMARY KEY").expect("parse");
    assert_eq!(col.name, "id");
    assert_eq!(col.column_type, ColumnType::Int64);
    assert!(!col.nullable);
    assert!(col.primary_key);
}

#[test]
fn parse_column_nullable() {
    let col = parse_column_def("email TEXT").expect("parse");
    assert_eq!(col.name, "email");
    assert_eq!(col.column_type, ColumnType::String);
    assert!(col.nullable);
    assert!(!col.primary_key);
}

#[test]
fn parse_column_with_default() {
    let col = parse_column_def("status TEXT NOT NULL DEFAULT 'active'").expect("parse");
    assert_eq!(col.name, "status");
    assert!(!col.nullable);
    assert_eq!(col.default.as_deref(), Some("'active'"));
}

#[test]
fn parse_column_vector_type() {
    let col = parse_column_def("emb VECTOR(768) NOT NULL").expect("parse");
    assert_eq!(col.column_type, ColumnType::Vector(768));
    assert!(!col.nullable);
}
