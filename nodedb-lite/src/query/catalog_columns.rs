// SPDX-License-Identifier: Apache-2.0

//! Column metadata the SQL catalog builds from engine schemas, persisted
//! collection configs and descriptor field hints.

use nodedb_sql::types::*;
use nodedb_types::columnar::{ColumnarSchema, FloatWidth, IntWidth, StrictSchema};

/// Build column metadata + primary key from a slice of `ColumnDef`.
///
/// Shared by `StrictSchema`/`KvConfig::schema` and `ColumnarSchema`, which
/// both carry `Vec<ColumnDef>` with identical (name, column_type, nullable,
/// default, primary_key) shape.
///
/// The declared numeric width comes from the typed `ColumnDef`, so the write
/// path (range validation) and the read path (OID and binary payload width)
/// read the same width the engine enforces.
fn columns_from_column_defs(
    columns: &[nodedb_types::columnar::ColumnDef],
) -> (Vec<ColumnInfo>, Option<String>) {
    let cols = columns
        .iter()
        .map(|c| ColumnInfo {
            name: c.name.clone(),
            data_type: convert_column_type(&c.column_type),
            nullable: c.nullable,
            is_primary_key: c.primary_key,
            default: c.default.clone(),
            raw_type: None,
            int_width: c.int_width,
            float_width: c.float_width,
        })
        .collect();
    let pk = columns
        .iter()
        .find(|c| c.primary_key)
        .map(|c| c.name.clone());
    (cols, pk)
}

/// Build column metadata + primary key from a strict/KV schema.
pub(super) fn columns_from_strict_schema(
    schema: &StrictSchema,
) -> (Vec<ColumnInfo>, Option<String>) {
    columns_from_column_defs(&schema.columns)
}

/// Build column metadata + primary key from a live `ColumnarSchema`.
///
/// This is the schema the columnar engine actually encodes rows against —
/// the timeseries/spatial INSERT planner needs these exact columns, not the
/// descriptor's field hints.
pub(super) fn columns_from_columnar_schema(
    schema: &ColumnarSchema,
) -> (Vec<ColumnInfo>, Option<String>) {
    columns_from_column_defs(&schema.columns)
}

/// Build column metadata from `(name, type_hint)` descriptor field pairs.
///
/// The hint is the declared type text. It resolves to the SQL type a
/// schemaless or columnar-family column advertises, and it stays on the
/// column as `raw_type` with the width it declares.
pub(super) fn columns_from_fields(fields: &[(String, String)]) -> Vec<ColumnInfo> {
    fields
        .iter()
        .map(|(fname, type_hint)| ColumnInfo {
            name: fname.clone(),
            data_type: declared_sql_type(type_hint),
            nullable: true,
            is_primary_key: false,
            default: None,
            int_width: IntWidth::from_declared_type(type_hint),
            float_width: FloatWidth::from_declared_type(type_hint),
            raw_type: Some(type_hint.clone()),
        })
        .collect()
}

pub(super) fn parse_strict_config(config_json: Option<&str>) -> Option<StrictSchema> {
    config_json.and_then(|s| sonic_rs::from_str::<StrictSchema>(s).ok())
}

pub(super) fn parse_kv_config(config_json: Option<&str>) -> Option<nodedb_types::KvConfig> {
    config_json.and_then(|s| sonic_rs::from_str::<nodedb_types::KvConfig>(s).ok())
}

fn convert_column_type(ct: &nodedb_types::columnar::ColumnType) -> SqlDataType {
    use nodedb_types::columnar::ColumnType;
    match ct {
        ColumnType::Int64 => SqlDataType::Int64,
        ColumnType::Float64 => SqlDataType::Float64,
        ColumnType::String => SqlDataType::String,
        ColumnType::Bool => SqlDataType::Bool,
        ColumnType::Bytes => SqlDataType::Bytes,
        // A structured column reads back as its JSON text.
        ColumnType::Json
        | ColumnType::Array
        | ColumnType::Set
        | ColumnType::Range
        | ColumnType::Record => SqlDataType::Json,
        ColumnType::Geometry => SqlDataType::Geometry,
        ColumnType::Timestamp | ColumnType::SystemTimestamp => SqlDataType::Timestamp,
        ColumnType::Timestamptz => SqlDataType::Timestamptz,
        ColumnType::Decimal(typmod) => SqlDataType::Decimal(*typmod),
        ColumnType::Uuid => SqlDataType::Uuid,
        ColumnType::Ulid | ColumnType::Regex | ColumnType::SparseVector => SqlDataType::String,
        ColumnType::Duration => SqlDataType::Int64,
        ColumnType::Vector(dim) => SqlDataType::Vector(*dim as usize),
        // `ColumnType` is non-exhaustive; an unknown type reads as bytes.
        _ => SqlDataType::Bytes,
    }
}

/// The SQL type a declared `fields` hint advertises.
///
/// A hint that names no known type resolves to `String`, the widest
/// rendering a catalog column can fall back to.
fn declared_sql_type(type_hint: &str) -> SqlDataType {
    match nodedb_types::columnar::ColumnType::from_declared_type(type_hint) {
        Some(declared) => declared_column_type_to_sql(declared),
        None => SqlDataType::String,
    }
}

/// Map a declared column type onto the SQL type a `fields` column advertises.
///
/// A schemaless or columnar-family column stores the types below as the text
/// the client wrote, not in the strict binary encoding, so they render as
/// text or as a naive timestamp. Every other type defers to
/// [`convert_column_type`].
fn declared_column_type_to_sql(declared: nodedb_types::columnar::ColumnType) -> SqlDataType {
    use nodedb_types::columnar::ColumnType;
    match declared {
        ColumnType::Timestamptz => SqlDataType::Timestamp,
        ColumnType::SystemTimestamp => SqlDataType::String,
        ColumnType::Geometry
        | ColumnType::Json
        | ColumnType::Vector(_)
        | ColumnType::Duration
        | ColumnType::Array
        | ColumnType::Set
        | ColumnType::Range
        | ColumnType::Record => SqlDataType::String,
        other => convert_column_type(&other),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::columnar::{ColumnDef, ColumnType, DecimalTypmod};

    use super::*;

    #[test]
    fn typed_columns_carry_declared_widths_and_no_raw_type() {
        let columns = vec![
            ColumnDef::nullable("s", ColumnType::Int64).with_declared_width("SMALLINT"),
            ColumnDef::nullable("r", ColumnType::Float64).with_declared_width("REAL"),
        ];
        let (infos, _) = columns_from_column_defs(&columns);
        assert_eq!(infos[0].int_width, Some(IntWidth::I16));
        assert_eq!(infos[1].float_width, Some(FloatWidth::F32));
        assert!(infos.iter().all(|c| c.raw_type.is_none()));
    }

    #[test]
    fn typed_columns_keep_decimal_uuid_and_timestamptz() {
        let typmod = DecimalTypmod::new(5, 2).expect("valid typmod");
        assert_eq!(
            convert_column_type(&ColumnType::Decimal(Some(typmod))),
            SqlDataType::Decimal(Some(typmod))
        );
        assert_eq!(convert_column_type(&ColumnType::Uuid), SqlDataType::Uuid);
        assert_eq!(
            convert_column_type(&ColumnType::Timestamptz),
            SqlDataType::Timestamptz
        );
        assert_eq!(convert_column_type(&ColumnType::Json), SqlDataType::Json);
        assert_eq!(
            convert_column_type(&ColumnType::Geometry),
            SqlDataType::Geometry
        );
    }

    #[test]
    fn declared_hints_keep_typmod_width_and_raw_text() {
        let typmod = DecimalTypmod::new(5, 2).expect("valid typmod");
        let infos = columns_from_fields(&[
            ("d".to_string(), "DECIMAL(5, 2)".to_string()),
            ("s".to_string(), "SMALLINT".to_string()),
            ("t".to_string(), "TIMESTAMPTZ".to_string()),
            ("x".to_string(), "SOMETHING_ELSE".to_string()),
        ]);
        assert_eq!(infos[0].data_type, SqlDataType::Decimal(Some(typmod)));
        assert_eq!(infos[1].int_width, Some(IntWidth::I16));
        assert_eq!(infos[2].data_type, SqlDataType::Timestamp);
        assert_eq!(infos[3].data_type, SqlDataType::String);
        assert_eq!(infos[0].raw_type.as_deref(), Some("DECIMAL(5, 2)"));
    }
}
