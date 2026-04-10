//! DDL handlers for CONVERT COLLECTION between storage modes.
//!
//! - CONVERT COLLECTION <name> TO strict (<col_defs>)
//! - CONVERT COLLECTION <name> TO columnar (<col_defs>)
//! - CONVERT COLLECTION <name> TO document

use nodedb_types::columnar::{
    ColumnDef, ColumnType, ColumnarProfile, ColumnarSchema, StrictSchema,
};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// Bridge async storage operations from sync DDL context.
fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

use super::parser::parse_strict_create_sql;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Handle: CONVERT COLLECTION <name> TO strict (<col_defs>)
    ///
    /// Reads all schemaless documents from the CRDT engine, validates each
    /// against the target schema, and writes as Binary Tuples in the strict engine.
    /// The original schemaless collection is dropped after successful conversion.
    pub(in crate::query) async fn handle_convert_to_strict(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let (source_name, target_schema) = parse_convert_sql(sql, "strict")?;

        // Read all documents from the source (CRDT/schemaless).
        let docs = {
            let crdt = match self.crdt.lock() {
                Ok(c) => c,
                Err(p) => p.into_inner(),
            };
            let ids = crdt.list_ids(&source_name);
            let mut docs = Vec::with_capacity(ids.len());
            for id in &ids {
                if let Some(loro_val) = crdt.read(&source_name, id) {
                    let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                    docs.push(doc);
                }
            }
            docs
        };

        if docs.is_empty() {
            return Err(LiteError::Query(format!(
                "collection '{source_name}' is empty or does not exist"
            )));
        }

        // Create the strict collection.
        {
            let mut strict = match self.strict.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            block_on(strict.create_collection(&source_name, target_schema.clone()))?;
        }

        // Convert each document to a row and insert.
        let mut converted = 0u64;
        {
            let strict = match self.strict.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            for doc in &docs {
                let values = document_to_row(&doc.fields, &target_schema.columns);
                match block_on(strict.insert(&source_name, &values)) {
                    Ok(()) => converted += 1,
                    Err(e) => {
                        tracing::warn!(doc_id = %doc.id, error = %e, "conversion insert failed")
                    }
                }
            }
        }

        // Drop the old schemaless collection from CRDT.
        {
            let mut crdt = match self.crdt.lock() {
                Ok(c) => c,
                Err(p) => p.into_inner(),
            };
            for doc in &docs {
                let _ = crdt.delete(&source_name, &doc.id);
            }
        }

        self.register_strict_collection(&source_name);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "converted {converted} documents to strict '{source_name}'"
            ))]],
            rows_affected: converted,
        })
    }

    /// Handle: CONVERT COLLECTION <name> TO columnar (<col_defs>)
    pub(in crate::query) async fn handle_convert_to_columnar(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let (source_name, target_schema) = parse_convert_sql(sql, "columnar")?;
        let columnar_schema = ColumnarSchema::new(target_schema.columns)
            .map_err(|e| LiteError::Query(e.to_string()))?;

        // Read from CRDT or strict.
        let rows = self.read_source_rows(&source_name, &columnar_schema.columns)?;

        // Create columnar collection.
        {
            let mut columnar = match self.columnar.lock() {
                Ok(c) => c,
                Err(p) => p.into_inner(),
            };
            block_on(columnar.create_collection(
                &source_name,
                columnar_schema,
                ColumnarProfile::Plain,
            ))?;
        }

        // Insert rows.
        let mut converted = 0u64;
        {
            let mut columnar = match self.columnar.lock() {
                Ok(c) => c,
                Err(p) => p.into_inner(),
            };
            for row in &rows {
                if columnar.insert(&source_name, row).is_ok() {
                    converted += 1;
                }
            }
        }

        self.register_columnar_collection(&source_name);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "converted {converted} rows to columnar '{source_name}'"
            ))]],
            rows_affected: converted,
        })
    }

    /// Handle: CONVERT COLLECTION <name> TO document
    ///
    /// Reads from strict or columnar, writes as schemaless MessagePack documents.
    pub(in crate::query) async fn handle_convert_to_document(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let parts: Vec<&str> = sql.split_whitespace().collect();
        let source_name = parts
            .get(2)
            .ok_or(LiteError::Query("expected collection name".into()))?
            .to_lowercase();

        // Read from strict or columnar.
        let is_strict = {
            let strict = match self.strict.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            strict.schema(&source_name).is_some()
        };

        let mut converted = 0u64;

        if is_strict {
            let strict = match self.strict.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            let schema = strict
                .schema(&source_name)
                .cloned()
                .ok_or(LiteError::Query("strict collection not found".into()))?;

            let raw = block_on(strict.scan_raw(&source_name))?;

            let decoder = nodedb_strict::TupleDecoder::new(&schema);
            let mut crdt = match self.crdt.lock() {
                Ok(c) => c,
                Err(p) => p.into_inner(),
            };

            for tuple_bytes in &raw {
                if let Ok(values) = decoder.extract_all(tuple_bytes) {
                    let doc_id = nodedb_types::id_gen::uuid_v7();
                    let fields: Vec<(&str, loro::LoroValue)> = schema
                        .columns
                        .iter()
                        .zip(values.iter())
                        .map(|(col, val)| (col.name.as_str(), value_to_loro(val)))
                        .collect();
                    if crdt.upsert(&source_name, &doc_id, &fields).is_ok() {
                        converted += 1;
                    }
                }
            }

            drop(crdt);
            drop(strict);

            // Drop the strict collection.
            let mut strict = match self.strict.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            block_on(strict.drop_collection(&source_name))?;
        }

        self.register_collection(&source_name);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "converted {converted} rows to document '{source_name}'"
            ))]],
            rows_affected: converted,
        })
    }

    /// Read rows from any source (CRDT or strict) as Vec<Value>.
    fn read_source_rows(
        &self,
        collection: &str,
        target_columns: &[ColumnDef],
    ) -> Result<Vec<Vec<Value>>, LiteError> {
        // Try CRDT first.
        let crdt = match self.crdt.lock() {
            Ok(c) => c,
            Err(p) => p.into_inner(),
        };
        let ids = crdt.list_ids(collection);
        if !ids.is_empty() {
            let mut rows = Vec::with_capacity(ids.len());
            for id in &ids {
                if let Some(loro_val) = crdt.read(collection, id) {
                    let doc = crate::nodedb::convert::loro_value_to_document(id, &loro_val);
                    rows.push(document_to_row(&doc.fields, target_columns));
                }
            }
            return Ok(rows);
        }
        drop(crdt);

        // Try strict.
        let strict = match self.strict.lock() {
            Ok(s) => s,
            Err(p) => p.into_inner(),
        };
        if strict.schema(collection).is_some() {
            let schema = strict
                .schema(collection)
                .cloned()
                .ok_or(LiteError::Query("collection not found".into()))?;
            let raw = block_on(strict.scan_raw(collection))?;
            let decoder = nodedb_strict::TupleDecoder::new(&schema);
            let mut rows = Vec::with_capacity(raw.len());
            for tuple_bytes in &raw {
                if let Ok(values) = decoder.extract_all(tuple_bytes) {
                    rows.push(values);
                }
            }
            return Ok(rows);
        }

        Err(LiteError::Query(format!(
            "collection '{collection}' not found in any storage mode"
        )))
    }
}

/// Parse CONVERT COLLECTION <name> TO <mode> [(<col_defs>)]
fn parse_convert_sql(sql: &str, target_mode: &str) -> Result<(String, StrictSchema), LiteError> {
    let parts: Vec<&str> = sql.split_whitespace().collect();
    let source_name = parts
        .get(2)
        .ok_or(LiteError::Query("expected collection name".into()))?
        .to_lowercase();

    // Validate that the SQL TO clause matches the expected target mode.
    if let Some(to_idx) = parts.iter().position(|p| p.eq_ignore_ascii_case("TO"))
        && let Some(mode) = parts.get(to_idx + 1)
        && !mode.eq_ignore_ascii_case(target_mode)
    {
        return Err(LiteError::Query(format!(
            "expected CONVERT TO {target_mode}, got '{mode}'"
        )));
    }

    // If there are column defs in parens, parse them.
    if sql.contains('(') {
        let (_, schema) = parse_strict_create_sql(sql)?;
        Ok((source_name, schema))
    } else {
        // No schema specified — infer from source. Use a minimal default.
        Ok((
            source_name,
            StrictSchema {
                columns: vec![
                    ColumnDef::required("id", ColumnType::String).with_primary_key(),
                    ColumnDef::nullable("data", ColumnType::String),
                ],
                version: 1,
            },
        ))
    }
}

/// Convert a Document's fields to a Vec<Value> matching the target schema.
fn document_to_row(
    fields: &std::collections::HashMap<String, Value>,
    target_columns: &[ColumnDef],
) -> Vec<Value> {
    target_columns
        .iter()
        .map(|col| fields.get(&col.name).cloned().unwrap_or(Value::Null))
        .collect()
}

// Re-use the crate-wide value_to_loro conversion.
use crate::nodedb::convert::value_to_loro;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_convert_with_schema() {
        let sql = "CONVERT COLLECTION users TO strict (id BIGINT NOT NULL PRIMARY KEY, name TEXT)";
        let (name, schema) = parse_convert_sql(sql, "strict").expect("parse");
        assert_eq!(name, "users");
        assert_eq!(schema.columns.len(), 2);
    }

    #[test]
    fn parse_convert_without_schema() {
        let sql = "CONVERT COLLECTION users TO document";
        let (name, _schema) = parse_convert_sql(sql, "document").expect("parse");
        assert_eq!(name, "users");
    }

    #[test]
    fn document_to_row_maps_fields() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("name".into(), Value::String("Alice".into()));
        fields.insert("age".into(), Value::Integer(30));

        let columns = vec![
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::nullable("age", ColumnType::Int64),
            ColumnDef::nullable("email", ColumnType::String),
        ];

        let row = document_to_row(&fields, &columns);
        assert_eq!(row.len(), 3);
        assert_eq!(row[0], Value::String("Alice".into()));
        assert_eq!(row[1], Value::Integer(30));
        assert_eq!(row[2], Value::Null); // email not in doc.
    }
}
