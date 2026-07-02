//! SqlCatalog implementation for Lite.
//!
//! Resolves collection metadata from the CRDT, strict, and columnar engines.

use std::sync::{Arc, Mutex};

use nodedb_sql::types::*;

use crate::engine::columnar::ColumnarEngine;
use crate::engine::crdt::CrdtEngine;
use crate::engine::strict::StrictEngine;
use crate::storage::engine::StorageEngine;

/// Catalog adapter for Lite that resolves collections from local engines.
pub struct LiteCatalog<S: StorageEngine> {
    crdt: Arc<Mutex<CrdtEngine>>,
    strict: Arc<StrictEngine<S>>,
    columnar: Arc<ColumnarEngine<S>>,
}

impl<S: StorageEngine> LiteCatalog<S> {
    pub fn new(
        crdt: Arc<Mutex<CrdtEngine>>,
        strict: Arc<StrictEngine<S>>,
        columnar: Arc<ColumnarEngine<S>>,
    ) -> Self {
        Self {
            crdt,
            strict,
            columnar,
        }
    }
}

impl<S: StorageEngine> SqlCatalog for LiteCatalog<S> {
    fn get_collection(
        &self,
        _database_id: nodedb_types::id::DatabaseId,
        name: &str,
    ) -> Result<Option<CollectionInfo>, nodedb_sql::catalog::SqlCatalogError> {
        // Check strict collections first.
        if let Some(schema) = self.strict.schema(name) {
            let columns = schema
                .columns
                .iter()
                .map(|c| ColumnInfo {
                    name: c.name.clone(),
                    data_type: convert_column_type(&c.column_type),
                    nullable: c.nullable,
                    is_primary_key: c.primary_key,
                    default: c.default.clone(),
                    raw_type: Some(format!("{:?}", c.column_type)),
                })
                .collect();
            let pk = schema
                .columns
                .iter()
                .find(|c| c.primary_key)
                .map(|c| c.name.clone());
            return Ok(Some(CollectionInfo {
                name: name.into(),
                engine: EngineType::DocumentStrict,
                columns,
                primary_key: pk,
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::default(),
            }));
        }

        // Check columnar collections.
        if self.columnar.schema(name).is_some() {
            return Ok(Some(CollectionInfo {
                name: name.into(),
                engine: EngineType::Columnar,
                columns: Vec::new(),
                primary_key: None,
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::default(),
            }));
        }

        // Check CRDT (schemaless) collections.
        if let Ok(crdt) = self.crdt.lock()
            && crdt.collection_names().iter().any(|n| n == name)
        {
            return Ok(Some(CollectionInfo {
                name: name.into(),
                engine: EngineType::DocumentSchemaless,
                columns: vec![ColumnInfo {
                    name: "id".into(),
                    data_type: SqlDataType::String,
                    nullable: false,
                    is_primary_key: true,
                    default: None,
                    raw_type: None,
                }],
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::default(),
            }));
        }

        Ok(None)
    }
}

fn convert_column_type(ct: &nodedb_types::columnar::ColumnType) -> SqlDataType {
    use nodedb_types::columnar::ColumnType;
    match ct {
        ColumnType::Int64 => SqlDataType::Int64,
        ColumnType::Float64 => SqlDataType::Float64,
        ColumnType::String => SqlDataType::String,
        ColumnType::Bool => SqlDataType::Bool,
        ColumnType::Bytes | ColumnType::Geometry | ColumnType::Json => SqlDataType::Bytes,
        ColumnType::Timestamp | ColumnType::SystemTimestamp => SqlDataType::Timestamp,
        ColumnType::Decimal { .. } | ColumnType::Uuid | ColumnType::Ulid | ColumnType::Regex => {
            SqlDataType::String
        }
        ColumnType::Duration => SqlDataType::Int64,
        ColumnType::Array | ColumnType::Set | ColumnType::Range | ColumnType::Record => {
            SqlDataType::Bytes
        }
        ColumnType::Vector(dim) => SqlDataType::Vector(*dim as usize),
        _ => SqlDataType::Bytes,
    }
}
