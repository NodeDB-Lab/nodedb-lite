//! Tests for the strict document engine.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_types::Namespace;
use nodedb_types::columnar::{ColumnDef, ColumnType};
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::engine::StrictEngine;

/// In-memory storage for tests.
struct MemStorage {
    data: tokio::sync::Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

impl MemStorage {
    fn new() -> Self {
        Self {
            data: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    fn make_key(ns: Namespace, key: &[u8]) -> Vec<u8> {
        let mut k = Vec::with_capacity(1 + key.len());
        k.push(ns as u8);
        k.extend_from_slice(key);
        k
    }
}

#[async_trait::async_trait]
impl StorageEngine for MemStorage {
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        let data = self.data.lock().await;
        Ok(data.get(&Self::make_key(ns, key)).cloned())
    }

    async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
        let mut data = self.data.lock().await;
        data.insert(Self::make_key(ns, key), value.to_vec());
        Ok(())
    }

    async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
        let mut data = self.data.lock().await;
        data.remove(&Self::make_key(ns, key));
        Ok(())
    }

    async fn scan_prefix(
        &self,
        ns: Namespace,
        prefix: &[u8],
    ) -> Result<Vec<crate::storage::engine::KvPair>, LiteError> {
        let data = self.data.lock().await;
        let ns_byte = ns as u8;
        let mut full_prefix = vec![ns_byte];
        full_prefix.extend_from_slice(prefix);

        let mut results: Vec<crate::storage::engine::KvPair> = data
            .iter()
            .filter(|(k, _)| k.starts_with(&full_prefix))
            .map(|(k, v)| (k[1..].to_vec(), v.clone()))
            .collect();
        results.sort_by(|(a, _), (b, _)| a.cmp(b));
        Ok(results)
    }

    async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
        let mut data = self.data.lock().await;
        for op in ops {
            match op {
                WriteOp::Put { ns, key, value } => {
                    data.insert(Self::make_key(*ns, key), value.clone());
                }
                WriteOp::Delete { ns, key } => {
                    data.remove(&Self::make_key(*ns, key));
                }
            }
        }
        Ok(())
    }

    async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
        let data = self.data.lock().await;
        let ns_byte = ns as u8;
        Ok(data.keys().filter(|k| k[0] == ns_byte).count() as u64)
    }
}

fn crm_schema() -> nodedb_types::columnar::StrictSchema {
    nodedb_types::columnar::StrictSchema::new(vec![
        ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
        ColumnDef::required("name", ColumnType::String),
        ColumnDef::nullable("email", ColumnType::String),
        ColumnDef::required("balance", ColumnType::Decimal),
        ColumnDef::nullable("active", ColumnType::Bool),
    ])
    .expect("valid schema")
}

#[tokio::test]
async fn create_collection_and_insert() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));

    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create failed");

    assert!(engine.schema("customers").is_some());
    assert_eq!(engine.collection_names().len(), 1);

    // Insert a row.
    engine
        .insert(
            "customers",
            &[
                Value::Integer(1),
                Value::String("Alice".into()),
                Value::String("alice@co.com".into()),
                Value::Decimal(rust_decimal::Decimal::new(5000, 2)),
                Value::Bool(true),
            ],
        )
        .await
        .expect("insert failed");

    // Verify count.
    assert_eq!(engine.count("customers").await.expect("count"), 1);
}

#[tokio::test]
async fn get_by_pk() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));
    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create");

    engine
        .insert(
            "customers",
            &[
                Value::Integer(42),
                Value::String("Bob".into()),
                Value::Null,
                Value::Decimal(rust_decimal::Decimal::ZERO),
                Value::Null,
            ],
        )
        .await
        .expect("insert");

    let row = engine
        .get("customers", &Value::Integer(42))
        .await
        .expect("get")
        .expect("row should exist");

    assert_eq!(row[0], Value::Integer(42));
    assert_eq!(row[1], Value::String("Bob".into()));
    assert_eq!(row[2], Value::Null);
}

#[tokio::test]
async fn get_projected() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));
    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create");

    engine
        .insert(
            "customers",
            &[
                Value::Integer(1),
                Value::String("Charlie".into()),
                Value::String("c@co.com".into()),
                Value::Decimal(rust_decimal::Decimal::new(100, 0)),
                Value::Bool(false),
            ],
        )
        .await
        .expect("insert");

    // Only fetch name and balance.
    let row = engine
        .get_projected("customers", &Value::Integer(1), &["name", "balance"])
        .await
        .expect("get_projected")
        .expect("row");

    assert_eq!(row.len(), 2);
    assert_eq!(row[0], Value::String("Charlie".into()));
    assert_eq!(row[1], Value::Decimal(rust_decimal::Decimal::new(100, 0)));
}

#[tokio::test]
async fn update_row() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));
    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create");

    engine
        .insert(
            "customers",
            &[
                Value::Integer(1),
                Value::String("Dana".into()),
                Value::Null,
                Value::Decimal(rust_decimal::Decimal::new(500, 0)),
                Value::Bool(true),
            ],
        )
        .await
        .expect("insert");

    // Update balance.
    let mut updates = HashMap::new();
    updates.insert(
        "balance".into(),
        Value::Decimal(rust_decimal::Decimal::new(600, 0)),
    );
    let updated = engine
        .update("customers", &Value::Integer(1), &updates)
        .await
        .expect("update");
    assert!(updated);

    // Verify.
    let row = engine
        .get("customers", &Value::Integer(1))
        .await
        .expect("get")
        .expect("row");
    assert_eq!(row[3], Value::Decimal(rust_decimal::Decimal::new(600, 0)));
    // Other fields unchanged.
    assert_eq!(row[1], Value::String("Dana".into()));
}

#[tokio::test]
async fn delete_row() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));
    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create");

    engine
        .insert(
            "customers",
            &[
                Value::Integer(1),
                Value::String("Eve".into()),
                Value::Null,
                Value::Decimal(rust_decimal::Decimal::ZERO),
                Value::Null,
            ],
        )
        .await
        .expect("insert");

    assert!(
        engine
            .delete("customers", &Value::Integer(1))
            .await
            .expect("delete")
    );
    assert!(
        !engine
            .delete("customers", &Value::Integer(1))
            .await
            .expect("delete again")
    );
    assert!(
        engine
            .get("customers", &Value::Integer(1))
            .await
            .expect("get")
            .is_none()
    );
}

#[tokio::test]
async fn duplicate_pk_rejected() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));
    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create");

    let row = vec![
        Value::Integer(1),
        Value::String("X".into()),
        Value::Null,
        Value::Decimal(rust_decimal::Decimal::ZERO),
        Value::Null,
    ];

    engine
        .insert("customers", &row)
        .await
        .expect("first insert");
    let err = engine.insert("customers", &row).await;
    assert!(err.is_err());
}

#[tokio::test]
async fn scan_and_count() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));
    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create");

    for i in 0..5 {
        engine
            .insert(
                "customers",
                &[
                    Value::Integer(i),
                    Value::String(format!("user_{i}")),
                    Value::Null,
                    Value::Decimal(rust_decimal::Decimal::from(i)),
                    Value::Null,
                ],
            )
            .await
            .expect("insert");
    }

    assert_eq!(engine.count("customers").await.expect("count"), 5);

    let raw = engine.scan_raw("customers").await.expect("scan_raw");
    assert_eq!(raw.len(), 5);
}

#[tokio::test]
async fn restore_from_storage() {
    let storage = Arc::new(MemStorage::new());

    // Create and populate.
    {
        let mut engine = StrictEngine::new(Arc::clone(&storage));
        engine
            .create_collection("customers", crm_schema())
            .await
            .expect("create");
        engine
            .insert(
                "customers",
                &[
                    Value::Integer(1),
                    Value::String("Alice".into()),
                    Value::Null,
                    Value::Decimal(rust_decimal::Decimal::ZERO),
                    Value::Null,
                ],
            )
            .await
            .expect("insert");
    }

    // Restore from storage (simulating restart).
    let engine = StrictEngine::restore(Arc::clone(&storage))
        .await
        .expect("restore");

    assert!(engine.schema("customers").is_some());
    let row = engine
        .get("customers", &Value::Integer(1))
        .await
        .expect("get")
        .expect("row");
    assert_eq!(row[0], Value::Integer(1));
    assert_eq!(row[1], Value::String("Alice".into()));
}

#[tokio::test]
async fn batch_insert() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));
    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create");

    let rows: Vec<Vec<Value>> = (0..10)
        .map(|i| {
            vec![
                Value::Integer(i),
                Value::String(format!("batch_{i}")),
                Value::Null,
                Value::Decimal(rust_decimal::Decimal::from(i)),
                Value::Null,
            ]
        })
        .collect();

    engine
        .insert_batch("customers", &rows)
        .await
        .expect("batch_insert");

    assert_eq!(engine.count("customers").await.expect("count"), 10);
}

#[tokio::test]
async fn drop_collection() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));
    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create");

    engine
        .insert(
            "customers",
            &[
                Value::Integer(1),
                Value::String("X".into()),
                Value::Null,
                Value::Decimal(rust_decimal::Decimal::ZERO),
                Value::Null,
            ],
        )
        .await
        .expect("insert");

    engine.drop_collection("customers").await.expect("drop");

    assert!(engine.schema("customers").is_none());
    assert!(engine.collection_names().is_empty());
}

#[tokio::test]
async fn alter_add_column() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));
    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create");

    // Insert a row with the original 5-column schema.
    engine
        .insert(
            "customers",
            &[
                Value::Integer(1),
                Value::String("Alice".into()),
                Value::Null,
                Value::Decimal(rust_decimal::Decimal::ZERO),
                Value::Null,
            ],
        )
        .await
        .expect("insert");

    // ALTER ADD COLUMN — nullable column, no default.
    engine
        .alter_add_column(
            "customers",
            ColumnDef::nullable("phone", ColumnType::String),
        )
        .await
        .expect("alter");

    // Schema should now have 6 columns, version 2.
    let schema = engine.schema("customers").expect("schema exists");
    assert_eq!(schema.columns.len(), 6);
    assert_eq!(schema.version, 2);
    assert_eq!(schema.columns[5].name, "phone");

    // Old row should still be readable (version 1 tuple, 5 columns).
    // The new column returns Null for old tuples.
    let row = engine
        .get("customers", &Value::Integer(1))
        .await
        .expect("get")
        .expect("row");
    assert_eq!(row.len(), 6); // 5 original + 1 new (Null).
    assert_eq!(row[5], Value::Null); // New column is Null for old tuples.
}

#[tokio::test]
async fn alter_add_non_nullable_without_default_rejected() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));
    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create");

    let err = engine
        .alter_add_column(
            "customers",
            ColumnDef::required("phone", ColumnType::String),
        )
        .await;
    assert!(err.is_err()); // Non-nullable without default rejected.
}

#[tokio::test]
async fn alter_add_duplicate_column_rejected() {
    let storage = Arc::new(MemStorage::new());
    let mut engine = StrictEngine::new(Arc::clone(&storage));
    engine
        .create_collection("customers", crm_schema())
        .await
        .expect("create");

    let err = engine
        .alter_add_column("customers", ColumnDef::nullable("name", ColumnType::String))
        .await;
    assert!(err.is_err()); // Duplicate column name.
}
