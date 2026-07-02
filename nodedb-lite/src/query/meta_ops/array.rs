// SPDX-License-Identifier: Apache-2.0
//! Array DDL meta-ops: AlterArray.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

/// `AlterArray` — update the bitemporal retention policy for an array.
///
/// The new `audit_retain_ms` is persisted to `Namespace::Meta` under the key
/// `array_retain/<array_id>` so that subsequent compact calls can read it.
pub async fn handle_alter_array<S: StorageEngine>(
    engine: &LiteQueryEngine<S>,
    array_id: &str,
    audit_retain_ms: Option<Option<i64>>,
    minimum_audit_retain_ms: Option<Option<u64>>,
) -> Result<QueryResult, LiteError> {
    use nodedb_types::Namespace;

    // Persist the new audit_retain_ms if the field was set.
    if let Some(new_retain) = audit_retain_ms {
        let key = format!("array_retain/{array_id}");
        match new_retain {
            Some(ms) => {
                engine
                    .storage
                    .put(Namespace::Meta, key.as_bytes(), &ms.to_le_bytes())
                    .await?;
            }
            None => {
                engine
                    .storage
                    .delete(Namespace::Meta, key.as_bytes())
                    .await?;
            }
        }
    }

    // Persist the minimum_audit_retain_ms if the field was set.
    if let Some(new_min) = minimum_audit_retain_ms {
        let key = format!("array_min_retain/{array_id}");
        match new_min {
            Some(ms) => {
                engine
                    .storage
                    .put(Namespace::Meta, key.as_bytes(), &ms.to_le_bytes())
                    .await?;
            }
            None => {
                engine
                    .storage
                    .delete(Namespace::Meta, key.as_bytes())
                    .await?;
            }
        }
    }

    Ok(QueryResult {
        columns: vec!["array_id".into()],
        rows: vec![vec![Value::String(array_id.to_owned())]],
        rows_affected: 1,
    })
}
