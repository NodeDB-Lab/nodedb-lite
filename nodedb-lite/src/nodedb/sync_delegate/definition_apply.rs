//! Free function extracted from `import_definition` in `SyncDelegate`.

use crate::error::LiteError;
use crate::nodedb::core::NodeDbLite;
use crate::storage::engine::StorageEngine;

pub(super) async fn apply_definition_sync<S: StorageEngine>(
    db: &NodeDbLite<S>,
    msg: &nodedb_types::sync::wire::DefinitionSyncMsg,
) -> Result<(), LiteError> {
    use crate::nodedb::definitions::*;

    let result = match (msg.definition_type.as_str(), msg.action.as_str()) {
        ("function", "put") => match sonic_rs::from_slice::<LiteStoredFunction>(&msg.payload) {
            Ok(func) => db.put_function(&func).await,
            Err(e) => {
                tracing::warn!(name = %msg.name, error = %e, "failed to deserialize function");
                return Ok(());
            }
        },
        ("function", "delete") => db.delete_function(&msg.name).await,
        ("trigger", "put") => match sonic_rs::from_slice::<LiteStoredTrigger>(&msg.payload) {
            Ok(trigger) => db.put_trigger(&trigger).await,
            Err(e) => {
                tracing::warn!(name = %msg.name, error = %e, "failed to deserialize trigger");
                return Ok(());
            }
        },
        ("trigger", "delete") => db.delete_trigger(&msg.name).await,
        ("procedure", "put") => match sonic_rs::from_slice::<LiteStoredProcedure>(&msg.payload) {
            Ok(p) => db.put_procedure(&p).await,
            Err(e) => {
                tracing::warn!(name = %msg.name, error = %e, "failed to deserialize procedure");
                return Ok(());
            }
        },
        ("procedure", "delete") => db.delete_procedure(&msg.name).await,
        _ => {
            tracing::warn!(
                definition_type = %msg.definition_type,
                action = %msg.action,
                "unknown definition type/action"
            );
            return Ok(());
        }
    };
    result.map_err(|e| LiteError::Storage {
        detail: format!("definition sync storage error: {e}"),
    })
}
