//! Schema CRDT snapshot import handler.

use nodedb_array::sync::hlc::Hlc;
use nodedb_types::sync::wire::array::ArraySchemaSyncMsg;

use crate::error::LiteError;
use crate::storage::engine::StorageEngineSync;

use super::dispatcher::ArrayInbound;
use super::outcome::InboundOutcome;

impl<S: StorageEngineSync> ArrayInbound<S> {
    /// Import a schema CRDT snapshot from Origin.
    ///
    /// Updates the local [`crate::sync::array::SchemaRegistry`] with the Loro
    /// snapshot payload. After import, ops targeting this array that
    /// previously returned `SchemaTooNew` may succeed on retry.
    pub fn handle_schema(&self, msg: &ArraySchemaSyncMsg) -> Result<InboundOutcome, LiteError> {
        let schema_hlc = Hlc::from_bytes(&msg.schema_hlc_bytes);
        self.schemas
            .import_snapshot(&msg.array, &msg.snapshot_payload, schema_hlc)?;
        Ok(InboundOutcome::SchemaImported)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::sync::wire::array::ArraySchemaSyncMsg;

    use crate::storage::redb_storage::RedbStorage;
    use crate::sync::array::replica_state::ReplicaState;
    use crate::sync::array::schema_registry::SchemaRegistry;

    use super::super::fixtures::{make_inbound, simple_schema};
    use super::super::outcome::InboundOutcome;

    #[test]
    fn handle_schema_imports() {
        let (inbound, schemas, _pending, _storage) = make_inbound();

        // Create a schema on a "remote" registry.
        let remote_storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let remote_replica = Arc::new(ReplicaState::load_or_init(&*remote_storage).unwrap());
        let remote_schemas =
            SchemaRegistry::new(Arc::clone(&remote_storage), Arc::clone(&remote_replica));
        remote_schemas
            .put_schema("remote_arr", &simple_schema("remote_arr"))
            .unwrap();
        let snapshot_payload = remote_schemas
            .export_snapshot("remote_arr")
            .unwrap()
            .unwrap();
        let remote_hlc = remote_schemas.schema_hlc("remote_arr").unwrap();

        assert!(
            schemas.schema_hlc("remote_arr").is_none(),
            "not yet imported"
        );

        let msg = ArraySchemaSyncMsg {
            array: "remote_arr".into(),
            replica_id: 42,
            schema_hlc_bytes: remote_hlc.to_bytes(),
            snapshot_payload,
        };
        let outcome = inbound.handle_schema(&msg).unwrap();
        assert_eq!(outcome, InboundOutcome::SchemaImported);
        assert!(
            schemas.schema_hlc("remote_arr").is_some(),
            "must exist after import"
        );
    }
}
