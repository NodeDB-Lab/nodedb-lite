//! High-level emitter for array CRDT ops on the send path.
//!
//! [`ArrayOutbound`] is called by the array engine hooks after each local
//! mutation succeeds. It:
//!
//! 1. Looks up the current `schema_hlc` from the [`SchemaRegistry`].
//! 2. Mints a fresh HLC via [`ReplicaState::next_hlc`].
//! 3. Builds the [`ArrayOp`].
//! 4. Appends to the durable [`RedbOpLog`] (permanent record for GC).
//! 5. Enqueues in the durable [`PendingQueue`] (transport buffer).
//!
//! The caller must ensure the local engine write has already succeeded before
//! calling [`ArrayOutbound`] methods. If emit fails after a successful engine
//! write, the error is returned and the write is **not** rolled back — ack
//! reconciliation in later phases will detect the gap.

use std::sync::Arc;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
use nodedb_array::sync::op_log::OpLog;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;

use crate::error::LiteError;
use crate::storage::engine::StorageEngineSync;
use crate::sync::array::op_log_redb::RedbOpLog;
use crate::sync::array::pending::PendingQueue;
use crate::sync::array::replica_state::ReplicaState;
use crate::sync::array::schema_registry::SchemaRegistry;

/// Emitter for locally-originated array CRDT ops.
///
/// All fields are `Arc`-wrapped so the struct can be shared across the
/// `NodeDbLite` struct and any future transport tasks.
pub struct ArrayOutbound<S: StorageEngineSync> {
    pub(crate) op_log: Arc<RedbOpLog<S>>,
    pub(crate) pending: Arc<PendingQueue<S>>,
    pub(crate) schemas: Arc<SchemaRegistry<S>>,
    pub(crate) replica: Arc<ReplicaState>,
}

impl<S: StorageEngineSync> ArrayOutbound<S> {
    /// Create an [`ArrayOutbound`] from its component parts.
    pub fn new(
        op_log: Arc<RedbOpLog<S>>,
        pending: Arc<PendingQueue<S>>,
        schemas: Arc<SchemaRegistry<S>>,
        replica: Arc<ReplicaState>,
    ) -> Self {
        Self {
            op_log,
            pending,
            schemas,
            replica,
        }
    }

    /// Access the underlying op-log (for sharing with inbound handler).
    pub fn op_log(&self) -> &Arc<RedbOpLog<S>> {
        &self.op_log
    }

    /// Access the underlying pending queue (for sharing with inbound handler).
    pub fn pending(&self) -> &Arc<PendingQueue<S>> {
        &self.pending
    }

    /// Emit a `Put` op for a cell write.
    ///
    /// `coord` and `attrs` must be the same values passed to the array engine
    /// (cloned before the engine call to avoid moves).
    pub fn emit_put(
        &self,
        array: &str,
        coord: Vec<CoordValue>,
        attrs: Vec<CellValue>,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> Result<Hlc, LiteError> {
        let schema_hlc = self.require_schema_hlc(array)?;
        let hlc = self.replica.next_hlc()?;

        let op = ArrayOp {
            header: ArrayOpHeader {
                array: array.into(),
                hlc,
                schema_hlc,
                valid_from_ms,
                valid_until_ms,
                system_from_ms: hlc.physical_ms as i64,
            },
            kind: ArrayOpKind::Put,
            coord,
            attrs: Some(attrs),
        };

        self.record(&op)?;
        Ok(hlc)
    }

    /// Emit a `Delete` (soft tombstone) op.
    ///
    /// `valid_from_ms` / `valid_until_ms` default to `0` / `i64::MAX` at the
    /// call sites because the current [`NodeDbLite::array_delete_cell`] API
    /// does not yet carry valid-time arguments. Phase F will widen the API.
    pub fn emit_delete(
        &self,
        array: &str,
        coord: Vec<CoordValue>,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> Result<Hlc, LiteError> {
        let schema_hlc = self.require_schema_hlc(array)?;
        let hlc = self.replica.next_hlc()?;

        let op = ArrayOp {
            header: ArrayOpHeader {
                array: array.into(),
                hlc,
                schema_hlc,
                valid_from_ms,
                valid_until_ms,
                system_from_ms: hlc.physical_ms as i64,
            },
            kind: ArrayOpKind::Delete,
            coord,
            attrs: None,
        };

        self.record(&op)?;
        Ok(hlc)
    }

    /// Emit an `Erase` (GDPR hard tombstone) op.
    ///
    /// Same valid-time defaulting as [`emit_delete`].
    pub fn emit_erase(
        &self,
        array: &str,
        coord: Vec<CoordValue>,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> Result<Hlc, LiteError> {
        let schema_hlc = self.require_schema_hlc(array)?;
        let hlc = self.replica.next_hlc()?;

        let op = ArrayOp {
            header: ArrayOpHeader {
                array: array.into(),
                hlc,
                schema_hlc,
                valid_from_ms,
                valid_until_ms,
                system_from_ms: hlc.physical_ms as i64,
            },
            kind: ArrayOpKind::Erase,
            coord,
            attrs: None,
        };

        self.record(&op)?;
        Ok(hlc)
    }

    // ─── Internal helpers ─────────────────────────────────────────────────────

    /// Look up the current `schema_hlc` for `array`, returning an error if the
    /// array was never registered via [`SchemaRegistry::put_schema`].
    fn require_schema_hlc(&self, array: &str) -> Result<Hlc, LiteError> {
        self.schemas
            .schema_hlc(array)
            .ok_or_else(|| LiteError::Storage {
                detail: format!("array '{array}' has no schema CRDT — call create_array first"),
            })
    }

    /// Append to op-log then enqueue for transport.
    fn record(&self, op: &ArrayOp) -> Result<(), LiteError> {
        self.op_log.append(op).map_err(|e| LiteError::Storage {
            detail: format!("array sync op_log: {e}"),
        })?;
        self.pending.enqueue(op)?;
        Ok(())
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_storage::RedbStorage;
    use nodedb_array::schema::array_schema::ArraySchema;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::cell_order::{CellOrder, TileOrder};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::sync::op_log::OpLog;
    use nodedb_array::types::domain::{Domain, DomainBound};

    fn simple_schema(name: &str) -> ArraySchema {
        ArraySchema {
            name: name.into(),
            dims: vec![DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
            )],
            attrs: vec![AttrSpec::new("v", AttrType::Float64, true)],
            tile_extents: vec![10],
            cell_order: CellOrder::RowMajor,
            tile_order: TileOrder::RowMajor,
        }
    }

    fn make_outbound() -> (ArrayOutbound<RedbStorage>, Arc<RedbStorage>) {
        let storage = Arc::new(RedbStorage::open_in_memory().unwrap());
        let replica = Arc::new(ReplicaState::load_or_init(&*storage).unwrap());
        let schemas = Arc::new(SchemaRegistry::new(
            Arc::clone(&storage),
            Arc::clone(&replica),
        ));
        let op_log = Arc::new(RedbOpLog::new(Arc::clone(&storage)));
        let pending = Arc::new(PendingQueue::new(Arc::clone(&storage)));
        let ob = ArrayOutbound::new(op_log, pending, schemas, replica);
        (ob, storage)
    }

    #[test]
    fn emit_put_appends_to_log_and_queue() {
        let (ob, _storage) = make_outbound();
        ob.schemas.put_schema("arr", &simple_schema("arr")).unwrap();

        let coord = vec![CoordValue::Int64(5)];
        let attrs = vec![CellValue::Null];
        ob.emit_put("arr", coord, attrs, 0, i64::MAX).unwrap();

        assert_eq!(ob.op_log.len().unwrap(), 1);
        assert_eq!(ob.pending.len().unwrap(), 1);
    }

    #[test]
    fn emit_without_schema_errors() {
        let (ob, _storage) = make_outbound();
        let err = ob
            .emit_put(
                "unknown",
                vec![CoordValue::Int64(0)],
                vec![CellValue::Null],
                0,
                -1,
            )
            .unwrap_err();
        assert!(
            matches!(err, LiteError::Storage { ref detail } if detail.contains("no schema CRDT")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn emit_delete_carries_no_attrs() {
        let (ob, _storage) = make_outbound();
        ob.schemas.put_schema("d", &simple_schema("d")).unwrap();

        ob.emit_delete("d", vec![CoordValue::Int64(1)], 0, i64::MAX)
            .unwrap();

        let ops: Vec<_> = ob
            .op_log
            .scan_from(Hlc::ZERO)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ops.len(), 1);
        assert!(ops[0].attrs.is_none(), "Delete must carry no attrs");
    }

    #[test]
    fn emit_erase_carries_no_attrs() {
        let (ob, _storage) = make_outbound();
        ob.schemas.put_schema("e", &simple_schema("e")).unwrap();

        ob.emit_erase("e", vec![CoordValue::Int64(2)], 0, i64::MAX)
            .unwrap();

        let ops: Vec<_> = ob
            .op_log
            .scan_from(Hlc::ZERO)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();
        assert_eq!(ops.len(), 1);
        assert!(ops[0].attrs.is_none(), "Erase must carry no attrs");
    }

    #[test]
    fn emit_advances_hlc() {
        let (ob, _storage) = make_outbound();
        ob.schemas.put_schema("a", &simple_schema("a")).unwrap();

        let h1 = ob
            .emit_put(
                "a",
                vec![CoordValue::Int64(1)],
                vec![CellValue::Null],
                0,
                i64::MAX,
            )
            .unwrap();
        let h2 = ob
            .emit_put(
                "a",
                vec![CoordValue::Int64(2)],
                vec![CellValue::Null],
                0,
                i64::MAX,
            )
            .unwrap();
        assert!(h2 > h1, "each emit must mint a strictly greater HLC");
    }
}
