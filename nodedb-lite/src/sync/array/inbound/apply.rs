//! [`LiteApplyEngine`] — adapts NodeDB-Lite's array engine to the
//! [`ApplyEngine`] trait so [`nodedb_array::sync::apply::apply_op`] can drive
//! local state from inbound wire messages.

use std::sync::{Arc, Mutex};

use nodedb_array::error::ArrayError;
use nodedb_array::sync::apply::ApplyEngine;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_log::OpLog;
use nodedb_array::types::coord::value::CoordValue;

use crate::engine::array::engine::ArrayEngineState;
use crate::storage::engine::StorageEngineSync;
use crate::sync::array::op_log_redb::RedbOpLog;
use crate::sync::array::schema_registry::SchemaRegistry;

/// Adapts NodeDB-Lite's array engine state to the [`ApplyEngine`] trait.
///
/// All fields are `Arc`-wrapped so their interior mutability can satisfy the
/// trait's `&mut self` methods without requiring `&mut LiteApplyEngine`.
/// We implement `ApplyEngine` for `&LiteApplyEngine<S>` — a mutable reference
/// to a shared reference — which allows `apply_op(&mut engine_ref, &op)` where
/// `engine_ref: &LiteApplyEngine<S>` without any additional allocation.
///
/// # Outbound loop avoidance
///
/// Operations applied here go directly through `ArrayEngineState` methods,
/// which sit below `NodeDbLite::array_put_cell`. The Phase D `ArrayOutbound`
/// hook is therefore never triggered, making the receive path loop-free by
/// construction.
pub struct LiteApplyEngine<S: StorageEngineSync> {
    pub(super) storage: Arc<S>,
    pub(super) array_state: Arc<Mutex<ArrayEngineState>>,
    pub(super) schemas: Arc<SchemaRegistry<S>>,
    pub(super) op_log: Arc<RedbOpLog<S>>,
}

impl<S: StorageEngineSync> LiteApplyEngine<S> {
    /// Construct from the component parts shared with `NodeDbLite`.
    pub fn new(
        storage: Arc<S>,
        array_state: Arc<Mutex<ArrayEngineState>>,
        schemas: Arc<SchemaRegistry<S>>,
        op_log: Arc<RedbOpLog<S>>,
    ) -> Self {
        Self {
            storage,
            array_state,
            schemas,
            op_log,
        }
    }
}

/// Implement `ApplyEngine` on a *borrowed* `LiteApplyEngine<S>`.
///
/// Because all state lives behind `Arc` / `Arc<Mutex<...>>`, a shared
/// reference carries enough indirection to perform all mutations. The trait
/// requires `&mut self` (`E = &LiteApplyEngine<S>`), and `&mut E` is merely a
/// rebindable outer reference that we never actually need to mutate.
impl<S: StorageEngineSync> ApplyEngine for &LiteApplyEngine<S> {
    fn schema_hlc(&self, array: &str) -> nodedb_array::error::ArrayResult<Option<Hlc>> {
        Ok(self.schemas.schema_hlc(array))
    }

    fn already_seen(&self, array: &str, hlc: Hlc) -> nodedb_array::error::ArrayResult<bool> {
        let mut iter = self.op_log.scan_range(array, hlc, hlc)?;
        Ok(iter.next().is_some())
    }

    fn apply_put(&mut self, op: &ArrayOp) -> nodedb_array::error::ArrayResult<()> {
        let mut state = self
            .array_state
            .lock()
            .map_err(|_| ArrayError::HlcLockPoisoned)?;
        let system_from_ms = op.header.system_from_ms;
        let attrs = op.attrs.clone().unwrap_or_default();
        state
            .put_cell(
                &self.storage,
                &op.header.array,
                op.coord.clone(),
                attrs,
                system_from_ms,
                op.header.valid_from_ms,
                op.header.valid_until_ms,
            )
            .map_err(|e| ArrayError::SegmentCorruption {
                detail: format!("apply_put: {e}"),
            })?;
        // Record in op-log so that subsequent `already_seen` returns true.
        // `append` is idempotent on duplicate (array, hlc) pairs.
        self.op_log.append(op)
    }

    fn apply_delete(&mut self, op: &ArrayOp) -> nodedb_array::error::ArrayResult<()> {
        let mut state = self
            .array_state
            .lock()
            .map_err(|_| ArrayError::HlcLockPoisoned)?;
        state
            .delete_cell(&op.header.array, op.coord.clone(), op.header.system_from_ms)
            .map_err(|e| ArrayError::SegmentCorruption {
                detail: format!("apply_delete: {e}"),
            })?;
        self.op_log.append(op)
    }

    fn apply_erase(&mut self, op: &ArrayOp) -> nodedb_array::error::ArrayResult<()> {
        let mut state = self
            .array_state
            .lock()
            .map_err(|_| ArrayError::HlcLockPoisoned)?;
        state
            .gdpr_erase_cell(
                &self.storage,
                &op.header.array,
                op.coord.clone(),
                op.header.system_from_ms,
            )
            .map_err(|e| ArrayError::SegmentCorruption {
                detail: format!("apply_erase: {e}"),
            })?;
        self.op_log.append(op)
    }

    /// No-op: the Lite array engine reads from the durable memtable + segments
    /// after each write, so there is no separate read cache that would need
    /// explicit invalidation. If a tile read-cache is introduced later, this
    /// hook is the right place to drop overlapping entries.
    fn invalidate_tile(
        &mut self,
        _array: &str,
        _coord: &[CoordValue],
    ) -> nodedb_array::error::ArrayResult<()> {
        Ok(())
    }
}
