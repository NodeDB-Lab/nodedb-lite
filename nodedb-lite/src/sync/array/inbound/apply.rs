//! [`LiteApplyEngine`] — adapts NodeDB-Lite's array engine to the
//! [`ApplyEngine`] trait so [`nodedb_array::sync::apply::apply_op`] can drive
//! local state from inbound wire messages.

use std::collections::HashMap;
use std::future::Future;
use std::sync::{Arc, Mutex};

use nodedb_array::error::ArrayError;
use nodedb_array::sync::apply::ApplyEngine;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_log::OpLog;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::Namespace;

use crate::engine::array::engine::ArrayEngineState;
use crate::storage::engine::StorageEngine;
use crate::sync::array::op_log_redb::RedbOpLog;
use crate::sync::array::schema_registry::SchemaRegistry;

/// Key prefix for `last_applied_hlc` entries persisted under `Namespace::Meta`.
const LAST_APPLIED_PREFIX: &str = "array.last_applied:";

/// Bridge an async future into a sync context via `block_in_place`.
///
/// Used exclusively for bridging `StorageEngine` async calls inside the sync
/// `ApplyEngine` trait methods.
fn block<F, T>(f: F) -> T
where
    F: Future<Output = T>,
{
    tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(f))
}

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
/// which sit below `NodeDbLite::array_put_cell`. The `ArrayOutbound`
/// hook is therefore never triggered, making the receive path loop-free by
/// construction.
pub struct LiteApplyEngine<S: StorageEngine> {
    pub(super) storage: Arc<S>,
    pub(super) array_state: Arc<tokio::sync::Mutex<ArrayEngineState>>,
    pub(super) schemas: Arc<SchemaRegistry<S>>,
    pub(super) op_log: Arc<RedbOpLog<S>>,
    /// In-memory cache of the last applied HLC per array.
    /// Persisted under `Namespace::Meta` `"array.last_applied:{name}"`.
    last_applied: Mutex<HashMap<String, Hlc>>,
}

impl<S: StorageEngine> LiteApplyEngine<S> {
    /// Construct from the component parts shared with `NodeDbLite`.
    pub async fn new(
        storage: Arc<S>,
        array_state: Arc<tokio::sync::Mutex<ArrayEngineState>>,
        schemas: Arc<SchemaRegistry<S>>,
        op_log: Arc<RedbOpLog<S>>,
    ) -> Self {
        let last_applied = Self::load_last_applied(&storage).await;
        Self {
            storage,
            array_state,
            schemas,
            op_log,
            last_applied: Mutex::new(last_applied),
        }
    }

    async fn load_last_applied(storage: &Arc<S>) -> HashMap<String, Hlc> {
        let prefix = LAST_APPLIED_PREFIX.as_bytes();
        let Ok(pairs) = storage
            .scan_range(Namespace::Meta, prefix, usize::MAX)
            .await
        else {
            return HashMap::new();
        };
        let mut map = HashMap::new();
        for (key, value) in pairs {
            if !key.starts_with(prefix) {
                break;
            }
            let name_bytes = &key[prefix.len()..];
            let Ok(name) = std::str::from_utf8(name_bytes) else {
                continue;
            };
            if value.len() == 18 {
                let bytes: [u8; 18] = match value.try_into() {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                map.insert(name.to_owned(), Hlc::from_bytes(&bytes));
            }
        }
        map
    }

    /// Return the highest HLC applied for `array`, or `None` if no ops applied.
    pub fn last_applied_hlc(&self, array: &str) -> Option<Hlc> {
        self.last_applied.lock().ok()?.get(array).copied()
    }

    /// Record that `hlc` has been successfully applied for `array`.
    ///
    /// Advances the in-memory record and persists under `Namespace::Meta`.
    /// Uses `block_in_place` to bridge the async storage call from a sync context.
    fn record_applied_hlc(&self, array: &str, hlc: Hlc) {
        let should_persist = {
            let mut map = match self.last_applied.lock() {
                Ok(m) => m,
                Err(_) => return,
            };
            let current = map.get(array).copied().unwrap_or(Hlc::ZERO);
            if hlc > current {
                map.insert(array.to_owned(), hlc);
                true
            } else {
                false
            }
        };
        if should_persist {
            let key = format!("{LAST_APPLIED_PREFIX}{array}").into_bytes();
            let storage = Arc::clone(&self.storage);
            let bytes = hlc.to_bytes();
            let _ = block(async move { storage.put(Namespace::Meta, &key, &bytes).await });
        }
    }
}

/// Implement `ApplyEngine` on a *borrowed* `LiteApplyEngine<S>`.
///
/// Because all state lives behind `Arc` / `Arc<tokio::sync::Mutex<...>>`, a
/// shared reference carries enough indirection to perform all mutations. The
/// trait requires `&mut self` (`E = &LiteApplyEngine<S>`), and `&mut E` is
/// merely a rebindable outer reference that we never actually need to mutate.
///
/// `apply_put` and `apply_erase` call async `ArrayEngineState` methods via
/// `block_in_place` + `blocking_lock`, which is safe inside a `multi_thread`
/// Tokio runtime.
impl<S: StorageEngine> ApplyEngine for &LiteApplyEngine<S> {
    fn schema_hlc(&self, array: &str) -> nodedb_array::error::ArrayResult<Option<Hlc>> {
        Ok(self.schemas.schema_hlc(array))
    }

    fn already_seen(&self, array: &str, hlc: Hlc) -> nodedb_array::error::ArrayResult<bool> {
        let mut iter = self.op_log.scan_range(array, hlc, hlc)?;
        Ok(iter.next().is_some())
    }

    fn apply_put(&mut self, op: &ArrayOp) -> nodedb_array::error::ArrayResult<()> {
        let system_from_ms = op.header.system_from_ms;
        let attrs = op.attrs.clone().unwrap_or_default();
        let array_state = Arc::clone(&self.array_state);
        let storage = Arc::clone(&self.storage);
        let array = op.header.array.clone();
        let coord = op.coord.clone();
        let valid_from_ms = op.header.valid_from_ms;
        let valid_until_ms = op.header.valid_until_ms;
        block(async move {
            let mut state = array_state.lock().await;
            state
                .put_cell(
                    &storage,
                    &array,
                    coord,
                    attrs,
                    system_from_ms,
                    valid_from_ms,
                    valid_until_ms,
                )
                .await
                .map_err(|e| ArrayError::SegmentCorruption {
                    detail: format!("apply_put: {e}"),
                })
        })?;
        // Record in op-log so that subsequent `already_seen` returns true.
        self.op_log.append(op)?;
        self.record_applied_hlc(&op.header.array, op.header.hlc);
        Ok(())
    }

    fn apply_delete(&mut self, op: &ArrayOp) -> nodedb_array::error::ArrayResult<()> {
        {
            let mut state = self.array_state.blocking_lock();
            state
                .delete_cell(&op.header.array, op.coord.clone(), op.header.system_from_ms)
                .map_err(|e| ArrayError::SegmentCorruption {
                    detail: format!("apply_delete: {e}"),
                })?;
        }
        self.op_log.append(op)?;
        self.record_applied_hlc(&op.header.array, op.header.hlc);
        Ok(())
    }

    fn apply_erase(&mut self, op: &ArrayOp) -> nodedb_array::error::ArrayResult<()> {
        let array_state = Arc::clone(&self.array_state);
        let storage = Arc::clone(&self.storage);
        let array = op.header.array.clone();
        let coord = op.coord.clone();
        let system_from_ms = op.header.system_from_ms;
        block(async move {
            let mut state = array_state.lock().await;
            state
                .gdpr_erase_cell(&storage, &array, coord, system_from_ms)
                .await
                .map_err(|e| ArrayError::SegmentCorruption {
                    detail: format!("apply_erase: {e}"),
                })
        })?;
        self.op_log.append(op)?;
        self.record_applied_hlc(&op.header.array, op.header.hlc);
        Ok(())
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
