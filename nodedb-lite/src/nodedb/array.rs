//! Public array-engine methods on `NodeDbLite`.
//!
//! All methods are synchronous — the array engine uses `StorageEngineSync`
//! exclusively. The engine is locked via the `Mutex` held in `NodeDbLite`.

use nodedb_array::query::slice::DimRange;
use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::cell_payload::CellPayload;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_types::OPEN_UPPER;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::core::NodeDbLite;
use super::lock_ext::LockExt;

impl<S: StorageEngine + StorageEngineSync> NodeDbLite<S> {
    /// Create a new ND sparse array with the given schema.
    ///
    /// Returns an error if an array named `name` already exists.
    pub fn create_array(&self, name: &str, schema: ArraySchema) -> NodeDbResult<()> {
        // Clone before the engine call to avoid a partial-move of `schema`.
        let schema_for_crdt = schema.clone();

        self.array_state
            .lock_or_recover()
            .create_array(&self.storage, name, schema)
            .map_err(NodeDbError::storage)?;

        // Register the schema CRDT so subsequent emit_* calls can find schema_hlc.
        self.array_schemas
            .put_schema(name, &schema_for_crdt)
            .map_err(NodeDbError::storage)?;

        Ok(())
    }

    /// Write a cell into array `name` at `coord`.
    ///
    /// `system_from_ms` is the bitemporal system time (typically `now()`).
    /// `valid_from_ms` / `valid_until_ms` are the valid-time bounds
    /// (`OPEN_UPPER` = open-ended, no expiry).
    pub fn array_put_cell(
        &self,
        name: &str,
        coord: Vec<CoordValue>,
        attrs: Vec<CellValue>,
        system_from_ms: i64,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> NodeDbResult<()> {
        // Clone before the engine call to keep copies for emit.
        let coord_emit = coord.clone();
        let attrs_emit = attrs.clone();

        self.array_state
            .lock_or_recover()
            .put_cell(
                &self.storage,
                name,
                coord,
                attrs,
                system_from_ms,
                valid_from_ms,
                valid_until_ms,
            )
            .map_err(NodeDbError::storage)?;

        // Emit op after engine succeeds. If emit fails after a successful
        // engine write, log the error and return it; the engine write is
        // NOT rolled back — ack reconciliation in later phases will catch
        // the gap.
        if let Err(e) = self.array_outbound.emit_put(
            name,
            coord_emit,
            attrs_emit,
            valid_from_ms,
            valid_until_ms,
        ) {
            tracing::error!(
                array = name,
                "array_put_cell: emit failed after engine write — op-log gap: {e}"
            );
            return Err(NodeDbError::storage(e));
        }

        Ok(())
    }

    /// Slice query: return all live cells whose coordinates fall within
    /// `ranges` at or before `as_of_system_ms` (defaults to `i64::MAX` for
    /// the current live snapshot).
    pub fn array_slice(
        &self,
        name: &str,
        ranges: Vec<Option<DimRange>>,
        as_of_system_ms: Option<i64>,
    ) -> NodeDbResult<Vec<CellPayload>> {
        let sys = as_of_system_ms.unwrap_or(i64::MAX);
        self.array_state
            .lock_or_recover()
            .slice(&self.storage, name, ranges, sys)
            .map_err(NodeDbError::storage)
    }

    /// Read the most recent live payload for `coord` at or before
    /// `as_of_system_ms`. Returns `None` if not found, tombstoned, or erased.
    pub fn array_read_coord(
        &self,
        name: &str,
        coord: &[CoordValue],
        as_of_system_ms: Option<i64>,
    ) -> NodeDbResult<Option<CellPayload>> {
        let sys = as_of_system_ms.unwrap_or(i64::MAX);
        self.array_state
            .lock_or_recover()
            .read_coord(&self.storage, name, coord, sys)
            .map_err(NodeDbError::storage)
    }

    /// Soft-delete a cell by writing a tombstone version at `system_from_ms`.
    ///
    /// The cell is still visible AS-OF any system time < `system_from_ms`.
    pub fn array_delete_cell(
        &self,
        name: &str,
        coord: Vec<CoordValue>,
        system_from_ms: i64,
    ) -> NodeDbResult<()> {
        // Clone before the engine call for emit.
        let coord_emit = coord.clone();

        self.array_state
            .lock_or_recover()
            .delete_cell(name, coord, system_from_ms)
            .map_err(NodeDbError::storage)?;

        // valid_from_ms / valid_until_ms: the current API does not expose
        // valid-time arguments on delete. Defaults of 0 / OPEN_UPPER are used
        // here. Phase F will widen the API to carry the full bitemporal envelope.
        if let Err(e) = self
            .array_outbound
            .emit_delete(name, coord_emit, 0, OPEN_UPPER)
        {
            tracing::error!(
                array = name,
                "array_delete_cell: emit failed after engine write — op-log gap: {e}"
            );
            return Err(NodeDbError::storage(e));
        }

        Ok(())
    }

    /// GDPR erasure: write the `0xFE` sentinel and flush to disk.
    ///
    /// After this call `array_read_coord` returns `None` for the erased
    /// coordinate at any `system_as_of >= system_from_ms`, and the raw
    /// payload bytes are not present on disk.
    pub fn array_gdpr_erase_cell(
        &self,
        name: &str,
        coord: Vec<CoordValue>,
        system_from_ms: i64,
    ) -> NodeDbResult<()> {
        // Clone before the engine call for emit.
        let coord_emit = coord.clone();

        self.array_state
            .lock_or_recover()
            .gdpr_erase_cell(&self.storage, name, coord, system_from_ms)
            .map_err(NodeDbError::storage)?;

        // valid_from_ms / valid_until_ms: same defaulting as array_delete_cell.
        // Phase F will widen the API.
        if let Err(e) = self
            .array_outbound
            .emit_erase(name, coord_emit, 0, OPEN_UPPER)
        {
            tracing::error!(
                array = name,
                "array_gdpr_erase_cell: emit failed after engine write — op-log gap: {e}"
            );
            return Err(NodeDbError::storage(e));
        }

        Ok(())
    }

    /// Flush any pending memtable data for `name` to a durable segment.
    pub fn array_flush(&self, name: &str) -> NodeDbResult<()> {
        self.array_state
            .lock_or_recover()
            .flush(&self.storage, name)
            .map_err(NodeDbError::storage)
    }
}
