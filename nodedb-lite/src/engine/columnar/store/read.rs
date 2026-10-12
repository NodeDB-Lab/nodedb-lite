// SPDX-License-Identifier: Apache-2.0

//! Columnar row scans, segment reads, and collection counts.

use super::decode::decoded_column_value;
use super::segments::load_segment_bytes;
use super::state::{ColumnarEngine, SegmentMeta};
use crate::error::LiteError;
use crate::storage::engine::StorageEngine;
use nodedb_columnar::delete_bitmap::DeleteBitmap;
use nodedb_types::value::Value;

impl<S: StorageEngine> ColumnarEngine<S> {
    /// Scan all rows in a columnar collection, returning them in schema column order.
    ///
    /// Reads memtable rows first, then flushed segments. Each row is a
    /// `Vec<Value>` whose entries correspond 1-to-1 with `schema().columns`.
    pub async fn list_rows(&self, collection: &str) -> Result<Vec<Vec<Value>>, LiteError> {
        let state_arc = self.lookup(collection)?;

        // Collect memtable rows and segment metadata under the inner lock (briefly).
        struct Snapshot {
            memtable_rows: Vec<Vec<Value>>,
            seg_metas: Vec<SegmentMeta>,
            col_count: usize,
        }
        let snap = {
            let s = Self::lock_state(&state_arc)?;
            // A memtable row with a corrupt cell refuses the scan.
            let memtable_rows: Vec<Vec<Value>> = s
                .mutation
                .memtable()
                .iter_rows()
                .collect::<Result<_, _>>()
                .map_err(|e| LiteError::Storage {
                    detail: format!("read memtable row of '{collection}': {e}"),
                })?;
            Snapshot {
                memtable_rows,
                seg_metas: s.segments.clone(),
                col_count: s.mutation.schema().columns.len(),
            }
        };

        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        all_rows.extend(snap.memtable_rows);

        // Read each flushed segment from storage (lock dropped) and transpose
        // the columnar layout back to row-major Values. Skip fully-deleted
        // tombstones (their physical segment file has already been removed).
        for seg_meta in &snap.seg_metas {
            if seg_meta.fully_deleted_at_ms.is_some() {
                continue;
            }
            let seg_bytes =
                match load_segment_bytes(&*self.storage, collection, seg_meta.segment_id).await? {
                    Some(b) => b,
                    None => continue,
                };

            let reader = nodedb_columnar::reader::SegmentReader::open(&seg_bytes).map_err(|e| {
                LiteError::Storage {
                    detail: format!("open segment {}: {e}", seg_meta.segment_id),
                }
            })?;

            let row_count = reader.row_count() as usize;
            if row_count == 0 {
                continue;
            }

            // Decode all columns.
            let mut decoded: Vec<nodedb_columnar::reader::DecodedColumn> =
                Vec::with_capacity(snap.col_count);
            for col_idx in 0..snap.col_count {
                let col = reader
                    .read_column(col_idx)
                    .map_err(|e| LiteError::Storage {
                        detail: format!(
                            "read column {col_idx} of segment {}: {e}",
                            seg_meta.segment_id
                        ),
                    })?;
                decoded.push(col);
            }

            // Transpose: iterate row indices, extract one Value per column.
            for row_idx in 0..row_count {
                let row: Vec<Value> = decoded
                    .iter()
                    .map(|col| decoded_column_value(col, row_idx))
                    .collect();
                all_rows.push(row);
            }
        }

        Ok(all_rows)
    }

    /// Read all segment bytes for a collection (for the table provider).
    pub async fn read_segments(&self, collection: &str) -> Result<Vec<(u32, Vec<u8>)>, LiteError> {
        let state_arc = self.lookup(collection)?;
        let seg_metas: Vec<SegmentMeta> = {
            let s = Self::lock_state(&state_arc)?;
            s.segments.clone()
        };

        let mut segments = Vec::with_capacity(seg_metas.len());
        for seg_meta in &seg_metas {
            if seg_meta.fully_deleted_at_ms.is_some() {
                continue;
            }
            if let Some(bytes) =
                load_segment_bytes(&*self.storage, collection, seg_meta.segment_id).await?
            {
                segments.push((seg_meta.segment_id, bytes));
            }
        }

        Ok(segments)
    }

    /// Get the delete bitmap for a segment (returns a clone).
    pub fn delete_bitmap(&self, collection: &str, segment_id: u32) -> Option<DeleteBitmap> {
        let guard = self.collections.read().ok()?;
        let state_arc = guard.get(collection)?;
        let s = state_arc.lock().ok()?;
        s.mutation.delete_bitmap(segment_id as u64).cloned()
    }

    /// Row count across all segments + memtable for a collection.
    pub fn row_count(&self, collection: &str) -> usize {
        let Ok(guard) = self.collections.read() else {
            return 0;
        };
        let Some(state_arc) = guard.get(collection) else {
            return 0;
        };
        let Ok(s) = state_arc.lock() else {
            return 0;
        };
        let seg_rows: u64 = s
            .segments
            .iter()
            .filter(|m| m.fully_deleted_at_ms.is_none())
            .map(|m| m.row_count)
            .sum();
        seg_rows as usize + s.mutation.memtable().row_count()
    }

    /// Whether a collection has bitemporal tracking enabled.
    pub fn is_bitemporal(&self, collection: &str) -> bool {
        let Ok(guard) = self.collections.read() else {
            return false;
        };
        let Some(state_arc) = guard.get(collection) else {
            return false;
        };
        let Ok(s) = state_arc.lock() else {
            return false;
        };
        s.bitemporal
    }
}
