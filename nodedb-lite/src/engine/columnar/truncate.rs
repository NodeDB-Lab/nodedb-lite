// SPDX-License-Identifier: Apache-2.0

//! Whole-collection `TRUNCATE` on the Lite columnar engine.
//!
//! The collection stays registered with its schema, profile, and bitemporal
//! flag. Segment ids keep counting from where they were, so a stale reader
//! can never name a new segment by an old id.

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::{StorageEngine, WriteOp};

use super::store::{ColumnarEngine, SegmentMeta, remove_segment_bytes};

impl<S: StorageEngine> ColumnarEngine<S> {
    /// Remove every row of `collection`: the memtable, the primary-key
    /// index, every delete bitmap, every bitemporal version, and every
    /// flushed segment with its metadata. Returns the number of rows that
    /// existed across the memtable and live segments.
    ///
    /// Errors when `collection` is not a columnar collection.
    pub async fn truncate(&self, collection: &str) -> Result<usize, LiteError> {
        let state_arc = self.lookup(collection)?;

        // Drain in-memory state under the lock, then do storage I/O with the
        // lock dropped.
        let (segments, truncated): (Vec<SegmentMeta>, usize) = {
            let mut s = Self::lock_state(&state_arc)?;
            let segment_rows: u64 = s
                .segments
                .iter()
                .filter(|m| m.fully_deleted_at_ms.is_none())
                .map(|m| m.row_count)
                .sum();
            let truncated = segment_rows as usize + s.mutation.memtable().row_count();
            // Lite runs no transaction undo log, so the pre-image is dropped.
            drop(s.mutation.truncate());
            (std::mem::take(&mut s.segments), truncated)
        };

        for seg in &segments {
            if seg.fully_deleted_at_ms.is_none() {
                remove_segment_bytes(&*self.storage, collection, seg.segment_id).await?;
            }
        }

        let mut ops: Vec<WriteOp> = Vec::with_capacity(segments.len() + 1);
        for seg in &segments {
            ops.push(WriteOp::Delete {
                ns: Namespace::Columnar,
                key: format!("{collection}:del:{}", seg.segment_id).into_bytes(),
            });
        }
        ops.push(WriteOp::Delete {
            ns: Namespace::Columnar,
            key: format!("{collection}:meta").into_bytes(),
        });
        self.storage.batch_write(&ops).await?;

        Ok(truncated)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_types::columnar::{ColumnDef, ColumnType, ColumnarProfile, ColumnarSchema};
    use nodedb_types::value::Value;

    use super::*;
    use crate::PagedbStorageMem;
    use crate::query::engine::{test_governor, test_scoped_memory};

    async fn engine() -> ColumnarEngine<PagedbStorageMem> {
        let storage = Arc::new(
            PagedbStorageMem::open_in_memory()
                .await
                .expect("in-memory pagedb"),
        );
        let governor = test_governor();
        let engine = ColumnarEngine::new(
            storage,
            test_scoped_memory(&governor, nodedb_mem::EngineId::Columnar),
        );
        let schema = ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::nullable("v", ColumnType::Int64),
        ])
        .expect("schema");
        engine
            .create_collection("t", schema, ColumnarProfile::Plain, false)
            .await
            .expect("create");
        engine
    }

    fn row(i: i64) -> Vec<Value> {
        vec![Value::Integer(i), Value::Integer(i * 10)]
    }

    #[tokio::test]
    async fn truncate_empties_memtable_and_keeps_schema() {
        let engine = engine().await;
        for i in 1..=3 {
            engine.insert("t", &row(i)).expect("insert");
        }
        assert_eq!(engine.truncate("t").await.expect("truncate"), 3);
        assert_eq!(engine.row_count("t"), 0);
        assert!(engine.list_rows("t").await.expect("rows").is_empty());
        assert!(engine.schema("t").is_some(), "schema survives");
        engine.insert("t", &row(1)).expect("insert after truncate");
        assert_eq!(engine.row_count("t"), 1);
    }

    #[tokio::test]
    async fn truncate_drops_flushed_segments() {
        let engine = engine().await;
        for i in 1..=3 {
            engine.insert("t", &row(i)).expect("insert");
        }
        engine.flush_collection("t").await.expect("flush");
        engine.insert("t", &row(4)).expect("insert");
        assert_eq!(engine.truncate("t").await.expect("truncate"), 4);
        assert!(
            engine
                .read_segments("t")
                .await
                .expect("segments")
                .is_empty()
        );
        assert!(engine.list_rows("t").await.expect("rows").is_empty());
        let meta = engine
            .storage
            .get(Namespace::Columnar, b"t:meta")
            .await
            .expect("get");
        assert!(meta.is_none(), "segment metadata is removed");
    }

    #[tokio::test]
    async fn truncate_unknown_collection_is_an_error() {
        let engine = engine().await;
        assert!(engine.truncate("nope").await.is_err());
    }
}
