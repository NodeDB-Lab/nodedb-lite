//! Snapshot header and chunk handlers — buffer chunks until complete, then
//! assemble and apply the contained ops.

use nodedb_array::sync::apply::ApplyOutcome;
use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op_codec;
use nodedb_array::sync::snapshot::{SnapshotChunk, SnapshotHeader, assemble_chunks};
use nodedb_types::sync::wire::array::{ArraySnapshotChunkMsg, ArraySnapshotMsg};

use crate::error::LiteError;
use crate::storage::engine::StorageEngine;

use super::dispatcher::{ArrayInbound, SnapshotAssembly};
use super::outcome::InboundOutcome;

impl<S: StorageEngine> ArrayInbound<S> {
    /// Buffer an incoming snapshot header.
    ///
    /// Must arrive before any [`Self::handle_snapshot_chunk`] calls for the
    /// same `(array, snapshot_hlc)`. Returns
    /// `SnapshotPartial { received: 0, total }` so callers can track progress.
    pub fn handle_snapshot_header(
        &self,
        msg: &ArraySnapshotMsg,
    ) -> Result<InboundOutcome, LiteError> {
        let header: SnapshotHeader =
            zerompk::from_msgpack(&msg.header_payload).map_err(|e| LiteError::Storage {
                detail: format!("handle_snapshot_header decode: {e}"),
            })?;

        let total = header.total_chunks;
        let key = (msg.array.clone(), header.snapshot_hlc.to_bytes());

        let mut snapshots = self.snapshots.lock().map_err(|_| LiteError::LockPoisoned)?;
        let entry = snapshots.entry(key).or_insert_with(SnapshotAssembly::new);
        entry.header = Some(header);

        Ok(InboundOutcome::SnapshotPartial { received: 0, total })
    }

    /// Buffer a snapshot chunk and, when all chunks have arrived, assemble and
    /// apply the snapshot.
    ///
    /// Returns [`InboundOutcome::SnapshotPartial`] until the last chunk
    /// arrives, then returns [`InboundOutcome::SnapshotApplied`] after all
    /// ops in the assembled snapshot have been applied.
    pub fn handle_snapshot_chunk(
        &self,
        msg: &ArraySnapshotChunkMsg,
    ) -> Result<InboundOutcome, LiteError> {
        let key = (msg.array.clone(), msg.snapshot_hlc_bytes);

        let assembled: Option<(SnapshotHeader, Vec<SnapshotChunk>)> = {
            let mut snapshots = self.snapshots.lock().map_err(|_| LiteError::LockPoisoned)?;
            let entry = snapshots
                .entry(key.clone())
                .or_insert_with(SnapshotAssembly::new);

            let chunk = SnapshotChunk {
                array: msg.array.clone(),
                chunk_index: msg.chunk_index,
                total_chunks: msg.total_chunks,
                payload: msg.payload.clone(),
                snapshot_hlc: Hlc::from_bytes(&msg.snapshot_hlc_bytes),
            };
            entry.chunks.insert(msg.chunk_index, chunk);

            let total = msg.total_chunks as usize;
            match &entry.header {
                Some(h) if entry.chunks.len() == total => {
                    // All chunks received — extract for assembly outside the lock.
                    let header = h.clone();
                    let chunks_vec: Vec<SnapshotChunk> = entry.chunks.values().cloned().collect();
                    Some((header, chunks_vec))
                }
                _ => None,
            }
        };

        // If not yet complete, return partial status.
        let Some((header, mut chunks)) = assembled else {
            let snapshots = self.snapshots.lock().map_err(|_| LiteError::LockPoisoned)?;
            let received = snapshots
                .get(&key)
                .map(|e| e.chunks.len() as u32)
                .unwrap_or(0);
            return Ok(InboundOutcome::SnapshotPartial {
                received,
                total: msg.total_chunks,
            });
        };

        // Assemble snapshot.
        let snapshot = assemble_chunks(&header, &mut chunks).map_err(|e| LiteError::Storage {
            detail: format!("assemble_chunks: {e}"),
        })?;

        // The snapshot's tile_blob is a zerompk-encoded Vec<ArrayOp>.
        let ops =
            op_codec::decode_op_batch(&snapshot.tile_blob).map_err(|e| LiteError::Storage {
                detail: format!("snapshot decode_op_batch: {e}"),
            })?;

        let mut ops_applied: u64 = 0;
        for op in &ops {
            self.replica.observe(op.header.hlc)?;
            let outcome = self.apply_single_op(op)?;
            if matches!(outcome, ApplyOutcome::Applied) {
                ops_applied += 1;
            }
        }

        // Remove the completed assembly entry.
        if let Ok(mut snapshots) = self.snapshots.lock() {
            snapshots.remove(&key);
        }

        Ok(InboundOutcome::SnapshotApplied { ops_applied })
    }
}

#[cfg(test)]
mod tests {
    use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
    use nodedb_array::sync::op_codec;
    use nodedb_array::sync::snapshot::{CoordRange, TileSnapshot, split_into_chunks};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_types::sync::wire::array::{ArraySnapshotChunkMsg, ArraySnapshotMsg};

    use super::super::fixtures::{hlc, make_inbound, simple_schema};
    use super::super::outcome::InboundOutcome;

    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_chunks_assemble_and_apply() {
        let (inbound, schemas, _pending, storage) = make_inbound().await;
        schemas
            .put_schema("snap", &simple_schema("snap"))
            .await
            .unwrap();
        {
            let mut state = inbound.engine.array_state.lock().await;
            state
                .create_array(&storage, "snap", simple_schema("snap"))
                .await
                .unwrap();
        }
        let schema_hlc = schemas.schema_hlc("snap").unwrap();

        // Build 3 ops and encode them as a blob.
        let ops: Vec<ArrayOp> = (1u64..=3)
            .map(|i| ArrayOp {
                header: ArrayOpHeader {
                    array: "snap".into(),
                    hlc: hlc(i * 10),
                    schema_hlc,
                    valid_from_ms: 0,
                    valid_until_ms: -1,
                    system_from_ms: (i * 10) as i64,
                },
                kind: ArrayOpKind::Put,
                coord: vec![CoordValue::Int64(i as i64)],
                attrs: Some(vec![CellValue::Float64(i as f64)]),
            })
            .collect();

        let blob = op_codec::encode_op_batch(&ops).unwrap();
        let snapshot_hlc_val = hlc(1000);
        let tile_snapshot = TileSnapshot {
            array: "snap".into(),
            coord_range: CoordRange {
                lo: vec![CoordValue::Int64(0)],
                hi: vec![CoordValue::Int64(10)],
            },
            tile_blob: blob,
            snapshot_hlc: snapshot_hlc_val,
            schema_hlc,
        };

        // Split into 2 chunks.
        let (header, wire_chunks) = split_into_chunks(&tile_snapshot, 64).unwrap();
        assert!(
            wire_chunks.len() >= 2 || wire_chunks.len() == 1,
            "expected at least one chunk"
        );

        // Send header.
        let header_payload = zerompk::to_msgpack_vec(&header).unwrap();
        let header_msg = ArraySnapshotMsg {
            array: "snap".into(),
            header_payload,
        };
        let h_outcome = inbound.handle_snapshot_header(&header_msg).unwrap();
        assert!(matches!(
            h_outcome,
            InboundOutcome::SnapshotPartial { received: 0, .. }
        ));

        // Send all chunks; last chunk triggers assembly.
        let total = wire_chunks.len();
        let mut last_outcome = InboundOutcome::Idempotent;
        for (i, chunk) in wire_chunks.into_iter().enumerate() {
            let chunk_msg = ArraySnapshotChunkMsg {
                array: "snap".into(),
                snapshot_hlc_bytes: snapshot_hlc_val.to_bytes(),
                chunk_index: chunk.chunk_index,
                total_chunks: chunk.total_chunks,
                payload: chunk.payload,
            };
            last_outcome = inbound.handle_snapshot_chunk(&chunk_msg).unwrap();
            if i + 1 < total {
                assert!(
                    matches!(last_outcome, InboundOutcome::SnapshotPartial { .. }),
                    "expected partial while more chunks remain"
                );
            }
        }
        assert!(
            matches!(
                last_outcome,
                InboundOutcome::SnapshotApplied { ops_applied: 3 }
            ),
            "expected SnapshotApplied{{ops_applied: 3}}, got: {last_outcome:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn snapshot_partial_returns_partial() {
        let (inbound, schemas, _pending, storage) = make_inbound().await;
        schemas.put_schema("p", &simple_schema("p")).await.unwrap();
        {
            let mut state = inbound.engine.array_state.lock().await;
            state
                .create_array(&storage, "p", simple_schema("p"))
                .await
                .unwrap();
        }
        let schema_hlc = schemas.schema_hlc("p").unwrap();

        // Snapshot with 2-chunk blob.
        let ops: Vec<ArrayOp> = vec![ArrayOp {
            header: ArrayOpHeader {
                array: "p".into(),
                hlc: hlc(1),
                schema_hlc,
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: 1,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(0)],
            attrs: Some(vec![CellValue::Float64(0.0)]),
        }];
        let blob = op_codec::encode_op_batch(&ops).unwrap();
        // Make the blob large by repeating to guarantee multiple chunks.
        let big_blob: Vec<u8> = blob.into_iter().cycle().take(200).collect();
        let snapshot_hlc_val = hlc(500);
        let tile_snapshot = TileSnapshot {
            array: "p".into(),
            coord_range: CoordRange {
                lo: vec![CoordValue::Int64(0)],
                hi: vec![CoordValue::Int64(10)],
            },
            tile_blob: big_blob,
            snapshot_hlc: snapshot_hlc_val,
            schema_hlc,
        };

        let (header, wire_chunks) = split_into_chunks(&tile_snapshot, 64).unwrap();
        // Need at least 2 chunks for this test to be meaningful.
        if wire_chunks.len() < 2 {
            // Single chunk — skip; the assembly test above covers that path.
            return;
        }

        // Send header and only the first chunk.
        let header_payload = zerompk::to_msgpack_vec(&header).unwrap();
        let header_msg = ArraySnapshotMsg {
            array: "p".into(),
            header_payload,
        };
        inbound.handle_snapshot_header(&header_msg).unwrap();

        let first_chunk = &wire_chunks[0];
        let chunk_msg = ArraySnapshotChunkMsg {
            array: "p".into(),
            snapshot_hlc_bytes: snapshot_hlc_val.to_bytes(),
            chunk_index: first_chunk.chunk_index,
            total_chunks: first_chunk.total_chunks,
            payload: first_chunk.payload.clone(),
        };
        let outcome = inbound.handle_snapshot_chunk(&chunk_msg).unwrap();
        assert!(
            matches!(outcome, InboundOutcome::SnapshotPartial { .. }),
            "expected Partial while chunks remain, got: {outcome:?}"
        );
    }
}
