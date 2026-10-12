//! Array catch-up fixtures: a replica holding one array, encoded delta
//! batches, and chunked snapshots in the exact shape Origin's catch-up
//! server sends them.

use std::sync::Arc;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::ArrayOp;
use nodedb_array::sync::op_codec;
use nodedb_array::sync::snapshot::{CoordRange, TileSnapshot, split_into_chunks};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::sync::wire::array::{
    ArrayDeltaBatchMsg, ArraySnapshotChunkMsg, ArraySnapshotMsg,
};

use super::clock::replica;
use super::ops::put_op;
use super::schema::simple_schema;

/// Replica id stamped on ops that stand in for Origin-relayed writes.
pub const REMOTE_REPLICA: u64 = 99;

/// Open a fresh in-memory `NodeDbLite` holding `array` (see [`simple_schema`]).
pub async fn open_lite_with_array(array: &str) -> Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    let lite = NodeDbLite::open(storage).await.expect("open");
    lite.create_array(array, simple_schema(array))
        .await
        .expect("create_array");
    lite
}

/// A remote put at coord `x` with value `x * 10.0`, stamped at `ms`.
pub fn remote_put(array: &str, x: i64, ms: u64, schema_hlc: Hlc) -> ArrayOp {
    put_op(
        array,
        x,
        x as f64 * 10.0,
        ms,
        schema_hlc,
        replica(REMOTE_REPLICA),
    )
}

/// Encode `ops` as one `ArrayDeltaBatch`, as Origin's catch-up op stream does.
pub fn delta_batch(array: &str, ops: &[ArrayOp]) -> ArrayDeltaBatchMsg {
    ArrayDeltaBatchMsg {
        array: array.into(),
        op_payloads: ops
            .iter()
            .map(|op| op_codec::encode_op(op).expect("encode_op"))
            .collect(),
    }
}

/// Split a snapshot of `ops` into the header and chunk frames Origin sends.
///
/// `chunk_bytes` is small in tests so the snapshot spans several chunks.
pub fn snapshot_frames(
    array: &str,
    ops: &[ArrayOp],
    snapshot_hlc: Hlc,
    schema_hlc: Hlc,
    chunk_bytes: usize,
) -> (ArraySnapshotMsg, Vec<ArraySnapshotChunkMsg>) {
    let snapshot = TileSnapshot {
        array: array.into(),
        coord_range: CoordRange {
            lo: vec![CoordValue::Int64(0)],
            hi: vec![CoordValue::Int64(99)],
        },
        tile_blob: op_codec::encode_op_batch(ops).expect("encode_op_batch"),
        snapshot_hlc,
        schema_hlc,
    };
    let (header, chunks) = split_into_chunks(&snapshot, chunk_bytes).expect("split_into_chunks");
    let header_msg = ArraySnapshotMsg {
        array: array.into(),
        header_payload: zerompk::to_msgpack_vec(&header).expect("encode snapshot header"),
    };
    let chunk_msgs = chunks
        .into_iter()
        .map(|chunk| ArraySnapshotChunkMsg {
            array: array.into(),
            snapshot_hlc_bytes: chunk.snapshot_hlc.to_bytes(),
            chunk_index: chunk.chunk_index,
            total_chunks: chunk.total_chunks,
            payload: chunk.payload,
        })
        .collect();
    (header_msg, chunk_msgs)
}

/// The current value at coord `x`, or `None` when the cell is absent.
pub async fn read_value(
    lite: &NodeDbLite<PagedbStorageMem>,
    array: &str,
    x: i64,
) -> Option<CellValue> {
    lite.array_read_coord(array, &[CoordValue::Int64(x)], None)
        .await
        .expect("array_read_coord")
        .and_then(|cell| cell.attrs.first().cloned())
}
