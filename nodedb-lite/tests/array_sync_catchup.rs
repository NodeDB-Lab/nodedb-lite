//! Edge-side simulation — does NOT exercise real Origin transport.
//! All tests here call Lite's inbound/outbound handlers directly, bypassing
//! the WebSocket connection to a live Origin node.
//!
//! The real-transport round-trip (Lite → Origin WebSocket → Lite) is not covered
//! by any test in this file.  See §13 of the release checklist for the decision
//! record and the placeholder real-transport test in `tests/array_sync_interop.rs`.
//!
//! Original note: The CatchupTracker + ArrayInbound snapshot path
//! (handle_snapshot_header / handle_snapshot_chunk) are exercised in-process.
//! Phases F/H (Origin catch-up server / WebSocket reconnect) are not yet
//! validated end-to-end.  Tests here simulate the catch-up scenario by:
//! 1. Marking an array as needing catch-up via `record_reject_retention_floor`.
//! 2. Shipping a synthetic snapshot via handle_snapshot_header / handle_snapshot_chunk.
//! 3. Verifying the engine state after snapshot assembly.

mod common;

use nodedb_array::sync::hlc::Hlc;
use nodedb_array::sync::op::{ArrayOp, ArrayOpHeader, ArrayOpKind};
use nodedb_array::sync::op_codec;
use nodedb_array::sync::snapshot::{CoordRange, TileSnapshot, split_into_chunks};
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_lite::sync::array::inbound::outcome::InboundOutcome;
use nodedb_types::sync::wire::array::{ArraySnapshotChunkMsg, ArraySnapshotMsg};

fn build_ops(array: &str, schema_hlc: Hlc, count: u64) -> Vec<ArrayOp> {
    let rep = common::replica(1);
    (1..=count)
        .map(|i| ArrayOp {
            header: ArrayOpHeader {
                array: array.into(),
                hlc: common::hlc(i * 100, rep),
                schema_hlc,
                valid_from_ms: 0,
                valid_until_ms: -1,
                system_from_ms: (i * 100) as i64,
            },
            kind: ArrayOpKind::Put,
            coord: vec![CoordValue::Int64(i as i64)],
            attrs: Some(vec![CellValue::Float64(i as f64 * 10.0)]),
        })
        .collect()
}

/// CatchupTracker recognises that an array needs catch-up when no
/// `last_seen_hlc` is stored (first connect scenario).
#[tokio::test(flavor = "multi_thread")]
async fn first_connect_requires_catchup() {
    let harness = common::SyncHarness::new_in_memory().await;
    harness.create_array("fresh").await;

    let local_hlc = common::hlc1(1000);
    let needs = harness.catchup.should_request_catchup("fresh", local_hlc);
    assert!(
        needs,
        "first connect must require catch-up (no last_seen_hlc)"
    );

    let req = harness.catchup.build_request("fresh");
    assert_eq!(req.array, "fresh");
    assert_eq!(
        req.from_hlc_bytes,
        Hlc::ZERO.to_bytes(),
        "first connect request must start from HLC::ZERO"
    );
}

/// After `record_reject_retention_floor`, the array is flagged as needing
/// catch-up. After a simulated snapshot apply, the flag can be cleared.
#[tokio::test(flavor = "multi_thread")]
async fn retention_floor_reject_then_catchup_clears_flag() {
    let harness = common::SyncHarness::new_in_memory().await;
    harness.create_array("rcf").await;

    harness
        .catchup
        .record_reject_retention_floor("rcf")
        .await
        .expect("record_retention_floor");

    assert!(
        harness.catchup.should_request_catchup("rcf", Hlc::ZERO),
        "array must be flagged after RetentionFloor reject"
    );

    // Simulate a successful catch-up: clear the flag.
    harness
        .catchup
        .clear_catchup_needed("rcf")
        .await
        .expect("clear_catchup_needed");

    // Record a last_seen_hlc so the "first connect" branch doesn't fire.
    harness
        .catchup
        .record("rcf", common::hlc1(500))
        .await
        .expect("record");

    assert!(
        !harness
            .catchup
            .should_request_catchup("rcf", common::hlc1(500)),
        "flag must be cleared after clear_catchup_needed"
    );
}

/// Deliver a multi-op snapshot via handle_snapshot_header + handle_snapshot_chunk.
/// All ops in the snapshot must be applied to the local engine.
#[tokio::test(flavor = "multi_thread")]
async fn snapshot_stream_applies_all_ops() {
    let harness = common::SyncHarness::new_in_memory().await;
    harness.create_array("snap").await;
    let schema_hlc = harness.schema_hlc("snap");

    let ops = build_ops("snap", schema_hlc, 5);
    let blob = op_codec::encode_op_batch(&ops).expect("encode_op_batch");

    let snapshot_hlc = common::hlc1(9999);
    let tile_snapshot = TileSnapshot {
        array: "snap".into(),
        coord_range: CoordRange {
            lo: vec![CoordValue::Int64(0)],
            hi: vec![CoordValue::Int64(100)],
        },
        tile_blob: blob,
        snapshot_hlc,
        schema_hlc,
    };

    let (header, wire_chunks) = split_into_chunks(&tile_snapshot, 64).expect("split_into_chunks");
    let total = wire_chunks.len();

    // Send header.
    let header_payload = zerompk::to_msgpack_vec(&header).expect("header encode");
    let header_msg = ArraySnapshotMsg {
        array: "snap".into(),
        header_payload,
    };
    let h_out = harness
        .inbound
        .handle_snapshot_header(&header_msg)
        .expect("handle_snapshot_header");
    assert!(
        matches!(h_out, InboundOutcome::SnapshotPartial { received: 0, .. }),
        "expected SnapshotPartial after header, got: {h_out:?}"
    );

    // Send all chunks.
    let mut last_out = h_out;
    for (i, chunk) in wire_chunks.into_iter().enumerate() {
        let chunk_msg = ArraySnapshotChunkMsg {
            array: "snap".into(),
            snapshot_hlc_bytes: snapshot_hlc.to_bytes(),
            chunk_index: chunk.chunk_index,
            total_chunks: chunk.total_chunks,
            payload: chunk.payload,
        };
        last_out = harness
            .inbound
            .handle_snapshot_chunk(&chunk_msg)
            .expect("handle_snapshot_chunk");
        if i + 1 < total {
            assert!(
                matches!(last_out, InboundOutcome::SnapshotPartial { .. }),
                "expected partial before last chunk, got: {last_out:?}"
            );
        }
    }

    assert!(
        matches!(last_out, InboundOutcome::SnapshotApplied { ops_applied: 5 }),
        "expected SnapshotApplied{{ops_applied: 5}}, got: {last_out:?}"
    );

    harness.flush("snap").await;

    // All 5 cells must be readable.
    for i in 1..=5i64 {
        let val = harness.read_coord("snap", i, i64::MAX).await;
        assert!(
            val.is_some(),
            "coord {i} must be present after snapshot apply"
        );
        assert_eq!(
            val.unwrap(),
            CellValue::Float64(i as f64 * 10.0),
            "coord {i} value must match snapshot op"
        );
    }
}

/// `CatchupTracker::record` persists across a reload from the same storage.
#[tokio::test(flavor = "multi_thread")]
async fn catchup_last_seen_persists_across_reload() {
    use nodedb_lite::PagedbStorageDefault;
    use nodedb_lite::sync::array::catchup::CatchupTracker;
    use std::sync::Arc;

    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("catchup_persist.pagedb");

    let target_hlc = common::hlc1(77_000);

    {
        let storage = Arc::new(PagedbStorageDefault::open(&path).await.expect("open"));
        let tracker = CatchupTracker::load(Arc::clone(&storage))
            .await
            .expect("load");
        tracker.record("arr", target_hlc).await.expect("record");
    }

    {
        let storage = Arc::new(PagedbStorageDefault::open(&path).await.expect("reopen"));
        let tracker = CatchupTracker::load(storage)
            .await
            .expect("load after restart");
        assert_eq!(
            tracker.last_seen("arr"),
            target_hlc,
            "last_seen_hlc must survive storage restart"
        );
    }
}
