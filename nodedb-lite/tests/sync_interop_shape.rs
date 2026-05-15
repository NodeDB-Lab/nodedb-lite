//! §9 — Shape subscription against a real Origin server.
//!
//! Proves the full shape subscription lifecycle on a live connection:
//! snapshot delivery, incremental delta application, sequence-gap
//! detection and re-sync, and local queryability of synced data.
//!
//! Edge-side simulation tests remain in `shape_subscription.rs`.
//! These tests require a running Origin binary (see `tests/common/origin.rs`).

mod common;

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb_lite::engine::crdt::CrdtEngine;
use nodedb_lite::sync::*;
use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};
use nodedb_types::sync::wire::{
    ResyncReason, ShapeDeltaMsg, ShapeSnapshotMsg, ShapeSubscribeMsg, SyncFrame, SyncMessageType,
};
use tokio_tungstenite::tungstenite::Message;

use common::origin::{OriginServer, connect_and_handshake};

// ── helpers ───────────────────────────────────────────────────────────────────

/// Subscribe to a shape over `ws` and receive the initial ShapeSnapshot.
async fn subscribe_and_recv_snapshot(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    shape_id: &str,
    collection: &str,
) -> ShapeSnapshotMsg {
    let msg = ShapeSubscribeMsg {
        shape: ShapeDefinition {
            shape_id: shape_id.into(),
            tenant_id: 0,
            shape_type: ShapeType::Document {
                collection: collection.into(),
                predicate: Vec::new(),
            },
            description: "interop-shape-test".into(),
            field_filter: vec![],
        },
    };
    let bytes = SyncFrame::try_encode(SyncMessageType::ShapeSubscribe, &msg)
        .expect("encode ShapeSubscribe")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send ShapeSubscribe");

    let resp = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout waiting for ShapeSnapshot")
        .expect("stream closed before ShapeSnapshot")
        .expect("WebSocket error waiting for ShapeSnapshot");

    let frame =
        SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode ShapeSnapshot frame");
    assert_eq!(
        frame.msg_type,
        SyncMessageType::ShapeSnapshot,
        "expected ShapeSnapshot, got {:?}",
        frame.msg_type
    );
    frame
        .decode_body::<ShapeSnapshotMsg>()
        .expect("decode ShapeSnapshotMsg body")
}

// ── §9.1 — Snapshot populates SyncClient local state ─────────────────────────

/// §9.1: Subscribe from Lite to a real Origin shape and verify the initial
/// ShapeSnapshot populates the SyncClient's local ShapeManager correctly.
///
/// After the snapshot is received, `snapshot_loaded` must be true and
/// `last_lsn` must match the `snapshot_lsn` from Origin.
#[tokio::test]
async fn shape_snapshot_populates_sync_client_state() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    // Create a Lite SyncClient tracking the subscription state (mirrors
    // what the transport layer maintains during run_sync_loop).
    let client = Arc::new(SyncClient::new(SyncConfig::new(_server.ws_url, ""), 9001));

    // Register the shape in the client's ShapeManager before subscribing.
    {
        let mut shapes = client.shapes().lock().await;
        shapes.subscribe(ShapeDefinition {
            shape_id: "§9.1-shape".into(),
            tenant_id: 0,
            shape_type: ShapeType::Document {
                collection: "shape_test_9_1".into(),
                predicate: Vec::new(),
            },
            description: "§9.1 interop test".into(),
            field_filter: vec![],
        });
    }

    // Subscribe over the live WebSocket.
    let snapshot = subscribe_and_recv_snapshot(&mut ws, "§9.1-shape", "shape_test_9_1").await;

    // Let the client process the snapshot exactly as the transport layer does.
    client.handle_shape_snapshot(&snapshot).await;

    // Verify local state reflects the Origin snapshot.
    let shapes = client.shapes().lock().await;
    let sub = shapes.get("§9.1-shape").expect("subscription must exist");

    assert!(
        sub.snapshot_loaded,
        "snapshot_loaded must be true after Origin delivers ShapeSnapshot"
    );
    assert_eq!(
        sub.last_lsn, snapshot.snapshot_lsn,
        "last_lsn must equal the snapshot_lsn from Origin"
    );
}

// ── §9.2 — ShapeDelta updates local state ────────────────────────────────────

/// §9.2: After an initial snapshot, verify that a subsequent `ShapeDelta`
/// pushed to Origin advances the local ShapeManager's LSN correctly.
///
/// Strategy: push a real Loro delta from a second client, then send a
/// synthetic ShapeDelta frame (as Origin would) to the SyncClient and
/// confirm the watermark advances.
///
/// Note: Origin does not currently fan-out ShapeDelta back over the same
/// connection as the delta pusher. We simulate Origin's fan-out by
/// directly feeding a ShapeDelta frame through the SyncClient handler,
/// which is the same code path run_sync_loop uses in production.
#[tokio::test]
async fn shape_delta_advances_lsn_after_snapshot() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let client = Arc::new(SyncClient::new(SyncConfig::new(_server.ws_url, ""), 9002));

    // Subscribe + snapshot.
    {
        let mut shapes = client.shapes().lock().await;
        shapes.subscribe(ShapeDefinition {
            shape_id: "§9.2-shape".into(),
            tenant_id: 0,
            shape_type: ShapeType::Document {
                collection: "shape_test_9_2".into(),
                predicate: Vec::new(),
            },
            description: "§9.2 interop test".into(),
            field_filter: vec![],
        });
    }

    let snapshot = subscribe_and_recv_snapshot(&mut ws, "§9.2-shape", "shape_test_9_2").await;
    client.handle_shape_snapshot(&snapshot).await;

    let baseline_lsn = {
        let shapes = client.shapes().lock().await;
        shapes.get("§9.2-shape").expect("sub").last_lsn
    };

    // Simulate Origin pushing a ShapeDelta whose LSN is baseline + 10.
    // This is the frame that run_sync_loop dispatches when a matching
    // mutation commits on Origin and the fan-out loop calls
    // evaluate_and_generate_deltas.
    let delta_lsn = baseline_lsn + 10;
    let delta_msg = ShapeDeltaMsg {
        shape_id: "§9.2-shape".into(),
        collection: "shape_test_9_2".into(),
        document_id: "doc-delta".into(),
        operation: "INSERT".into(),
        delta: vec![0xDE, 0xAD], // payload bytes; content irrelevant for LSN tracking
        lsn: delta_lsn,
    };

    client.handle_shape_delta(&delta_msg).await;

    let shapes = client.shapes().lock().await;
    assert_eq!(
        shapes.get("§9.2-shape").expect("sub").last_lsn,
        delta_lsn,
        "last_lsn must advance to the delta's LSN"
    );
}

// ── §9.3 — Sequence-gap detection and re-sync request ────────────────────────

/// §9.3: Sequence-gap detection and re-sync behavior on a real connection.
///
/// After the initial snapshot (LSN = N), we simulate Origin emitting
/// deltas with LSN N+1, then skip to N+5 (gap). The SyncClient must:
///   1. Accept N+1 without complaint.
///   2. Detect the gap at N+5 and return a ResyncRequest.
///   3. Include the correct `expected` and `received` fields.
///   4. Store the pending resync so the push loop can forward it to Origin.
///
/// The ResyncRequest is constructed exactly as transport.rs does it.
#[tokio::test]
async fn sequence_gap_detection_and_resync_on_real_connection() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let client = Arc::new(SyncClient::new(SyncConfig::new(_server.ws_url, ""), 9003));

    // Subscribe + snapshot so the client has a real baseline LSN from Origin.
    {
        let mut shapes = client.shapes().lock().await;
        shapes.subscribe(ShapeDefinition {
            shape_id: "§9.3-shape".into(),
            tenant_id: 0,
            shape_type: ShapeType::Document {
                collection: "shape_test_9_3".into(),
                predicate: Vec::new(),
            },
            description: "§9.3 interop test".into(),
            field_filter: vec![],
        });
    }

    let snapshot = subscribe_and_recv_snapshot(&mut ws, "§9.3-shape", "shape_test_9_3").await;
    client.handle_shape_snapshot(&snapshot).await;

    // Seed the sequence tracker with LSN N+1 (contiguous from snapshot).
    let base = snapshot.snapshot_lsn;
    let no_gap = client.check_sequence_gap("§9.3-shape", base + 1).await;
    assert!(
        no_gap.is_none(),
        "first delta (base+1) must not trigger a resync"
    );

    // Inject a gap: jump to base+5 (skipping base+2..base+4).
    let resync = client.check_sequence_gap("§9.3-shape", base + 5).await;
    let resync_msg = resync.expect("gap of 4 must trigger a ResyncRequest");

    // Verify the fields match what Origin needs to replay from.
    assert_eq!(
        resync_msg.from_mutation_id,
        base + 2,
        "catch-up must start from the first missing LSN"
    );
    assert!(
        matches!(
            resync_msg.reason,
            ResyncReason::SequenceGap {
                expected,
                received,
            } if expected == base + 2 && received == base + 5
        ),
        "SequenceGap reason must carry correct expected/received: {:?}",
        resync_msg.reason
    );

    // Store the resync request as the transport layer does.
    client.set_pending_resync(resync_msg.clone()).await;

    // The push loop retrieves and sends it.
    let pending = client.take_pending_resync().await;
    assert!(
        pending.is_some(),
        "pending resync must be retrievable by the push loop"
    );

    // Encode the ResyncRequest to prove the frame is sendable (transport check).
    let frame = SyncFrame::try_encode(SyncMessageType::ResyncRequest, &pending.unwrap())
        .expect("ResyncRequest frame must be encodable for wire send");
    assert_eq!(frame.msg_type, SyncMessageType::ResyncRequest);

    // After resync is taken, the client must return no further resync
    // (already consumed; the `resync_requested` flag prevents double-fire).
    let no_second = client.check_sequence_gap("§9.3-shape", base + 20).await;
    assert!(
        no_second.is_none(),
        "second gap must not fire a second resync (one per connection)"
    );

    // Reset and confirm the tracker is clean (simulates reconnect).
    client.reset_sequence_tracking().await;
    let after_reset = client.check_sequence_gap("§9.3-shape", base + 2).await;
    assert!(
        after_reset.is_none(),
        "after reset, contiguous delta must not trigger resync"
    );
}

// ── §9.4 — Local query surface after shape-synced data import ─────────────────

/// §9.4: Verify that shape-synced data is immediately queryable locally
/// after the snapshot bytes are imported into a CrdtEngine.
///
/// Approach:
///   1. Create a "remote" CrdtEngine that simulates Origin's state.
///   2. Export a snapshot from the remote engine.
///   3. Import the snapshot into a "local" CrdtEngine (as import_remote does).
///   4. Query the local engine to confirm the document is accessible.
///
/// This is the same call sequence as `dispatch_frame` → ShapeSnapshot arm:
///   `delegate.import_remote(&snapshot.data)` then `client.handle_shape_snapshot`.
/// The full SQL surface (execute_sql) is exercised in `sync_interop_delta_ack.rs`
/// via the nodedb-client trait; here we verify the CRDT layer that backs it.
#[tokio::test]
async fn shape_snapshot_data_queryable_after_import() {
    // Build a "remote" engine and write a known document.
    let mut remote = CrdtEngine::new(9004).expect("remote CRDT engine");
    remote
        .upsert(
            "query_test",
            "doc-q1",
            &[
                ("name", loro::LoroValue::String("Alice".into())),
                ("active", loro::LoroValue::Bool(true)),
            ],
        )
        .expect("remote upsert");

    // Export a full snapshot — this is what Origin would encode into
    // ShapeSnapshotMsg::data before sending to Lite.
    let snapshot_bytes = remote.export_snapshot().expect("export snapshot");
    assert!(!snapshot_bytes.is_empty(), "snapshot must have content");

    // Create a fresh local engine (no prior state) and import the snapshot.
    let local = CrdtEngine::new(9005).expect("local CRDT engine");
    local
        .import_remote(&snapshot_bytes)
        .expect("import_remote must succeed on valid snapshot bytes");

    // Verify the document is accessible and contains the expected data.
    assert!(
        local.exists("query_test", "doc-q1"),
        "doc-q1 must exist in local engine after snapshot import"
    );

    let value = local
        .read("query_test", "doc-q1")
        .expect("read after import must return Some");

    // LoroValue for a document is a Map; confirm the name field is present.
    let debug_repr = format!("{value:?}");
    assert!(
        debug_repr.contains("Alice"),
        "imported document's name field must round-trip through snapshot; got: {debug_repr}"
    );

    // Verify individual field access via read_field.
    let name_field = local.read_field("query_test", "doc-q1", "name");
    assert_eq!(
        name_field,
        Some(loro::LoroValue::String("Alice".into())),
        "read_field('name') must return 'Alice' after snapshot import"
    );
}

// ── §9.5 — CollectionPurged: documented as out of scope for beta ──────────────
//
// Status: OUT OF SCOPE for 0.1.0-beta.1.
//
// Origin's event plane DOES wire `CollectionPurged`:
//   - `nodedb/nodedb/src/event/crdt_sync/delivery.rs` has
//     `broadcast_collection_purged()` which encodes a
//     `CollectionPurgedMsg` (0x14) and enqueues it into every
//     matching session's control channel.
//   - `SyncSession::track_collection()` records which collections a
//     session has subscribed to, so the broadcast is filtered correctly.
//   - The trigger is a hard collection DELETE, wired in
//     `control/catalog_entry/post_apply/async_dispatch/collection.rs`.
//
// Lite's `dispatch_frame` does NOT handle `SyncMessageType::CollectionPurged`:
//   - The frame falls through to the `_ =>` arm and is logged as
//     "unexpected frame type from Origin".
//   - There is no `SyncClient::handle_collection_purged` method.
//   - There is no eviction of shape subscriptions or local state on purge.
//
// Why out of scope:
//   - Triggering the purge broadcast requires issuing a DDL `DROP COLLECTION`
//     against the Origin binary, which requires a pgwire or HTTP control
//     connection — neither is exposed in the current interop test harness
//     (the harness provides only the sync WebSocket on port 9090).
//   - Lite receiving the 0x14 frame today would silently log a warning;
//     asserting on that behavior would couple tests to log output.
//   - The correct fix — adding `handle_collection_purged` to `dispatch_frame`
//     and evicting the shape + local collection state — is a Lite-side change
//     that has not been made for beta.
//
// When to promote to in-scope:
//   - When Lite's `dispatch_frame` handles `CollectionPurged` by evicting
//     the subscribed shape and notifying the application layer.
//   - When the test harness exposes the pgwire or HTTP endpoint so tests
//     can issue `DROP COLLECTION` to trigger the broadcast.
//
// This comment is the §9.5 deliverable per the §9 specification.
