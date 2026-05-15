//! Real-transport array sync tests — require a live Origin node.
//!
//! Every test in this file is `#[ignore]` because array sync over the actual
//! WebSocket transport has not been validated end-to-end for 0.1.0-beta.1.
//! The in-process simulations live in `tests/array_sync_*.rs`; this file is
//! the placeholder for promotion once Origin's outbound array-delta fan-out
//! path is wired to Lite's `dispatch_frame` handler.
//!
//! ## What blocks promotion
//!
//! Origin's `session_handler.rs` dispatches inbound array messages
//! (`ArraySnapshot`, `ArraySnapshotChunk`, `ArrayCatchupRequest`, `ArraySchema`,
//! `ArrayAck`) to `OriginArrayInbound`.  The outbound path — Origin emitting
//! `ArrayDeltaMsg` / `ArrayDeltaBatchMsg` back to Lite subscribers via
//! `ArrayFanout` — is implemented in
//! `nodedb/nodedb/src/control/array_sync/outbound/`.
//!
//! Lite's `sync/client/receive.rs` does not yet match on `SyncMessageType::ArrayDelta`
//! or `SyncMessageType::ArrayDeltaBatch`; those frame types fall through to the
//! catch-all arm.  Until that receive path is wired, a round-trip over a live
//! Origin transport cannot be asserted.
//!
//! ## How to promote
//!
//! 1. Wire `SyncMessageType::ArrayDelta` and `SyncMessageType::ArrayDeltaBatch`
//!    in `nodedb-lite/nodedb-lite/src/sync/client/receive.rs`.
//! 2. Remove `#[ignore]` from the tests below and run:
//!    `cargo nextest run -p nodedb-lite array_sync_interop`
//! 3. Update `docs/lite-support-matrix.md`: change "Array sync" from
//!    EXPERIMENTAL to PREVIEW (after 1 passing real-transport gate) or BETA
//!    (after full suite passes).

mod common;

/// Smoke test: Lite pushes an array put op to Origin over WebSocket;
/// Origin applies it and echoes a delta back; Lite receives and applies it.
///
/// Ignored until `SyncMessageType::ArrayDelta` is handled in
/// `nodedb-lite/src/sync/client/receive.rs` and the Origin outbound fan-out
/// path delivers `ArrayDeltaMsg` to subscribed Lite sessions.
#[test]
#[ignore = "array sync over real Origin transport not yet wired; see module doc"]
fn array_interop_put_roundtrip() {
    let _origin = common::origin::OriginServer::spawn();
    // When unignored this test should:
    // 1. Connect a Lite sync client to _origin.sync_addr().
    // 2. Subscribe to an array shape.
    // 3. Emit a put op via the outbound path.
    // 4. Assert Lite receives an ArrayDelta frame back from Origin.
    // 5. Assert the local array engine reflects the applied cell.
    todo!(
        "implement once ArrayDelta receive path is wired in nodedb-lite/src/sync/client/receive.rs"
    );
}

/// Catch-up test: Lite connects after missing deltas; Origin sends a snapshot
/// followed by incremental deltas; Lite converges to the correct state.
///
/// Ignored for the same reason as `array_interop_put_roundtrip`.
#[test]
#[ignore = "array sync over real Origin transport not yet wired; see module doc"]
fn array_interop_catchup_after_gap() {
    let _origin = common::origin::OriginServer::spawn();
    // When unignored this test should:
    // 1. Seed Origin with array ops via a Lite client that then disconnects.
    // 2. Connect a fresh Lite client with a stale cursor.
    // 3. Assert Origin delivers ArraySnapshotMsg + ArraySnapshotChunkMsg.
    // 4. Assert the new Lite client converges to the seeded state.
    todo!(
        "implement once ArrayDelta receive path is wired in nodedb-lite/src/sync/client/receive.rs"
    );
}
