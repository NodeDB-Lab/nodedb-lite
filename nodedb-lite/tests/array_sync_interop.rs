//! Real-transport array sync follow-ups — `#[ignore]`d.
//!
//! `ArrayDelta` / `ArrayDeltaBatch` receive is wired and gated by
//! `tests/array_sync_interop_real.rs`. The two scenarios below — full
//! put round-trip and post-disconnect catch-up — require Origin's outbound
//! fan-out path (`ArrayFanout`) to deliver to subscribed Lite sessions and
//! are deferred until that path lands.

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
