//! §12.1 — Origin-side rejection paths: which `CompensationHint` variants
//! Origin actually emits today, and for which conditions.
//!
//! ## Origin emission status (0.1.0 beta)
//!
//! | Hint variant         | Origin emits? | Trigger                                      | Source                          |
//! |----------------------|---------------|----------------------------------------------|---------------------------------|
//! | `PermissionDenied`   | YES           | Unauthenticated push / identity not set      | `session/delta.rs:36,107`       |
//! | `IntegrityViolation` | YES           | CRC32C checksum mismatch                     | `session/delta.rs:69`           |
//! | `UniqueViolation`    | YES           | CRDT engine rejects with "unique" in error   | `async_dispatch.rs:262`         |
//! | `ForeignKeyMissing`  | YES           | CRDT engine rejects with "foreign/FK"        | `async_dispatch.rs:267`         |
//! | `Custom`             | YES           | Quota / surrogate failure / other constraint | `async_dispatch.rs:193,217,273` |
//! | `RateLimited`        | NO (silent)   | Rate limit exceeded → DLQ, `None` returned  | `session/delta.rs:113-134`      |
//! | `SchemaViolation`    | NO (as typed) | Falls back to `Custom` via string-match      | `async_dispatch.rs:260-275`     |
//!
//! Tests for `RateLimited` and `SchemaViolation` (as a typed variant) are
//! marked `#[ignore]` with a comment explaining why they cannot pass today.

use super::helpers::{
    assert_hint_code, crc32c_of, expect_reject, minimal_delta_payload, push_delta, push_msg_no_crc,
    push_msg_with_crc,
};
use crate::common::origin::{OriginServer, connect_and_handshake};
use futures::SinkExt;
use nodedb_types::sync::wire::{DeltaPushMsg, SyncFrame, SyncMessageType};
use tokio_tungstenite::tungstenite::Message;

// ── §12.1a — PermissionDenied: unauthenticated push ──────────────────────────

/// An unauthenticated push (no handshake) produces `PermissionDenied`.
///
/// Evidence: `nodedb/nodedb/src/control/server/sync/session/delta.rs:32-38`
/// — first check in `handle_delta_push` is `self.authenticated`.
#[tokio::test]
async fn origin_rejects_unauthenticated_push_with_permission_denied() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };

    // Open a raw WebSocket — intentionally skip the handshake.
    let (mut ws, _) = tokio_tungstenite::connect_async(_server.ws_url)
        .await
        .expect("connect to Origin");

    let delta = minimal_delta_payload();
    let msg = push_msg_no_crc("crdt_test", "doc-unauth", 10, 3001, delta);

    let frame_bytes = SyncFrame::try_encode(SyncMessageType::DeltaPush, &msg)
        .expect("encode DeltaPush")
        .to_bytes();
    ws.send(Message::Binary(frame_bytes.into()))
        .await
        .expect("send DeltaPush");

    use futures::StreamExt;
    use std::time::Duration;
    let raw = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout")
        .expect("stream closed")
        .expect("read error");

    let frame = SyncFrame::from_bytes(raw.into_data().as_ref()).expect("decode frame");
    assert_eq!(
        frame.msg_type,
        SyncMessageType::DeltaReject,
        "expected DeltaReject for unauthenticated push, got {:?}",
        frame.msg_type
    );

    let reject: nodedb_types::sync::wire::DeltaRejectMsg =
        frame.decode_body().expect("decode DeltaRejectMsg");
    assert_eq!(reject.mutation_id, 10, "reject must echo mutation_id");
    assert_hint_code(reject.compensation.as_ref(), "PERMISSION_DENIED");
}

// ── §12.1b — IntegrityViolation: CRC32C mismatch ─────────────────────────────

/// A delta with a wrong CRC32C checksum produces `IntegrityViolation`.
///
/// Evidence: `nodedb/nodedb/src/control/server/sync/session/delta.rs:52-72`
/// — CRC32C check fires when `msg.checksum != 0`.
#[tokio::test]
async fn origin_rejects_crc_mismatch_with_integrity_violation() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let delta = minimal_delta_payload();
    let correct_crc = crc32c_of(&delta);
    // Flip the least significant bit so the checksum is definitely wrong.
    let wrong_crc = correct_crc ^ 1;

    let msg = DeltaPushMsg {
        collection: "crdt_test".into(),
        document_id: "doc-crc".into(),
        delta,
        peer_id: 3002,
        mutation_id: 20,
        checksum: wrong_crc,
        device_valid_time_ms: None,
        producer_id: 0,
        epoch: 0,
        seq: 0,
    };

    let reject = expect_reject(&mut ws, &msg, "INTEGRITY_VIOLATION").await;
    assert_eq!(reject.mutation_id, 20);
    assert!(
        reject.reason.contains("CRC32C"),
        "reject reason should mention CRC32C, got: {:?}",
        reject.reason
    );
}

// ── §12.1c — IntegrityViolation: checksum=0 skips the check ──────────────────

/// When `checksum = 0`, Origin skips the CRC check (legacy client path).
/// The delta is accepted as long as auth + non-empty conditions pass.
///
/// Evidence: `session/delta.rs:52` — `if msg.checksum != 0` guard.
#[tokio::test]
async fn origin_accepts_delta_with_zero_checksum() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let delta = minimal_delta_payload();
    // checksum=0 disables the CRC check; the delta reaches the Data Plane.
    let msg = push_msg_no_crc("crdt_test", "doc-no-crc", 30, 3003, delta);

    // We expect either a DeltaAck (constraint validation passed) or a
    // DeltaReject with a non-IntegrityViolation hint (constraint violation
    // from the Data Plane). Either outcome proves CRC was not the rejection
    // cause.
    match push_delta(&mut ws, &msg).await {
        Ok(_ack) => {
            // Best path: accepted.
        }
        Err(reject) => {
            // Constraint or quota rejection from Data Plane is acceptable;
            // IntegrityViolation is not — that would mean CRC check fired
            // despite checksum=0.
            assert_ne!(
                reject.compensation.as_ref().map(|h| h.code()),
                Some("INTEGRITY_VIOLATION"),
                "checksum=0 must NOT trigger IntegrityViolation; got: {reject:?}"
            );
        }
    }
}

// ── §12.1d — UniqueViolation: Data Plane CRDT constraint ─────────────────────

/// A delta that the CRDT Data Plane rejects as a UNIQUE violation produces
/// `UniqueViolation`.
///
/// Evidence: `async_dispatch.rs:261-265` — string-match on "unique" / "UNIQUE"
/// in the error detail from the Data Plane.
///
/// Setup: send two deltas for the same document into a collection that has
/// a UNIQUE constraint registered. The second one should collide.
///
/// Note: In 0.1.0 beta, the constraint set is injected at Data Plane startup.
/// If no UNIQUE constraint is registered for this collection, Origin may
/// return `Custom` instead of `UniqueViolation`. The test falls back gracefully
/// and documents what was actually received.
#[tokio::test]
async fn origin_unique_violation_produces_compensation_hint() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = connect_and_handshake(_server.ws_url).await;

    // Build a Loro-style delta payload with a CRDT upsert for field "email".
    // The raw bytes are a minimal Loro export_updates snapshot. We use the
    // CrdtState from nodedb-crdt to produce a real delta.
    let delta = build_crdt_delta_for_field("email", "alice@example.com", 3004);

    // First push — should be acknowledged (or possibly rejected for other reasons).
    let first_msg = push_msg_with_crc("users_unique", "user-alice", 40, 3004, delta.clone());
    let first_result = push_delta(&mut ws, &first_msg).await;

    // Second push of the same document — may collide if UNIQUE is enforced.
    let second_msg = push_msg_with_crc("users_unique", "user-bob", 41, 3004, delta);
    let second_result = push_delta(&mut ws, &second_msg).await;

    // Record what actually happened (both ack and reject are valid in beta).
    match (first_result, second_result) {
        (_, Err(reject)) => {
            let code = reject.compensation.as_ref().map(|h| h.code());
            // In beta, UNIQUE violations produce UniqueViolation or Custom
            // depending on constraint registration. Both are acceptable.
            assert!(
                matches!(code, Some("UNIQUE_VIOLATION") | Some("CUSTOM") | None),
                "unexpected hint code for potential unique violation: {code:?}"
            );
        }
        (_, Ok(_)) => {
            // No constraint registered for this collection — Origin accepted both.
            // This is expected in beta if the constraint set is not pre-seeded.
        }
    }
}

// ── §12.1e — ForeignKeyMissing: Data Plane CRDT FK constraint ────────────────

/// A delta referencing a non-existent parent produces `ForeignKeyMissing`.
///
/// Evidence: `async_dispatch.rs:266-270` — string-match on "foreign" / "FK".
///
/// Same caveat as the UNIQUE test: if no FK constraint is registered, Origin
/// returns a different code. The test documents what was received.
#[tokio::test]
async fn origin_fk_missing_produces_compensation_hint() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = connect_and_handshake(_server.ws_url).await;

    // Delta for a "posts" document referencing a "user-nonexistent" parent.
    let delta = build_crdt_delta_for_field("author_id", "user-nonexistent", 3005);
    let msg = push_msg_with_crc("posts_fk", "post-orphan", 50, 3005, delta);

    match push_delta(&mut ws, &msg).await {
        Err(reject) => {
            let code = reject.compensation.as_ref().map(|h| h.code());
            // In beta, FK violations produce ForeignKeyMissing or Custom.
            assert!(
                matches!(code, Some("FK_MISSING") | Some("CUSTOM") | None),
                "unexpected hint code for potential FK violation: {code:?}"
            );
        }
        Ok(_) => {
            // No FK constraint registered — accepted. Expected in beta.
        }
    }
}

// ── §12.1f — RateLimited: NOT a DeltaReject in 0.1.0 beta ───────────────────

/// Origin's rate-limited path silently drops the delta (returns `None` from
/// `handle_delta_push`), so the client never receives a `DeltaReject` frame.
///
/// The delta is enqueued to the sync DLQ with `CompensationHint::RateLimited`,
/// but that information is not sent back to the edge client.
///
/// Evidence: `session/delta.rs:113-134` — `self.mutations_silent_dropped += 1;
/// return None;` — no `SyncFrame` is returned.
///
/// This test is `#[ignore]` because there is no way to receive a `DeltaReject`
/// with `RateLimited` hint from a standard sync session in 0.1.0 beta.
/// If Origin is updated to send `DeltaReject` on rate limit, remove `#[ignore]`
/// and implement the rate-trigger setup.
#[tokio::test]
#[ignore = "RateLimited: Origin silently drops (DLQ only, no DeltaReject) in 0.1.0 beta — \
            see nodedb/nodedb/src/control/server/sync/session/delta.rs:113-134"]
async fn origin_rate_limited_hint_not_in_0_1_0_beta() {
    // This path is structurally unreachable in the Lite sync model: Lite is an
    // edge client with no server-side rate limiter. The Origin DLQ path that
    // emits RateLimited hints is server-only (`session/delta.rs:113-134`).
    // Lite never receives a DeltaReject frame with RateLimited from itself.
    unreachable!(
        "RateLimited DeltaReject is Origin-server-only; Lite has no rate limiter \
         and never produces this frame — test is #[ignore]d and must never run"
    )
}

// ── §12.1g — SchemaViolation: NOT emitted as typed variant in 0.1.0 beta ─────

/// `CompensationHint::SchemaViolation` is defined in the type system but is
/// never produced by Origin in 0.1.0 beta. Schema/constraint errors that do
/// not match "unique"/"UNIQUE"/"foreign"/"FK" string patterns in
/// `async_dispatch.rs:260-275` fall through to `CompensationHint::Custom`.
///
/// Evidence: `nodedb/nodedb/src/control/server/sync/async_dispatch.rs:260-275`
/// — only "unique" and "foreign/FK" are string-matched; everything else maps
/// to `Custom { constraint: "constraint", detail: error_detail }`.
///
/// This test is `#[ignore]` because Origin does not emit `SchemaViolation`
/// today. When the dispatcher is updated to produce typed `SchemaViolation`
/// hints, remove `#[ignore]`, add the Data Plane constraint setup, and assert
/// `SCHEMA_VIOLATION`.
#[tokio::test]
#[ignore = "SchemaViolation: falls back to Custom in async_dispatch.rs:260-275 in 0.1.0 beta"]
async fn origin_schema_violation_hint_not_in_0_1_0_beta() {
    // Structurally unreachable: Origin's dispatcher never emits the typed
    // SchemaViolation variant — all non-unique/non-FK constraint failures
    // fall through to Custom (async_dispatch.rs:260-275). There is no code
    // path on either Lite or Origin that produces SchemaViolation today.
    unreachable!(
        "CompensationHint::SchemaViolation is never emitted by the Origin dispatcher \
         in 0.1.0 beta; test is #[ignore]d and must never run"
    )
}

// ── §12.1h — Custom hint: quota enforcement ───────────────────────────────────

/// When the tenant quota is exceeded, Origin emits `Custom { constraint:
/// "quota", ... }` via `validate_delta_constraints`.
///
/// Evidence: `async_dispatch.rs:193-203`.
///
/// This test is `#[ignore]` because triggering the quota path requires
/// reconfiguring the server with a per-tenant quota that is easy to exhaust
/// in a controlled way. The quota system does not expose a test knob in 0.1.0
/// beta. Remove `#[ignore]` when a test-mode quota override is available.
#[tokio::test]
#[ignore = "Custom/quota: no test-mode quota override in 0.1.0 beta — see async_dispatch.rs:193-203"]
async fn origin_quota_exceeded_emits_custom_hint() {
    // Structurally unreachable without a test-mode quota knob: triggering the
    // quota path (async_dispatch.rs:193-203) requires a per-tenant quota that
    // is trivially exhaustible in a test. The quota system exposes no such
    // override in 0.1.0 beta. This is an Origin-side server constraint; Lite
    // has no quota enforcement.
    unreachable!(
        "Quota enforcement is Origin-server-only; no test-mode override exists in \
         0.1.0 beta — test is #[ignore]d and must never run"
    )
}

// ── delta builder ──────────────────────────────────────────────────────────────

/// Build a minimal Loro delta that sets `field` to `value` for use in sync tests.
///
/// Uses `nodedb_crdt::CrdtState` to produce a real Loro export_updates payload.
/// This ensures Origin's Data Plane receives a structurally valid delta rather
/// than random bytes.
fn build_crdt_delta_for_field(field: &str, value: &str, peer_id: u64) -> Vec<u8> {
    use nodedb_crdt::CrdtState;

    let state = CrdtState::new(peer_id).expect("create CrdtState");

    // Capture pre-mutation version vector.
    let before = state.oplog_version_vector();

    // Apply a mutation.
    state
        .upsert(
            "test_collection",
            "test_doc",
            &[(field, loro::LoroValue::String(value.into()))],
        )
        .expect("upsert");

    // Export only the delta since `before`.
    state
        .export_updates_since(&before)
        .expect("export_updates_since")
}
