//! §7.4 — End-to-end DeltaReject with typed `CompensationHint`.
//!
//! Sends delta pushes that Origin's session handler must reject, then verifies
//! that the `DeltaReject` frame carries the expected `CompensationHint` variant.
//!
//! Each test spawns its own Origin instance with a fresh temp data dir.

mod common;

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb_types::sync::compensation::CompensationHint;
use nodedb_types::sync::wire::{DeltaPushMsg, DeltaRejectMsg, SyncFrame, SyncMessageType};
use tokio_tungstenite::tungstenite::Message;

use common::origin::{OriginServer, connect_and_handshake};

// ── helper ────────────────────────────────────────────────────────────────────

async fn push_and_recv_reject(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    msg: &DeltaPushMsg,
) -> DeltaRejectMsg {
    let bytes = SyncFrame::try_encode(SyncMessageType::DeltaPush, msg)
        .expect("encode DeltaPush")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send DeltaPush");

    let resp = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout waiting for DeltaReject")
        .expect("stream closed before response")
        .expect("WebSocket read error");

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode frame");

    assert_eq!(
        frame.msg_type,
        SyncMessageType::DeltaReject,
        "expected DeltaReject, got {:?}",
        frame.msg_type
    );

    frame
        .decode_body::<DeltaRejectMsg>()
        .expect("decode DeltaRejectMsg")
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// §7.4a — Empty delta produces a DeltaReject (no compensation hint).
#[tokio::test]
async fn empty_delta_reject_no_hint() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let msg = DeltaPushMsg {
        collection: "comp_test".into(),
        document_id: "doc-empty".into(),
        delta: Vec::new(),
        peer_id: 2001,
        mutation_id: 1,
        checksum: 0,
        device_valid_time_ms: None,
    };

    let reject = push_and_recv_reject(&mut ws, &msg).await;
    assert_eq!(reject.mutation_id, 1, "reject must echo the mutation_id");
    assert!(
        reject.compensation.is_none(),
        "empty delta rejection should carry no compensation hint, got {:?}",
        reject.compensation
    );
}

/// §7.4b — CRC32C mismatch produces `CompensationHint::IntegrityViolation`.
#[tokio::test]
async fn crc_mismatch_yields_integrity_violation_hint() {
    let _server = OriginServer::spawn();
    let mut ws = connect_and_handshake(_server.ws_url).await;

    let payload = vec![10u8, 20, 30, 40];
    let wrong_checksum = 0xDEAD_BEEFu32;

    let msg = DeltaPushMsg {
        collection: "comp_test".into(),
        document_id: "doc-crc".into(),
        delta: payload,
        peer_id: 2002,
        mutation_id: 2,
        checksum: wrong_checksum,
        device_valid_time_ms: None,
    };

    let reject = push_and_recv_reject(&mut ws, &msg).await;

    match &reject.compensation {
        Some(CompensationHint::IntegrityViolation) => {}
        other => panic!(
            "expected IntegrityViolation hint for CRC mismatch, got {:?}",
            other
        ),
    }
}

/// §7.4c — DeltaReject for an unauthenticated session carries `PermissionDenied`.
#[tokio::test]
async fn unauthenticated_push_yields_permission_denied() {
    let _server = OriginServer::spawn();

    let (mut ws, _) = tokio_tungstenite::connect_async(_server.ws_url)
        .await
        .expect("connect");

    let msg = DeltaPushMsg {
        collection: "comp_test".into(),
        document_id: "doc-unauth".into(),
        delta: vec![1, 2, 3],
        peer_id: 2003,
        mutation_id: 3,
        checksum: 0,
        device_valid_time_ms: None,
    };

    let bytes = SyncFrame::try_encode(SyncMessageType::DeltaPush, &msg)
        .expect("encode")
        .to_bytes();
    ws.send(Message::Binary(bytes.into())).await.expect("send");

    let resp = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout")
        .expect("stream closed")
        .expect("read error");

    let frame = SyncFrame::from_bytes(resp.into_data().as_ref()).expect("decode frame");

    match frame.msg_type {
        SyncMessageType::DeltaReject => {
            let reject: DeltaRejectMsg = frame.decode_body().expect("decode DeltaRejectMsg");
            match &reject.compensation {
                Some(CompensationHint::PermissionDenied) => {}
                other => panic!(
                    "unauthenticated push should produce PermissionDenied, got {:?}",
                    other
                ),
            }
        }
        other => panic!(
            "expected DeltaReject for unauthenticated push, got {:?}",
            other
        ),
    }
}

/// §7.4d — Typed hint codes surface correctly on the Lite client side.
///
/// Pure in-process check — no Origin needed for this assertion, but it is
/// co-located with the compensation tests as it validates the same type
/// invariants used by 7.4b and 7.4c.
#[tokio::test]
async fn compensation_hint_codes_round_trip() {
    let cases: &[CompensationHint] = &[
        CompensationHint::IntegrityViolation,
        CompensationHint::PermissionDenied,
        CompensationHint::UniqueViolation {
            field: "email".into(),
            conflicting_value: "x@y.com".into(),
        },
        CompensationHint::ForeignKeyMissing {
            referenced_id: "user-99".into(),
        },
        CompensationHint::RateLimited {
            retry_after_ms: 5000,
        },
    ];

    for hint in cases {
        assert!(
            !hint.code().is_empty(),
            "hint code must be non-empty for {hint:?}"
        );
    }

    assert_eq!(
        CompensationHint::IntegrityViolation.code(),
        "INTEGRITY_VIOLATION"
    );
    assert_eq!(
        CompensationHint::PermissionDenied.code(),
        "PERMISSION_DENIED"
    );
    assert_eq!(
        CompensationHint::UniqueViolation {
            field: String::new(),
            conflicting_value: String::new(),
        }
        .code(),
        "UNIQUE_VIOLATION"
    );
    assert_eq!(
        CompensationHint::RateLimited { retry_after_ms: 0 }.code(),
        "RATE_LIMITED"
    );
}
