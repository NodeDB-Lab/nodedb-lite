//! Shared helpers for §12 CRDT semantics tests.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb_types::sync::compensation::CompensationHint;
use nodedb_types::sync::wire::{
    DeltaAckMsg, DeltaPushMsg, DeltaRejectMsg, SyncFrame, SyncMessageType,
};
use tokio_tungstenite::tungstenite::Message;

pub type Ws =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// Send a `DeltaPushMsg` and wait for either `DeltaAck` or `DeltaReject`.
///
/// Returns `Ok(DeltaAckMsg)` on ack or `Err(DeltaRejectMsg)` on reject.
pub async fn push_delta(ws: &mut Ws, msg: &DeltaPushMsg) -> Result<DeltaAckMsg, DeltaRejectMsg> {
    let frame_bytes = SyncFrame::try_encode(SyncMessageType::DeltaPush, msg)
        .expect("encode DeltaPush frame")
        .to_bytes();
    ws.send(Message::Binary(frame_bytes.into()))
        .await
        .expect("send DeltaPush");

    let raw = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .expect("timeout waiting for delta response")
        .expect("stream closed before response")
        .expect("WebSocket error reading delta response");

    let frame = SyncFrame::from_bytes(raw.into_data().as_ref()).expect("decode SyncFrame");

    match frame.msg_type {
        SyncMessageType::DeltaAck => {
            let ack: DeltaAckMsg = frame.decode_body().expect("decode DeltaAckMsg");
            Ok(ack)
        }
        SyncMessageType::DeltaReject => {
            let reject: DeltaRejectMsg = frame.decode_body().expect("decode DeltaRejectMsg");
            Err(reject)
        }
        other => panic!(
            "expected DeltaAck or DeltaReject for delta push, got {:?}",
            other
        ),
    }
}

/// Assert that `push_delta` returns a `DeltaReject` with the expected
/// `CompensationHint` variant (checked via discriminant).
///
/// Returns the full `DeltaRejectMsg` for further assertions.
pub async fn expect_reject(ws: &mut Ws, msg: &DeltaPushMsg, expected_code: &str) -> DeltaRejectMsg {
    match push_delta(ws, msg).await {
        Err(reject) => {
            let actual_code = reject
                .compensation
                .as_ref()
                .map(|h| h.code())
                .unwrap_or("(none)");
            assert_eq!(
                actual_code, expected_code,
                "expected compensation code {expected_code}, got {actual_code:?} \
                 (full reject: {reject:?})"
            );
            reject
        }
        Ok(ack) => panic!(
            "expected DeltaReject with code {expected_code}, but got DeltaAck \
             (mutation_id={})",
            ack.mutation_id
        ),
    }
}

/// Build a minimal valid delta payload — enough bytes to pass the empty-delta
/// check and have a plausible CRC32C.
pub fn minimal_delta_payload() -> Vec<u8> {
    // 4 bytes: enough to not be empty; content is irrelevant for rejection
    // tests that don't reach constraint validation.
    vec![0xDE, 0xAD, 0xBE, 0xEF]
}

/// Compute the CRC32C checksum for `data` (matches Origin's check).
pub fn crc32c_of(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// A `DeltaPushMsg` with correct CRC32C over `delta`.
pub fn push_msg_with_crc(
    collection: &str,
    document_id: &str,
    mutation_id: u64,
    peer_id: u64,
    delta: Vec<u8>,
) -> DeltaPushMsg {
    let checksum = crc32c_of(&delta);
    DeltaPushMsg {
        collection: collection.into(),
        document_id: document_id.into(),
        delta,
        peer_id,
        mutation_id,
        checksum,
        device_valid_time_ms: None,
    }
}

/// A `DeltaPushMsg` with checksum=0 (legacy / skip-CRC path).
pub fn push_msg_no_crc(
    collection: &str,
    document_id: &str,
    mutation_id: u64,
    peer_id: u64,
    delta: Vec<u8>,
) -> DeltaPushMsg {
    DeltaPushMsg {
        collection: collection.into(),
        document_id: document_id.into(),
        delta,
        peer_id,
        mutation_id,
        checksum: 0,
        device_valid_time_ms: None,
    }
}

/// Assert the hint variant matches without inspecting inner fields.
pub fn assert_hint_code(hint: Option<&CompensationHint>, expected: &str) {
    let actual = hint.map(|h| h.code()).unwrap_or("(none)");
    assert_eq!(
        actual, expected,
        "expected CompensationHint code {expected}, got {actual}"
    );
}
