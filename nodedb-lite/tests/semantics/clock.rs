//! §8.1 — Global-clock encoding and resume semantics.

use std::collections::HashMap;

use super::helpers::{minimal_hs, raw_connect, recv_ack, send_hs};
use crate::common::origin::OriginServer;
use nodedb_types::sync::wire::HandshakeMsg;

/// §8.1a — Lite's `_global` vector-clock encoding is accepted by Origin.
///
/// Origin computes `last_seen_lsn` as the max value across all inner maps of
/// all collection keys.  Lite sends `{ "_global": { peer_hex: counter } }`.
/// This must be accepted without error.
#[tokio::test]
async fn global_clock_encoding_is_accepted() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = raw_connect(_server.ws_url).await;

    let peer_hex = format!("{:016x}", 0xdeadbeef_u64);
    let mut inner = HashMap::new();
    inner.insert(peer_hex, 42_u64);
    let mut clock = HashMap::new();
    clock.insert("_global".to_string(), inner);

    let hs = HandshakeMsg {
        vector_clock: clock,
        ..minimal_hs()
    };

    send_hs(&mut ws, &hs).await;
    let ack = recv_ack(&mut ws).await;

    assert!(
        ack.success,
        "Origin must accept the _global clock encoding; error: {:?}",
        ack.error
    );
    assert!(
        !ack.session_id.is_empty(),
        "session_id must be non-empty on success"
    );
}

/// §8.1b — Reconnect with same or advanced global clock succeeds (no gap, no replay rejection).
///
/// Verifies that:
///   1. First connect with counter C succeeds.
///   2. Reconnect with same counter C succeeds (idempotent).
///   3. Reconnect with C+10 (post-write advance) also succeeds.
#[tokio::test]
async fn global_clock_reconnect_resumes_cleanly() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };

    let peer_hex = format!("{:016x}", 0xc1_0c_u64);
    let base_counter = 100_u64;

    // First connection.
    {
        let mut ws = raw_connect(_server.ws_url).await;
        let mut inner = HashMap::new();
        inner.insert(peer_hex.clone(), base_counter);
        let mut clock = HashMap::new();
        clock.insert("_global".to_string(), inner);
        let hs = HandshakeMsg {
            vector_clock: clock,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;
        assert!(ack.success, "first connect failed: {:?}", ack.error);
    }

    // Reconnect with same counter.
    {
        let mut ws = raw_connect(_server.ws_url).await;
        let mut inner = HashMap::new();
        inner.insert(peer_hex.clone(), base_counter);
        let mut clock = HashMap::new();
        clock.insert("_global".to_string(), inner);
        let hs = HandshakeMsg {
            vector_clock: clock,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;
        assert!(
            ack.success,
            "reconnect with same clock failed: {:?}",
            ack.error
        );
        assert!(!ack.fork_detected, "no fork when lite_id is empty");
    }

    // Reconnect with advanced counter.
    {
        let mut ws = raw_connect(_server.ws_url).await;
        let mut inner = HashMap::new();
        inner.insert(peer_hex.clone(), base_counter + 10);
        let mut clock = HashMap::new();
        clock.insert("_global".to_string(), inner);
        let hs = HandshakeMsg {
            vector_clock: clock,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;
        assert!(
            ack.success,
            "reconnect with advanced clock failed: {:?}",
            ack.error
        );
    }
}

/// §8.1c — Empty vector clock is accepted (fresh device, no prior state).
#[tokio::test]
async fn empty_clock_accepted_for_fresh_device() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = raw_connect(_server.ws_url).await;

    send_hs(&mut ws, &minimal_hs()).await;
    let ack = recv_ack(&mut ws).await;

    assert!(
        ack.success,
        "empty clock (fresh device) must be accepted; error: {:?}",
        ack.error
    );
}
