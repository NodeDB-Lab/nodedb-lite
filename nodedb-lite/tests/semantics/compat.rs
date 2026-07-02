//! §8.2 — Compatibility sentinel tests.
//!
//! These tests assert exact accepted/rejected wire-version values and exact
//! required handshake ack fields.  If Origin changes its constants or field
//! shape without a corresponding Lite update, these fail loudly.

use super::helpers::{minimal_hs, raw_connect, recv_ack, send_hs};
use crate::common::origin::OriginServer;
use nodedb_types::sync::wire::HandshakeMsg;
use nodedb_types::wire_version::WIRE_FORMAT_VERSION;

/// §8.2a — WIRE_FORMAT_VERSION == 7 and is accepted by Origin.
///
/// If Origin bumps its constant without updating Lite (or vice versa), this
/// test fails with a message pointing at the constant.
#[tokio::test]
async fn exact_wire_version_7_is_accepted() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = raw_connect(_server.ws_url).await;

    assert_eq!(
        WIRE_FORMAT_VERSION, 7,
        "Lite WIRE_FORMAT_VERSION drifted from 7; update this test and the protocol doc"
    );

    let hs = HandshakeMsg {
        wire_version: 7,
        ..minimal_hs()
    };
    send_hs(&mut ws, &hs).await;
    let ack = recv_ack(&mut ws).await;

    assert!(
        ack.success,
        "wire_version=7 must be accepted; error: {:?}",
        ack.error
    );
    assert_eq!(
        ack.server_wire_version, 7,
        "server_wire_version must be 7; if this fails Origin bumped its constant without updating Lite"
    );
}

/// §8.2b — Wire version 0 (missing field / ancient client) must be rejected.
#[tokio::test]
async fn wire_version_zero_is_rejected() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = raw_connect(_server.ws_url).await;

    let hs = HandshakeMsg {
        wire_version: 0,
        ..minimal_hs()
    };
    send_hs(&mut ws, &hs).await;
    let ack = recv_ack(&mut ws).await;

    assert!(
        !ack.success,
        "wire_version=0 must be rejected; if this passes Origin loosened its version check"
    );
    let err = ack
        .error
        .expect("error field must be present on version rejection");
    assert!(
        err.contains("wire version") || err.contains("incompatible"),
        "error must mention wire version incompatibility; got: {err}"
    );
}

/// §8.2c — Wire version 6 (one below current floor) must be rejected.
///
/// MIN_WIRE_FORMAT_VERSION == WIRE_FORMAT_VERSION == 7.  Any version below 7
/// must fail.
#[tokio::test]
async fn wire_version_6_is_rejected() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = raw_connect(_server.ws_url).await;

    let hs = HandshakeMsg {
        wire_version: 6,
        ..minimal_hs()
    };
    send_hs(&mut ws, &hs).await;
    let ack = recv_ack(&mut ws).await;

    assert!(
        !ack.success,
        "wire_version=6 must be rejected (floor is 7); if this passes Origin relaxed MIN_WIRE_FORMAT_VERSION"
    );
}

/// §8.2d — Exact ack shape on success: session_id non-empty, error None,
/// fork_detected false, server_wire_version >= 1.
#[tokio::test]
async fn ack_shape_on_success() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = raw_connect(_server.ws_url).await;

    send_hs(&mut ws, &minimal_hs()).await;
    let ack = recv_ack(&mut ws).await;

    assert!(ack.success, "handshake should succeed");
    assert!(
        !ack.session_id.is_empty(),
        "session_id must be non-empty on success"
    );
    assert!(
        ack.error.is_none(),
        "error must be None on success, got: {:?}",
        ack.error
    );
    assert!(
        !ack.fork_detected,
        "fork_detected must be false for empty lite_id"
    );
    assert!(
        ack.server_wire_version >= 1,
        "server_wire_version must be >= 1"
    );
}

/// §8.2e — Exact ack shape on rejection: success=false, error=Some, session_id echoed.
#[tokio::test]
async fn ack_shape_on_rejection() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = raw_connect(_server.ws_url).await;

    let hs = HandshakeMsg {
        wire_version: 0,
        ..minimal_hs()
    };
    send_hs(&mut ws, &hs).await;
    let ack = recv_ack(&mut ws).await;

    assert!(!ack.success);
    assert!(ack.error.is_some(), "error field must be Some on rejection");
    // session_id is echoed by Origin even on failure.
    assert!(
        !ack.session_id.is_empty(),
        "session_id is echoed by Origin even on rejection"
    );
}
