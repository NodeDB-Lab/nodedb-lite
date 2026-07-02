//! §8.3 — Fork detection scenarios (lite_id + epoch).

use super::helpers::{minimal_hs, raw_connect, recv_ack, send_hs};
use crate::common::origin::OriginServer;
use nodedb_types::sync::wire::HandshakeMsg;

/// §8.3a — Same `lite_id`, bumped `epoch` → legitimate reconnect, no fork.
#[tokio::test]
async fn bumped_epoch_is_not_a_fork() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let lite_id = "test-lite-id-bumped-epoch-a1b2c3d4".to_string();

    // First connect: epoch=1.
    {
        let mut ws = raw_connect(_server.ws_url).await;
        let hs = HandshakeMsg {
            lite_id: lite_id.clone(),
            epoch: 1,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;
        assert!(ack.success, "epoch=1 must succeed; error: {:?}", ack.error);
        assert!(!ack.fork_detected, "epoch=1 must not trigger fork");
    }

    // Second connect: epoch=2 (bumped) → accepted, no fork.
    {
        let mut ws = raw_connect(_server.ws_url).await;
        let hs = HandshakeMsg {
            lite_id: lite_id.clone(),
            epoch: 2,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;
        assert!(
            ack.success,
            "bumped epoch=2 must succeed; error: {:?}",
            ack.error
        );
        assert!(
            !ack.fork_detected,
            "bumped epoch must NOT trigger fork_detected"
        );
    }
}

/// §8.3b — Same `lite_id` + same `epoch` reconnect is an idempotent producer resume,
/// NOT a fork (clone-with-stale-epoch is caught by `lower_epoch_than_seen_triggers_fork`;
/// same-epoch divergence is handled by the seq gate).
#[tokio::test]
async fn same_epoch_reconnect_is_idempotent_accept() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let lite_id = "test-lite-id-idempotent-reconnect-x9y8z7".to_string();

    // Register lite_id+epoch on Origin.
    {
        let mut ws = raw_connect(_server.ws_url).await;
        let hs = HandshakeMsg {
            lite_id: lite_id.clone(),
            epoch: 5,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;
        assert!(
            ack.success,
            "initial registration must succeed; error: {:?}",
            ack.error
        );
    }

    // Second connection with same lite_id + epoch → idempotent accept, not a fork.
    {
        let mut ws = raw_connect(_server.ws_url).await;
        let hs = HandshakeMsg {
            lite_id: lite_id.clone(),
            epoch: 5,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;

        assert!(
            ack.success,
            "same lite_id+epoch reconnect must be accepted as idempotent resume; error: {:?}",
            ack.error
        );
        assert!(
            !ack.fork_detected,
            "fork_detected must be false for same-epoch idempotent reconnect"
        );
    }
}

/// §8.3c — Stale epoch (lower than last seen) → also triggers fork detection.
#[tokio::test]
async fn lower_epoch_than_seen_triggers_fork() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let lite_id = "test-lite-id-stale-epoch-p5q6r7s8".to_string();

    // Register at epoch=10.
    {
        let mut ws = raw_connect(_server.ws_url).await;
        let hs = HandshakeMsg {
            lite_id: lite_id.clone(),
            epoch: 10,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;
        assert!(ack.success, "epoch=10 must succeed");
    }

    // Connect with epoch=9 (stale backup restore) → fork.
    {
        let mut ws = raw_connect(_server.ws_url).await;
        let hs = HandshakeMsg {
            lite_id: lite_id.clone(),
            epoch: 9,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;

        assert!(!ack.success, "epoch=9 after epoch=10 must be rejected");
        assert!(
            ack.fork_detected,
            "fork_detected must be true for stale epoch"
        );
    }
}

/// §8.3d — Clean reconnect after Origin restart: epoch tracker is cleared in
/// memory, so the same lite_id+epoch is accepted on the fresh process.
#[tokio::test]
async fn reconnect_after_origin_restart_not_a_fork() {
    let lite_id = "test-lite-id-restart-scenario-z0a1b2".to_string();
    let epoch = 7_u64;

    {
        let Some(server) = OriginServer::try_spawn() else {
            eprintln!(
                "SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)"
            );
            return;
        };
        let mut ws = raw_connect(server.ws_url).await;
        let hs = HandshakeMsg {
            lite_id: lite_id.clone(),
            epoch,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;
        assert!(ack.success, "pre-restart registration must succeed");
    } // server killed here.

    // Fresh Origin process: tracker is empty.
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = raw_connect(_server.ws_url).await;
    let hs = HandshakeMsg {
        lite_id: lite_id.clone(),
        epoch,
        ..minimal_hs()
    };
    send_hs(&mut ws, &hs).await;
    let ack = recv_ack(&mut ws).await;

    assert!(
        ack.success,
        "after Origin restart the same lite_id+epoch must be accepted (tracker cleared); \
         error: {:?}",
        ack.error
    );
    assert!(!ack.fork_detected, "no fork after server restart");
}

/// §8.3e — Empty `lite_id` with any epoch → fork detection is skipped.
#[tokio::test]
async fn empty_lite_id_skips_fork_detection() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let mut ws = raw_connect(_server.ws_url).await;

    let hs = HandshakeMsg {
        lite_id: String::new(),
        epoch: 999,
        ..minimal_hs()
    };
    send_hs(&mut ws, &hs).await;
    let ack = recv_ack(&mut ws).await;

    assert!(
        ack.success,
        "empty lite_id must skip fork detection regardless of epoch; error: {:?}",
        ack.error
    );
    assert!(
        !ack.fork_detected,
        "fork_detected must be false when lite_id is empty"
    );
}

/// §8.3f — `epoch=0` with non-empty `lite_id` → fork detection skipped (never recorded).
#[tokio::test]
async fn epoch_zero_skips_fork_detection() {
    let Some(_server) = OriginServer::try_spawn() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };

    {
        let mut ws = raw_connect(_server.ws_url).await;
        let hs = HandshakeMsg {
            lite_id: "test-lite-id-epoch-zero-skip".to_string(),
            epoch: 0,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;
        assert!(ack.success, "epoch=0 must succeed; error: {:?}", ack.error);
        assert!(!ack.fork_detected, "no fork when epoch=0");
    }

    // Second connect with same lite_id+epoch=0: still not a fork (never recorded).
    {
        let mut ws = raw_connect(_server.ws_url).await;
        let hs = HandshakeMsg {
            lite_id: "test-lite-id-epoch-zero-skip".to_string(),
            epoch: 0,
            ..minimal_hs()
        };
        send_hs(&mut ws, &hs).await;
        let ack = recv_ack(&mut ws).await;
        assert!(
            ack.success,
            "second epoch=0 connect must succeed; error: {:?}",
            ack.error
        );
        assert!(!ack.fork_detected);
    }
}
