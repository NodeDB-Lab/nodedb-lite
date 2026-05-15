//! §7.8 — Non-blocking concurrent sync load check.
//!
//! Converted from `examples/load_test.rs`. Runs as an `#[ignore]`d perf test
//! that can be promoted to a hard gate by removing the attribute. The test is
//! always compiled so regressions are caught by the type-checker even when not
//! executing.
//!
//! Run explicitly with:
//!   cargo nextest run -p nodedb-lite --test sync_load -- --include-ignored

// This file is compiled as an integration test (not a criterion bench) so that
// nextest can manage it. Criterion is not required.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use futures::{SinkExt, StreamExt};
use nodedb_lite::engine::crdt::CrdtEngine;
use nodedb_types::sync::wire::{
    DeltaPushMsg, HandshakeAckMsg, HandshakeMsg, SyncFrame, SyncMessageType,
};
use nodedb_types::wire_version::WIRE_FORMAT_VERSION;
use tokio_tungstenite::tungstenite::Message;

use common::origin::{ORIGIN_WS, find_origin_binary};

const NUM_CLIENTS: u32 = 100;

#[tokio::test]
#[ignore = "load test — run explicitly with --include-ignored; requires Origin on port 9090"]
async fn sync_load_100_concurrent_clients() {
    // Spawn Origin.
    let binary = find_origin_binary();
    let mut child = std::process::Command::new(&binary)
        .env_remove("RUST_LOG")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap_or_else(|e| panic!("spawn Origin: {e}"));

    // Wait for readiness.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if std::net::TcpStream::connect("127.0.0.1:9090").is_ok() {
            break;
        }
        if std::time::Instant::now() > deadline {
            let _ = child.kill();
            panic!("Origin not ready within 15s");
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    let connected = Arc::new(AtomicU32::new(0));
    let handshook = Arc::new(AtomicU32::new(0));
    let deltas_sent = Arc::new(AtomicU32::new(0));
    let deltas_acked = Arc::new(AtomicU32::new(0));
    let deltas_rejected = Arc::new(AtomicU32::new(0));
    let errors = Arc::new(AtomicU32::new(0));

    let start = Instant::now();
    let mut handles = Vec::with_capacity(NUM_CLIENTS as usize);

    for i in 0..NUM_CLIENTS {
        let connected = Arc::clone(&connected);
        let handshook = Arc::clone(&handshook);
        let deltas_sent = Arc::clone(&deltas_sent);
        let deltas_acked = Arc::clone(&deltas_acked);
        let deltas_rejected = Arc::clone(&deltas_rejected);
        let errors = Arc::clone(&errors);

        handles.push(tokio::spawn(async move {
            run_client(
                i,
                connected,
                handshook,
                deltas_sent,
                deltas_acked,
                deltas_rejected,
                errors,
            )
            .await;
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    let elapsed = start.elapsed();

    let h = handshook.load(Ordering::Relaxed);
    let e = errors.load(Ordering::Relaxed);

    let _ = child.kill();
    let _ = child.wait();

    let throughput = h as f64 / elapsed.as_secs_f64();
    println!(
        "load: connected={} handshook={} sent={} acked={} rejected={} errors={} \
         elapsed={:.2}s throughput={:.0}/s",
        connected.load(Ordering::Relaxed),
        h,
        deltas_sent.load(Ordering::Relaxed),
        deltas_acked.load(Ordering::Relaxed),
        deltas_rejected.load(Ordering::Relaxed),
        e,
        elapsed.as_secs_f64(),
        throughput,
    );

    assert!(
        h >= NUM_CLIENTS,
        "only {h}/{NUM_CLIENTS} clients completed handshake — {e} errors"
    );
}

async fn run_client(
    id: u32,
    connected: Arc<AtomicU32>,
    handshook: Arc<AtomicU32>,
    deltas_sent: Arc<AtomicU32>,
    deltas_acked: Arc<AtomicU32>,
    deltas_rejected: Arc<AtomicU32>,
    errors: Arc<AtomicU32>,
) {
    let (mut ws, _) = match tokio_tungstenite::connect_async(ORIGIN_WS).await {
        Ok(ws) => ws,
        Err(_) => {
            errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    connected.fetch_add(1, Ordering::Relaxed);

    let hs = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: format!("load-test-{id}"),
        lite_id: String::new(),
        epoch: 0,
        wire_version: WIRE_FORMAT_VERSION,
    };
    if ws
        .send(Message::Binary(
            SyncFrame::try_encode(SyncMessageType::Handshake, &hs)
                .expect("encode handshake")
                .to_bytes()
                .into(),
        ))
        .await
        .is_err()
    {
        errors.fetch_add(1, Ordering::Relaxed);
        return;
    }

    let resp = match tokio::time::timeout(std::time::Duration::from_secs(10), ws.next()).await {
        Ok(Some(Ok(msg))) => msg,
        _ => {
            errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    if let Some(frame) = SyncFrame::from_bytes(resp.into_data().as_ref())
        && let Some(ack) = frame.decode_body::<HandshakeAckMsg>()
    {
        if ack.success {
            handshook.fetch_add(1, Ordering::Relaxed);
        } else {
            errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    }

    let mut engine = match CrdtEngine::new(1000 + id as u64) {
        Ok(e) => e,
        Err(_) => {
            errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let _ = engine.upsert(
        "load_test",
        &format!("doc-{id}"),
        &[("client_id", loro::LoroValue::I64(id as i64))],
    );

    let deltas = engine.pending_deltas();
    if let Some(delta) = deltas.first() {
        let msg = DeltaPushMsg {
            collection: "load_test".into(),
            document_id: format!("doc-{id}"),
            delta: delta.delta_bytes.clone(),
            peer_id: 1000 + id as u64,
            mutation_id: 1,
            checksum: 0,
            device_valid_time_ms: None,
        };
        if ws
            .send(Message::Binary(
                SyncFrame::try_encode(SyncMessageType::DeltaPush, &msg)
                    .expect("encode DeltaPush")
                    .to_bytes()
                    .into(),
            ))
            .await
            .is_ok()
        {
            deltas_sent.fetch_add(1, Ordering::Relaxed);
            if let Ok(Some(Ok(resp))) =
                tokio::time::timeout(std::time::Duration::from_secs(10), ws.next()).await
                && let Some(frame) = SyncFrame::from_bytes(resp.into_data().as_ref())
            {
                match frame.msg_type {
                    SyncMessageType::DeltaAck => {
                        deltas_acked.fetch_add(1, Ordering::Relaxed);
                    }
                    SyncMessageType::DeltaReject => {
                        deltas_rejected.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {}
                }
            }
        }
    }

    let _ = ws.close(None).await;
}
