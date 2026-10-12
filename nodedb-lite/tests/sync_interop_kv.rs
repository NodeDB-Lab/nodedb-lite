// SPDX-License-Identifier: Apache-2.0

//! KV writes on Lite reach a real Origin through `KvPush`.
//!
//! - A public `kv_put` is readable on Origin.
//! - A SQL `INSERT` into a KV collection is readable on Origin.
//! - A delete, public or SQL, removes the key on Origin.
//! - A `KvPush` re-sent after a later write applies once: Origin answers
//!   `Duplicate` and keeps the later value.
//!
//! ## How to run
//!
//! Build the Origin binary first:
//! ```text
//! cd <project-root>/nodedb && cargo build -p nodedb
//! ```
//! Then run from the nodedb-lite workspace root:
//! ```text
//! cargo nextest run -p nodedb-lite --test sync_interop_kv
//! ```
//!
//! The `binary(/sync_interop/)` filter in `.config/nextest.toml` puts the
//! test in the serialized `heavy` group.

#![cfg(feature = "sync")]

mod common;

use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use nodedb_client::NodeDb;
use nodedb_lite::sync::{SyncClient, SyncConfig, SyncState, run_sync_loop};
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::sync::wire::{
    AckStatus, HandshakeAckMsg, HandshakeMsg, KvPushAckMsg, KvPushMsg, KvPushOp, SyncFrame,
    SyncMessageType,
};
use nodedb_types::wire_version::WIRE_FORMAT_VERSION;
use tokio_tungstenite::tungstenite::Message;

use common::origin::{ORIGIN_WS, OriginServer, OriginWs};
use common::sql::{OriginPgwire, open_lite};

/// How long a write may take to reach Origin.
const SYNC_DEADLINE: Duration = Duration::from_secs(10);

fn create_kv(name: &str) -> String {
    format!("CREATE COLLECTION {name} (key TEXT PRIMARY KEY, value TEXT) WITH (engine='kv')")
}

/// Run the sync loop for `lite` and wait until it is connected.
async fn start_sync(lite: Arc<NodeDbLite<PagedbStorageMem>>) -> Arc<SyncClient> {
    let client = Arc::new(SyncClient::new(SyncConfig::new(ORIGIN_WS, "")));
    let delegate = Arc::clone(&lite) as Arc<dyn nodedb_lite::sync::SyncDelegate>;
    let loop_client = Arc::clone(&client);
    tokio::spawn(async move {
        run_sync_loop(loop_client, delegate).await;
    });
    let deadline = tokio::time::Instant::now() + SYNC_DEADLINE;
    while client.state().await != SyncState::Connected {
        assert!(
            tokio::time::Instant::now() < deadline,
            "sync connection did not establish within {SYNC_DEADLINE:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    client
}

/// Spawn Origin, create the KV collection `name` on both sides, and start
/// syncing. `None` when the Origin binary is unavailable.
async fn setup(
    name: &str,
) -> Option<(
    OriginServer,
    OriginPgwire,
    Arc<NodeDbLite<PagedbStorageMem>>,
    Arc<SyncClient>,
)> {
    let origin = OriginServer::try_spawn_with_pgwire()?;
    let pg = OriginPgwire::connect().await;
    pg.execute(&create_kv(name)).await;

    let lite = open_lite().await;
    lite.execute_sql(&create_kv(name), &[])
        .await
        .unwrap_or_else(|e| panic!("Lite CREATE COLLECTION {name}: {e}"));

    let sync = start_sync(Arc::clone(&lite)).await;
    Some((origin, pg, lite, sync))
}

/// The value Origin holds for `key` in `collection`, `None` when absent.
async fn origin_value(pg: &OriginPgwire, collection: &str, key: &str) -> Option<String> {
    let rows = pg
        .poll_query(&format!(
            "SELECT value FROM {collection} WHERE key = '{key}'"
        ))
        .await;
    rows.first()
        .and_then(|row| row.try_get::<_, String>(0).ok())
}

/// Poll Origin until `key` holds `expected` (`None` for absent), or panic
/// at the deadline.
async fn await_origin_value(
    pg: &OriginPgwire,
    collection: &str,
    key: &str,
    expected: Option<&str>,
) {
    let deadline = tokio::time::Instant::now() + SYNC_DEADLINE;
    loop {
        let seen = origin_value(pg, collection, key).await;
        if seen.as_deref() == expected {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "Origin {collection}/{key}: expected {expected:?}, still {seen:?} after {SYNC_DEADLINE:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn skip() {
    eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
}

#[tokio::test]
async fn a_public_kv_put_is_readable_on_origin() {
    let Some((_origin, pg, lite, _sync)) = setup("kv_api").await else {
        return skip();
    };
    lite.kv_put("kv_api", "p1", b"from-api")
        .await
        .expect("Lite kv_put");
    await_origin_value(&pg, "kv_api", "p1", Some("from-api")).await;
}

#[tokio::test]
async fn a_sql_kv_insert_is_readable_on_origin() {
    let Some((_origin, pg, lite, _sync)) = setup("kv_sql").await else {
        return skip();
    };
    lite.execute_sql(
        "INSERT INTO kv_sql (key, value) VALUES ('s1', 'from-sql')",
        &[],
    )
    .await
    .expect("Lite SQL insert");
    await_origin_value(&pg, "kv_sql", "s1", Some("from-sql")).await;
}

#[tokio::test]
async fn a_delete_removes_the_key_on_origin() {
    let Some((_origin, pg, lite, _sync)) = setup("kv_del").await else {
        return skip();
    };
    lite.kv_put("kv_del", "d1", b"v1").await.expect("kv_put d1");
    lite.execute_sql("INSERT INTO kv_del (key, value) VALUES ('d2', 'v2')", &[])
        .await
        .expect("SQL insert d2");
    await_origin_value(&pg, "kv_del", "d1", Some("v1")).await;
    await_origin_value(&pg, "kv_del", "d2", Some("v2")).await;

    assert!(lite.kv_delete("kv_del", "d1").await.expect("kv_delete d1"));
    lite.execute_sql("DELETE FROM kv_del WHERE key = 'd2'", &[])
        .await
        .expect("SQL delete d2");
    await_origin_value(&pg, "kv_del", "d1", None).await;
    await_origin_value(&pg, "kv_del", "d2", None).await;
}

/// Open a raw sync session as the producer `lite_id` and return the socket
/// and the producer id Origin assigned.
async fn raw_session(lite_id: &str) -> (OriginWs, u64) {
    let (mut ws, _) = tokio_tungstenite::connect_async(ORIGIN_WS)
        .await
        .expect("connect to Origin");
    let hello = HandshakeMsg {
        jwt_token: String::new(),
        vector_clock: std::collections::HashMap::new(),
        subscribed_shapes: Vec::new(),
        client_version: "kv-interop-test".into(),
        lite_id: lite_id.into(),
        epoch: 1,
        wire_version: WIRE_FORMAT_VERSION,
    };
    send(&mut ws, SyncMessageType::Handshake, &hello).await;
    let ack: HandshakeAckMsg = next_of(&mut ws, SyncMessageType::HandshakeAck).await;
    assert!(ack.success, "handshake rejected: {:?}", ack.error);
    assert_ne!(ack.producer_id, 0, "Origin assigns a producer id");
    (ws, ack.producer_id)
}

async fn send<T: zerompk::ToMessagePack>(ws: &mut OriginWs, kind: SyncMessageType, body: &T) {
    let bytes = SyncFrame::try_encode(kind, body)
        .expect("encode frame")
        .to_bytes();
    ws.send(Message::Binary(bytes.into()))
        .await
        .expect("send frame");
}

/// The next frame of type `kind`, skipping any other frame.
async fn next_of<T>(ws: &mut OriginWs, kind: SyncMessageType) -> T
where
    T: zerompk::FromMessagePackOwned,
{
    let deadline = tokio::time::Instant::now() + SYNC_DEADLINE;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let message = tokio::time::timeout(left, ws.next())
            .await
            .unwrap_or_else(|_| panic!("no {kind:?} frame within {SYNC_DEADLINE:?}"))
            .expect("socket closed")
            .expect("socket error");
        let Message::Binary(bytes) = message else {
            continue;
        };
        let frame = SyncFrame::from_bytes(bytes.as_ref()).expect("decode frame");
        if frame.msg_type == kind {
            return frame.decode_body().expect("decode frame body");
        }
    }
}

fn put(collection: &str, key: &str, value: &str, producer_id: u64, seq: u64) -> KvPushMsg {
    KvPushMsg {
        lite_id: "kv-resend".into(),
        collection: collection.into(),
        key: key.as_bytes().to_vec(),
        op: KvPushOp::Put {
            row: nodedb_query::msgpack_scan::kv_row_msgpack(key, value.as_bytes()),
            expire_at_ms: 0,
        },
        batch_id: seq,
        producer_id,
        epoch: 1,
        seq,
    }
}

#[tokio::test]
async fn a_resent_push_applies_once() {
    let Some(origin) = OriginServer::try_spawn_with_pgwire() else {
        return skip();
    };
    let pg = OriginPgwire::connect().await;
    pg.execute(&create_kv("kv_once")).await;

    let (mut ws, producer_id) = raw_session("kv-resend").await;
    let first = put("kv_once", "r1", "first", producer_id, 1);
    send(&mut ws, SyncMessageType::KvPush, &first).await;
    let ack: KvPushAckMsg = next_of(&mut ws, SyncMessageType::KvPushAck).await;
    assert_eq!(ack.status, AckStatus::Applied, "seq 1 applies");

    let second = put("kv_once", "r1", "second", producer_id, 2);
    send(&mut ws, SyncMessageType::KvPush, &second).await;
    let ack: KvPushAckMsg = next_of(&mut ws, SyncMessageType::KvPushAck).await;
    assert_eq!(ack.status, AckStatus::Applied, "seq 2 applies");

    // A reconnecting producer re-sends seq 1. Applying it again would put
    // "first" back over the later "second".
    drop(ws);
    let (mut ws, resumed_id) = raw_session("kv-resend").await;
    assert_eq!(
        resumed_id, producer_id,
        "the same lite_id resumes its producer"
    );
    send(&mut ws, SyncMessageType::KvPush, &first).await;
    let ack: KvPushAckMsg = next_of(&mut ws, SyncMessageType::KvPushAck).await;
    assert_eq!(
        ack.status,
        AckStatus::Duplicate,
        "a re-sent seq is a duplicate"
    );
    assert_eq!(ack.applied_seq, 2);

    await_origin_value(&pg, "kv_once", "r1", Some("second")).await;
    drop(origin);
}
