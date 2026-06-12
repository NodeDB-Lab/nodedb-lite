//! Real-transport definition sync tests — require a live Origin node.
//!
//! Origin emits `DefinitionSync` (0x70) frames from the DDL commit path for
//! `CREATE FUNCTION`, `CREATE TRIGGER`, `CREATE PROCEDURE`, `DROP FUNCTION`,
//! `DROP TRIGGER`, and `DROP PROCEDURE`. Lite's receive path applies the
//! definition locally via `SyncDelegate::import_definition`.
//!
//! ## Scope
//!
//! These tests cover function, trigger, and procedure DDL.  Collection DDL
//! (CREATE COLLECTION, ALTER COLLECTION, DROP COLLECTION) is not part of
//! `DefinitionSyncMsg` — those are structural schema changes delivered via
//! `ShapeSnapshot`/`ShapeDelta` (0x21/0x22). The matrix row "Definition sync"
//! refers specifically to executable definitions (functions, triggers,
//! procedures) that Lite needs to execute locally.
//!
//! ## How to run
//!
//! Build the Origin binary first:
//! ```text
//! cd <project-root>/nodedb && cargo build -p nodedb
//! ```
//! Then run from the nodedb-lite workspace:
//! ```text
//! cargo nextest run -p nodedb-lite definition_sync_interop
//! ```
//!
//! Tests run in the `heavy` nextest group (serialized, one at a time).

mod common;

use std::time::Duration;

use futures::StreamExt;
use nodedb_types::sync::wire::{DefinitionSyncMsg, SyncFrame, SyncMessageType};

use sonic_rs::JsonValueTrait;

use common::origin::{OriginServer, connect_and_handshake};
use common::sql::OriginPgwire;

// ── helpers ──────────────────────────────────────────────────────────────────

/// Read frames from the WebSocket until a `DefinitionSync` frame for the
/// given `(definition_type, name, action)` triple is received, or until
/// the timeout expires.
///
/// Other frame types (HandshakeAck, ShapeSnapshot, PingPong, etc.) are
/// skipped — they may arrive interleaved with the definition frame.
async fn wait_for_definition_frame(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    definition_type: &str,
    name: &str,
    action: &str,
    timeout: Duration,
) -> Option<DefinitionSyncMsg> {
    use tokio_tungstenite::tungstenite::Message;

    let deadline = tokio::time::sleep(timeout);
    tokio::pin!(deadline);

    loop {
        tokio::select! {
            msg = ws.next() => {
                match msg? {
                    Ok(Message::Binary(data)) => {
                        let frame = SyncFrame::from_bytes(&data)?;
                        if frame.msg_type == SyncMessageType::DefinitionSync {
                            let msg: DefinitionSyncMsg = frame.decode_body()?;
                            if msg.definition_type == definition_type
                                && msg.name == name
                                && msg.action == action
                            {
                                return Some(msg);
                            }
                            // Different definition frame — keep waiting.
                        }
                        // Other frame types are ignored; keep waiting.
                    }
                    Ok(Message::Ping(_) | Message::Pong(_) | Message::Text(_)) => {}
                    _ => return None,
                }
            }
            _ = &mut deadline => return None,
        }
    }
}

// ── tests ─────────────────────────────────────────────────────────────────────

/// Origin creates a function via pgwire; Lite receives the `DefinitionSync`
/// (0x70) frame with `action = "put"` and the frame decodes successfully.
#[tokio::test]
async fn definition_sync_function_put() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };

    // Open a WebSocket sync connection and complete the handshake.
    let mut ws = connect_and_handshake(_origin.ws_url).await;

    // Issue the DDL via pgwire — Origin will broadcast a DefinitionSync
    // frame to all authenticated sync sessions after the catalog commit.
    let pg = OriginPgwire::connect().await;
    pg.execute("CREATE OR REPLACE FUNCTION double_int(x INT) RETURNS INT AS SELECT x * 2")
        .await;

    // Wait up to 5 s for the DefinitionSync frame.
    let msg = wait_for_definition_frame(
        &mut ws,
        "function",
        "double_int",
        "put",
        Duration::from_secs(5),
    )
    .await
    .expect(
        "did not receive DefinitionSync 'put' for 'double_int' within 5 s; \
         check that Origin emits DefinitionSyncMsg from DDL commit path",
    );

    assert_eq!(msg.definition_type, "function");
    assert_eq!(msg.name, "double_int");
    assert_eq!(msg.action, "put");
    assert!(!msg.payload.is_empty(), "put payload must not be empty");

    // Verify the payload decodes as the expected structure.
    let parsed: sonic_rs::Value =
        sonic_rs::from_slice(&msg.payload).expect("payload must be valid JSON");
    assert_eq!(
        parsed["name"].as_str().unwrap_or(""),
        "double_int",
        "payload.name must match"
    );
    assert_eq!(
        parsed["return_type"].as_str().unwrap_or(""),
        "INT",
        "payload.return_type must match"
    );
}

/// Origin drops a function via pgwire; Lite receives the `DefinitionSync`
/// frame with `action = "delete"`.
#[tokio::test]
async fn definition_sync_function_delete() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };

    let mut ws = connect_and_handshake(_origin.ws_url).await;

    let pg = OriginPgwire::connect().await;
    // Create first so the DROP has something to remove.
    pg.execute("CREATE OR REPLACE FUNCTION to_drop_fn(x TEXT) RETURNS TEXT AS SELECT x")
        .await;

    // Drain the 'put' frame so it doesn't confuse the wait below.
    let _ = wait_for_definition_frame(
        &mut ws,
        "function",
        "to_drop_fn",
        "put",
        Duration::from_secs(5),
    )
    .await;

    pg.execute("DROP FUNCTION to_drop_fn").await;

    let msg = wait_for_definition_frame(
        &mut ws,
        "function",
        "to_drop_fn",
        "delete",
        Duration::from_secs(5),
    )
    .await
    .expect("did not receive DefinitionSync 'delete' for 'to_drop_fn' within 5 s");

    assert_eq!(msg.action, "delete");
    assert!(
        msg.payload.is_empty(),
        "delete payload must be empty, got {} bytes",
        msg.payload.len()
    );
}

/// Origin creates a trigger; Lite receives the `DefinitionSync` frame with
/// `action = "put"` and the payload contains the expected fields.
#[tokio::test]
async fn definition_sync_trigger_put() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };

    let mut ws = connect_and_handshake(_origin.ws_url).await;

    let pg = OriginPgwire::connect().await;

    // Create a collection for the trigger to attach to.
    pg.execute(
        "CREATE COLLECTION IF NOT EXISTS trigger_test_col WITH (engine='document_schemaless')",
    )
    .await;

    pg.execute(
        "CREATE OR REPLACE TRIGGER log_insert \
         AFTER INSERT ON trigger_test_col \
         FOR EACH ROW AS BEGIN END",
    )
    .await;

    let msg = wait_for_definition_frame(
        &mut ws,
        "trigger",
        "log_insert",
        "put",
        Duration::from_secs(5),
    )
    .await
    .expect("did not receive DefinitionSync 'put' for trigger 'log_insert' within 5 s");

    assert_eq!(msg.definition_type, "trigger");
    assert_eq!(msg.name, "log_insert");
    assert_eq!(msg.action, "put");
    assert!(!msg.payload.is_empty(), "put payload must not be empty");

    let parsed: sonic_rs::Value =
        sonic_rs::from_slice(&msg.payload).expect("payload must be valid JSON");
    assert_eq!(
        parsed["name"].as_str().unwrap_or(""),
        "log_insert",
        "payload.name must match"
    );
    assert_eq!(
        parsed["collection"].as_str().unwrap_or(""),
        "trigger_test_col",
        "payload.collection must match"
    );
}

/// Origin creates a procedure; Lite receives the `DefinitionSync` frame with
/// `action = "put"` and the payload is valid.
#[tokio::test]
async fn definition_sync_procedure_put() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };

    let mut ws = connect_and_handshake(_origin.ws_url).await;

    let pg = OriginPgwire::connect().await;
    pg.execute("CREATE OR REPLACE PROCEDURE noop_proc() AS BEGIN END")
        .await;

    let msg = wait_for_definition_frame(
        &mut ws,
        "procedure",
        "noop_proc",
        "put",
        Duration::from_secs(5),
    )
    .await
    .expect("did not receive DefinitionSync 'put' for procedure 'noop_proc' within 5 s");

    assert_eq!(msg.definition_type, "procedure");
    assert_eq!(msg.name, "noop_proc");
    assert_eq!(msg.action, "put");
    assert!(!msg.payload.is_empty(), "put payload must not be empty");

    let parsed: sonic_rs::Value =
        sonic_rs::from_slice(&msg.payload).expect("payload must be valid JSON");
    assert_eq!(
        parsed["name"].as_str().unwrap_or(""),
        "noop_proc",
        "payload.name must match"
    );
}
