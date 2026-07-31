// SPDX-License-Identifier: Apache-2.0

//! Gate test: a write to an unrelated second collection, interleaved between
//! `document_put` calls on the collection under test, must not stall
//! replication of that collection to Origin.
//!
//! Both tests insert 3 documents into a bitemporal collection (`probe`) and
//! assert Origin ends up with 3 rows. The only difference between them is
//! whether a KV write to a second collection (`signals`) is interleaved
//! between each `document_put`. Keeping the two variants side by side is
//! what makes a causal-completeness regression diagnosable: the
//! documents-only variant isolates the `probe` sync path, and the
//! interleaved variant proves that a write to an entirely different
//! collection cannot leave `probe`'s own deltas looking causally incomplete
//! to Origin (which would otherwise buffer them indefinitely, waiting for a
//! dependency that will never arrive from `signals`).
//!
//! ## How to run
//!
//! Build the Origin binary first:
//! ```text
//! cd <project-root>/nodedb && cargo build -p nodedb
//! ```
//! Then run from the nodedb-lite workspace root:
//! ```text
//! cargo nextest run -p nodedb-lite --test sync_interop_cross_collection
//! ```
//!
//! The test is placed in the `heavy` nextest group (serialized) by the
//! `binary(/sync_interop/)` filter in `.config/nextest.toml`.

mod common;

use std::sync::Arc;
use std::time::Duration;

use nodedb_client::NodeDb;
use nodedb_lite::sync::{SyncClient, SyncConfig, run_sync_loop};
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::document::Document;
use nodedb_types::value::Value;

use common::origin::OriginServer;
use common::sql::{OriginPgwire, open_lite};

// ── Collection identity ─────────────────────────────────────────────────────

const PROBE: &str = "probe";
const SIGNALS: &str = "signals";

const CREATE_PROBE: &str = "CREATE COLLECTION probe WITH (bitemporal=true)";

/// Wire up the sync transport and wait until the connection is established.
async fn start_sync(lite: Arc<NodeDbLite<PagedbStorageMem>>) -> Arc<SyncClient> {
    let sync_config = SyncConfig::new(common::origin::ORIGIN_WS, "");
    let sync_client = Arc::new(SyncClient::new(sync_config));
    let delegate = Arc::clone(&lite) as Arc<dyn nodedb_lite::sync::SyncDelegate>;
    let client_clone = Arc::clone(&sync_client);
    tokio::spawn(async move {
        run_sync_loop(client_clone, delegate).await;
    });

    // Wait up to 10 s for the connection to become established.
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => panic!("sync connection did not establish within 10 seconds"),
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if sync_client.state().await == nodedb_lite::sync::SyncState::Connected {
                    break;
                }
            }
        }
    }

    sync_client
}

/// Build a document with a `body` field containing distinguishing content.
fn make_doc(id: &str, body: &str) -> Document {
    let mut doc = Document::new(id);
    doc.set("body", Value::String(body.to_owned()));
    doc
}

/// Shared setup for both tests: spawn Origin, create `probe` on both sides,
/// and start syncing Lite to it.
///
/// Returns `None` when the Origin binary is unavailable — callers should
/// print a skip message and return early.
async fn setup_probe() -> Option<(
    OriginServer,
    OriginPgwire,
    Arc<NodeDbLite<PagedbStorageMem>>,
    Arc<SyncClient>,
)> {
    let origin = OriginServer::try_spawn_with_pgwire()?;
    let pg = OriginPgwire::connect().await;
    pg.execute(CREATE_PROBE).await;

    let lite = open_lite().await;
    lite.execute_sql(CREATE_PROBE, &[])
        .await
        .expect("Lite CREATE COLLECTION probe WITH (bitemporal=true)");

    let sync = start_sync(Arc::clone(&lite)).await;
    Some((origin, pg, lite, sync))
}

/// Poll Origin for `probe` rows for up to 5 s, stopping as soon as all 3
/// documents are visible. Returns the row count last observed by the poll
/// (0 if the deadline was hit first) — the caller still issues a final
/// non-polling query for the actual assertion.
async fn wait_for_probe_rows(pg: &OriginPgwire) -> usize {
    let mut origin_count: usize = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let rows = pg.poll_query("SELECT id FROM probe").await;
                if rows.len() >= 3 {
                    origin_count = rows.len();
                    break;
                }
            }
        }
    }
    origin_count
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Variant A of the original report: 3 plain `document_put` calls into a
/// bitemporal collection, nothing else interleaved. This path always worked
/// and exists here purely as the control that variant B is contrasted
/// against.
#[tokio::test]
async fn documents_only_replicate_to_origin() {
    let Some((_origin, pg, lite, _sync)) = setup_probe().await else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };

    let ids = ["p-a", "p-b", "p-c"];
    for id in ids {
        let doc = make_doc(id, &format!("documents-only probe for {id}"));
        lite.document_put(PROBE, doc)
            .await
            .unwrap_or_else(|e| panic!("Lite document_put {id}: {e}"));
    }

    let origin_count = wait_for_probe_rows(&pg).await;

    let rows = pg.query("SELECT id FROM probe").await;
    assert_eq!(
        rows.len(),
        3,
        "Origin must have 3 rows after documents-only sync; got {} (poll saw {origin_count})",
        rows.len()
    );

    pg.execute("DROP COLLECTION probe").await;
}

/// Variant B of the original report: each `document_put` into `probe` is
/// followed by a write to the unrelated `signals` collection. `signals` is
/// written through the KV deferred path (`kv_put` + `kv_flush`) exactly as
/// the original report did, because KV writes go through the same
/// CRDT-backed deferred-delta path as documents — the shape that let a write
/// to one collection affect the causal completeness Origin computed for
/// another. Before the fix this returned 1 instead of 3; this is the
/// regression guard.
#[tokio::test]
async fn interleaved_second_collection_writes_still_replicate() {
    let Some((_origin, pg, lite, _sync)) = setup_probe().await else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };

    let ids = ["p-a", "p-b", "p-c"];
    for (i, id) in ids.iter().enumerate() {
        let doc = make_doc(id, &format!("interleaved probe for {id}"));
        lite.document_put(PROBE, doc)
            .await
            .unwrap_or_else(|e| panic!("Lite document_put {id}: {e}"));

        // The trigger: a write to a completely different collection, landed
        // between two `probe` writes. `kv_flush` forces it out immediately
        // instead of waiting for the KV auto-flush threshold, so the
        // interleaving is deterministic rather than timing-dependent.
        let entry_id = format!("signal-{i}");
        lite.kv_put(SIGNALS, &entry_id, entry_id.as_bytes())
            .await
            .unwrap_or_else(|e| panic!("Lite kv_put {entry_id}: {e}"));
        lite.kv_flush()
            .await
            .unwrap_or_else(|e| panic!("Lite kv_flush after {entry_id}: {e}"));
    }

    wait_for_probe_rows(&pg).await;

    let rows = pg.query("SELECT id FROM probe").await;
    assert_eq!(
        rows.len(),
        3,
        "Origin must have 3 rows after interleaved cross-collection sync; got {}",
        rows.len()
    );

    pg.execute("DROP COLLECTION probe").await;
}
