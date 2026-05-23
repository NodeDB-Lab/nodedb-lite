// SPDX-License-Identifier: Apache-2.0

//! Gate test: FTS document index/delete sync — Lite → Origin round-trip.
//!
//! Proves that documents inserted into a schemaless collection on Lite
//! replicate their text content to Origin via the `FtsIndex` (0xA6) wire frame
//! and are returned by a `text_match` query on Origin.  Also proves that
//! `FtsDelete` (0xA8) removes the document so it no longer appears in
//! subsequent FTS searches.
//!
//! ## How to run
//!
//! Build the Origin binary first:
//! ```text
//! cd <project-root>/nodedb && cargo build -p nodedb
//! ```
//! Then run from the nodedb-lite workspace root:
//! ```text
//! cargo nextest run -p nodedb-lite --test sync_interop_fts
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
use common::sql::OriginPgwire;

// ── Collection DDL ──────────────────────────────────────────────────────────

/// Schemaless document collection on Origin (pgwire dialect).
///
/// FTS search works on any document collection via `text_match(field, 'query')`
/// without requiring `CREATE FULLTEXT INDEX`; the BM25 index is always
/// available on the schemaless engine.
const COLLECTION: &str = "fts_sync_test";

const CREATE_ORIGIN: &str = "CREATE COLLECTION fts_sync_test WITH (engine='document_schemaless')";

// ── Helper: open a Lite DB backed by in-memory redb ─────────────────────────

async fn open_lite() -> Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    Arc::new(
        NodeDbLite::open(storage, 1)
            .await
            .expect("NodeDbLite::open"),
    )
}

/// Wire up the sync transport and wait until the connection is established.
async fn start_sync(lite: Arc<NodeDbLite<PagedbStorageMem>>, peer_id: u64) -> Arc<SyncClient> {
    let sync_config = SyncConfig::new(common::origin::ORIGIN_WS, "");
    let sync_client = Arc::new(SyncClient::new(sync_config, peer_id));
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

/// Build a document with a `body` text field containing the given content.
fn make_doc(id: &str, body: &str) -> Document {
    let mut doc = Document::new(id);
    doc.set("body", Value::String(body.to_owned()));
    doc
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Inserts 3 documents on Lite; waits for replication to Origin; asserts that
/// `text_match` on Origin returns all 3 matching documents.
#[tokio::test]
async fn fts_inserts_replicate_to_origin() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    // Create the collection on Origin.
    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;
    let _sync = start_sync(Arc::clone(&lite), 10).await;

    // Insert 3 documents with a common search term.
    for i in 0u32..3 {
        let doc = make_doc(
            &format!("article{i}"),
            &format!("rocketengine propulsion research document number {i}"),
        );
        lite.document_put(COLLECTION, doc)
            .await
            .unwrap_or_else(|e| panic!("Lite document_put article{i}: {e}"));
    }

    // Wait up to 5 s for Origin's FTS index to return 3 results.
    let mut origin_count: usize = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let rows = pg
                    .query(
                        "SELECT id FROM fts_sync_test \
                         WHERE text_match(body, 'rocketengine') \
                         LIMIT 10",
                    )
                    .await;
                if rows.len() >= 3 {
                    origin_count = rows.len();
                    break;
                }
            }
        }
    }

    assert_eq!(
        origin_count, 3,
        "Origin FTS must return 3 results after sync; got {origin_count}"
    );

    // Cleanup.
    pg.execute("DROP COLLECTION fts_sync_test").await;
}

/// Inserts a document on Lite, waits for FTS replication, deletes it on Lite,
/// waits for the delete to replicate, then asserts the document no longer
/// appears in FTS search results on Origin.
#[tokio::test]
async fn fts_delete_replicates_to_origin() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;
    let _sync = start_sync(Arc::clone(&lite), 11).await;

    // Insert 2 background documents and 1 target document.
    for i in 0u32..2 {
        let doc = make_doc(
            &format!("bg{i}"),
            &format!("backgroundarticle content number {i}"),
        );
        lite.document_put(COLLECTION, doc)
            .await
            .unwrap_or_else(|e| panic!("Lite document_put bg{i}: {e}"));
    }
    let target = make_doc("target", "targetarticle unique content to delete");
    lite.document_put(COLLECTION, target)
        .await
        .expect("Lite document_put target");

    // Wait for all 3 documents to appear in FTS on Origin.
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut all_appeared = false;
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                // Search for terms present in ALL 3 docs: "content"
                let rows_bg = pg
                    .query(
                        "SELECT id FROM fts_sync_test \
                         WHERE text_match(body, 'content') \
                         LIMIT 10",
                    )
                    .await;
                // Also verify target specifically
                let rows_target = pg
                    .query(
                        "SELECT id FROM fts_sync_test \
                         WHERE text_match(body, 'targetarticle') \
                         LIMIT 5",
                    )
                    .await;
                if rows_bg.len() >= 3 && !rows_target.is_empty() {
                    all_appeared = true;
                    break;
                }
            }
        }
    }
    assert!(
        all_appeared,
        "all 3 documents must appear on Origin before testing delete"
    );

    // Delete the target on Lite.
    lite.document_delete(COLLECTION, "target")
        .await
        .expect("Lite document_delete target");

    // Wait for the delete to replicate: target should no longer appear.
    let deadline = tokio::time::sleep(Duration::from_secs(5));
    tokio::pin!(deadline);
    let mut target_gone = false;
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let rows = pg
                    .query(
                        "SELECT id FROM fts_sync_test \
                         WHERE text_match(body, 'targetarticle') \
                         LIMIT 5",
                    )
                    .await;
                if rows.is_empty() {
                    target_gone = true;
                    break;
                }
            }
        }
    }
    assert!(
        target_gone,
        "after FtsDelete replicates, 'targetarticle' must return 0 results on Origin"
    );

    // Background documents must still be present.
    let bg_rows = pg
        .query(
            "SELECT id FROM fts_sync_test \
             WHERE text_match(body, 'backgroundarticle') \
             LIMIT 5",
        )
        .await;
    assert_eq!(
        bg_rows.len(),
        2,
        "background documents must still be present after target delete; got {}",
        bg_rows.len()
    );

    // Cleanup.
    pg.execute("DROP COLLECTION fts_sync_test").await;
}

/// Documents inserted before sync connects are flushed once the connection
/// comes up — same guarantee as vector/columnar.
#[tokio::test]
async fn fts_pre_connection_inserts_sync_after_connect() {
    let _origin = OriginServer::spawn_with_pgwire();
    let pg = OriginPgwire::connect().await;

    pg.execute(CREATE_ORIGIN).await;

    let lite = open_lite().await;

    // Insert 2 documents BEFORE starting sync.
    for i in 0u32..2 {
        let doc = make_doc(
            &format!("pre{i}"),
            &format!("preconnection document about stellarphysics number {i}"),
        );
        lite.document_put(COLLECTION, doc)
            .await
            .unwrap_or_else(|e| panic!("Lite pre-sync document_put pre{i}: {e}"));
    }

    // Now start sync transport.
    let _sync = start_sync(Arc::clone(&lite), 12).await;

    // Wait up to 8 s for Origin to have both documents in FTS.
    let mut origin_count: usize = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(8));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                let rows = pg
                    .query(
                        "SELECT id FROM fts_sync_test \
                         WHERE text_match(body, 'stellarphysics') \
                         LIMIT 5",
                    )
                    .await;
                if rows.len() >= 2 {
                    origin_count = rows.len();
                    break;
                }
            }
        }
    }

    assert_eq!(
        origin_count, 2,
        "pre-connection FTS documents must replicate once sync connects; got {origin_count}"
    );

    pg.execute("DROP COLLECTION fts_sync_test").await;
}
