// SPDX-License-Identifier: Apache-2.0

//! Gate test: collection registration via the outbound `CollectionSchema`
//! (opcode `0x13`) announce — Lite → Origin, with NO pre-creation on Origin.
//!
//! Proves that a schemaless DOCUMENT collection created ONLY on Lite becomes
//! registered and queryable on a real Origin server purely through Lite's
//! outbound schema announce that fires when documents are synced. Origin
//! never runs `CREATE COLLECTION` for this collection — its existence in the
//! Origin catalog (`pg_class`) and its rows (`SELECT id FROM ...`) must come
//! entirely from the sync protocol.
//!
//! The collection is created explicitly on Lite via
//! [`nodedb_lite::NodeDbLite::create_collection`] (not implicitly via
//! `document_put`, and not via `CREATE COLLECTION ... WITH (engine=...)`
//! SQL) because the outbound emit path reads the collection's persisted
//! [`nodedb_lite::nodedb::collection::CollectionMeta`] to build the
//! `CollectionDescriptor` carried in the announce; an implicitly-created
//! collection has no `CollectionMeta` row and would not be announced. The
//! `CREATE COLLECTION ... WITH (engine='document_schemaless')` SQL DDL does
//! not intercept this shape (it only intercepts `storage='strict'`,
//! `storage='columnar'`, `storage='kv'`, and `bitemporal=true` forms — see
//! `src/query/ddl/mod.rs::try_handle_ddl`), so it falls through to
//! DataFusion without persisting a `CollectionMeta`. The `create_collection`
//! API call is the mechanism that reliably persists one.
//!
//! ## How to run
//!
//! Build the Origin binary first:
//! ```text
//! cd <project-root>/nodedb && cargo build -p nodedb
//! ```
//! Then run from the nodedb-lite workspace root:
//! ```text
//! cargo nextest run -p nodedb-lite --test sync_interop_collection_registration
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

// ── Collection identity ─────────────────────────────────────────────────────

/// Schemaless document collection, created ONLY on Lite. Origin must learn of
/// it purely via the outbound `CollectionSchema` (0x13) announce.
const COLLECTION: &str = "doc_reg_test";

// ── Helper: open a Lite DB backed by in-memory storage ─────────────────────────

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

/// Build a document with a `body` field containing distinguishing content.
fn make_doc(id: &str, body: &str) -> Document {
    let mut doc = Document::new(id);
    doc.set("body", Value::String(body.to_owned()));
    doc
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Creates `doc_reg_test` ONLY on Lite (no Origin pre-create), inserts 3
/// documents, and asserts that Origin — having received only the sync
/// stream — registers the collection in its catalog and serves the rows.
///
/// This is the end-to-end proof for issue #146: collection registration must
/// happen purely via the outbound `CollectionSchema` announce, not via any
/// side-channel DDL against Origin.
#[tokio::test]
async fn document_collection_registers_on_origin_via_announce() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let pg = OriginPgwire::connect().await;

    // Deliberately NOT creating the collection on Origin. Origin must learn
    // of `doc_reg_test` solely from the sync announce triggered below.

    let lite = open_lite().await;

    // Create the collection explicitly on Lite so a `CollectionMeta` is
    // persisted — this is what makes the outbound `CollectionSchema`
    // announce fire (the emit path loads the collection's meta; an
    // implicitly-created collection has no meta and would be skipped).
    lite.create_collection(COLLECTION, &[])
        .await
        .expect("Lite create_collection doc_reg_test");

    let _sync = start_sync(Arc::clone(&lite), 20).await;

    // Insert 3 documents with distinct ids.
    let ids = ["doc-a", "doc-b", "doc-c"];
    for id in ids {
        let doc = make_doc(id, &format!("registration probe content for {id}"));
        lite.document_put(COLLECTION, doc)
            .await
            .unwrap_or_else(|e| panic!("Lite document_put {id}: {e}"));
    }

    // PROOF 1 — registration via the announce: the collection must become
    // catalog-visible on Origin with NO pre-create. `pg_class` always exists,
    // so this query returns an empty set (not an error) until the announced
    // `PutCollectionIfAbsent` lands — a tolerant poll target.
    let mut catalog_visible = false;
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                if let Ok(rows) = pg
                    .try_query("SELECT relname FROM pg_class WHERE relname = 'doc_reg_test'")
                    .await
                    && !rows.is_empty()
                {
                    catalog_visible = true;
                    break;
                }
            }
        }
    }
    assert!(
        catalog_visible,
        "doc_reg_test must become visible in Origin's pg_class catalog via the \
         CollectionSchema announce (no Origin pre-create) within the deadline"
    );

    // PROOF 2 — shape-servable: a plain document scan (`SELECT id`, the same
    // `DocumentScan` path a `ShapeSnapshot` uses) must return all 3 synced
    // documents. This is the literal #146 symptom: before the Origin-side
    // CRDT-delta materialization fix, the synced deltas were acked but never
    // written to the sparse document store, so this scan returned 0 rows
    // (ShapeSnapshot doc_count=0). It must now return 3.
    let mut origin_row_count: usize = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(8));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                if let Ok(rows) = pg.try_query("SELECT id FROM doc_reg_test").await
                    && rows.len() >= 3
                {
                    origin_row_count = rows.len();
                    break;
                }
            }
        }
    }
    assert_eq!(
        origin_row_count, 3,
        "Origin must serve 3 synced documents via a plain document scan for \
         doc_reg_test after registration via the CollectionSchema announce \
         (no Origin pre-create); got {origin_row_count}"
    );

    // Cleanup.
    pg.execute("DROP COLLECTION doc_reg_test").await;
}

/// Same end-to-end proof, but the collection is created via SQL DDL
/// (`CREATE COLLECTION ... WITH (bitemporal=true)`) instead of the programmatic
/// `create_collection`. This is the exact entry point of lite issues #3/#6:
/// before the fix, the SQL-DDL handler did not persist a `CollectionMeta`, so
/// the outbound `CollectionSchema` announce was silently skipped and the
/// collection never registered on Origin. With the meta persisted, the announce
/// fires and the round-trip completes — registration (#6) and shape-servable
/// materialization (#146) — via the SQL client path a real SQL consumer uses.
#[tokio::test]
async fn sql_ddl_bitemporal_collection_registers_and_serves_via_announce() {
    let Some(_origin) = OriginServer::try_spawn_with_pgwire() else {
        eprintln!("SKIP: Origin binary unavailable (set NODEDB_BIN or run via `cargo nextest`)");
        return;
    };
    let pg = OriginPgwire::connect().await;

    const SQL_COLLECTION: &str = "sql_ddl_reg_test";

    let lite = open_lite().await;

    // Create ONLY on Lite, via SQL DDL — no Origin pre-create. This is the path
    // that previously persisted no `CollectionMeta` and thus never announced.
    lite.execute_sql(
        "CREATE COLLECTION sql_ddl_reg_test WITH (bitemporal=true)",
        &[],
    )
    .await
    .expect("Lite CREATE COLLECTION ... WITH (bitemporal=true)");

    let _sync = start_sync(Arc::clone(&lite), 21).await;

    let ids = ["ddl-a", "ddl-b", "ddl-c"];
    for id in ids {
        let doc = make_doc(id, &format!("sql-ddl registration probe for {id}"));
        lite.document_put(SQL_COLLECTION, doc)
            .await
            .unwrap_or_else(|e| panic!("Lite document_put {id}: {e}"));
    }

    // Registration via the announce (#6): catalog-visible on Origin, no pre-create.
    let mut catalog_visible = false;
    let deadline = tokio::time::sleep(Duration::from_secs(10));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                if let Ok(rows) = pg
                    .try_query("SELECT relname FROM pg_class WHERE relname = 'sql_ddl_reg_test'")
                    .await
                    && !rows.is_empty()
                {
                    catalog_visible = true;
                    break;
                }
            }
        }
    }
    assert!(
        catalog_visible,
        "a SQL-DDL bitemporal collection must register on Origin via the \
         CollectionSchema announce (issue #6) within the deadline"
    );

    // Shape-servable materialization (#146): all 3 synced docs served.
    let mut origin_row_count: usize = 0;
    let deadline = tokio::time::sleep(Duration::from_secs(8));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            _ = &mut deadline => break,
            _ = tokio::time::sleep(Duration::from_millis(200)) => {
                if let Ok(rows) = pg.try_query("SELECT id FROM sql_ddl_reg_test").await
                    && rows.len() >= 3
                {
                    origin_row_count = rows.len();
                    break;
                }
            }
        }
    }
    assert_eq!(
        origin_row_count, 3,
        "Origin must serve 3 synced documents for a SQL-DDL bitemporal collection \
         after announce-driven registration; got {origin_row_count}"
    );

    // Cleanup.
    pg.execute("DROP COLLECTION sql_ddl_reg_test").await;
}
