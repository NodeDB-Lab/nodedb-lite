// SPDX-License-Identifier: Apache-2.0

//! Which handler a DDL statement reaches is decided by parsing it.
//!
//! Routing on substrings gets two things wrong that parsing gets right: a
//! statement matching no pattern silently reaches a planner that does not know
//! the syntax, and a statement whose *name* happens to contain a keyword is
//! sent somewhere it never asked to go. Both are collection-creation bugs the
//! caller sees as "this valid statement does not work".

use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageMem};
use nodedb_types::document::Document;
use nodedb_types::value::Value;

async fn open_lite() -> Arc<NodeDbLite<PagedbStorageMem>> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open_in_memory");
    NodeDbLite::open(storage).await.expect("open")
}

/// The plainest form of the statement. It names no engine and sets no options,
/// so it means the default schemaless document engine.
#[tokio::test]
async fn bare_create_collection_is_accepted() {
    let db = open_lite().await;

    db.execute_sql("CREATE COLLECTION notes", &[])
        .await
        .expect("CREATE COLLECTION with no options must be accepted");
}

/// Creating a collection makes it visible before anything is written to it —
/// that is what distinguishes it from the implicit creation a write performs.
#[tokio::test]
async fn bare_create_collection_registers_the_collection() {
    let db = open_lite().await;
    db.execute_sql("CREATE COLLECTION notes", &[])
        .await
        .expect("create collection");

    let collections = db.list_collections().await.expect("list_collections");
    assert!(
        collections.iter().any(|c| c.name == "notes"),
        "a created collection must be listed before its first write, got: {:?}",
        collections.iter().map(|c| &c.name).collect::<Vec<_>>()
    );
}

/// A created collection accepts and returns documents.
#[tokio::test]
async fn bare_create_collection_is_usable() {
    let db = open_lite().await;
    db.execute_sql("CREATE COLLECTION notes", &[])
        .await
        .expect("create collection");

    let mut doc = Document::new("note0".to_string());
    doc.set("body", Value::String("written".to_string()));
    db.document_put("notes", doc).await.expect("document_put");

    let fetched = db
        .document_get("notes", "note0")
        .await
        .expect("document_get");
    assert!(
        fetched.is_some(),
        "a document written to a freshly created collection must be readable"
    );
}

/// A collection whose *name* contains the words a bitemporal statement uses is
/// still a plain collection. Deciding by substring made this one bitemporal,
/// silently giving it history semantics its creator never asked for.
#[tokio::test]
async fn collection_name_containing_keywords_is_not_bitemporal() {
    let db = open_lite().await;

    db.execute_sql("CREATE COLLECTION notes_true_bitemporal", &[])
        .await
        .expect("create collection whose name contains DDL keywords");

    let collections = db.list_collections().await.expect("list_collections");
    let created = collections
        .iter()
        .find(|c| c.name == "notes_true_bitemporal")
        .expect("the collection must exist under the name it was given");
    assert!(
        !created.bitemporal,
        "the collection asked for no bitemporal flag; its name is not a request"
    );
}

/// The explicit form still selects bitemporal history.
#[tokio::test]
async fn explicit_bitemporal_option_is_honored() {
    let db = open_lite().await;

    db.execute_sql("CREATE COLLECTION events WITH (bitemporal=true)", &[])
        .await
        .expect("create bitemporal collection");

    let collections = db.list_collections().await.expect("list_collections");
    let created = collections
        .iter()
        .find(|c| c.name == "events")
        .expect("the collection must exist");
    assert!(
        created.bitemporal,
        "a statement that asks for bitemporal=true must get it"
    );
}

/// An engine name that Lite has no handler for is reported. Passing it on
/// produces a confusing error from a planner that never saw the engine, and in
/// the worst case creates a collection with the wrong storage behind it.
#[tokio::test]
async fn unsupported_engine_is_reported() {
    let db = open_lite().await;

    let result = db
        .execute_sql("CREATE COLLECTION odd WITH (engine='nonexistent')", &[])
        .await;

    let Err(e) = result else {
        panic!("an unknown engine must not be accepted");
    };
    let msg = e.to_string();
    assert!(
        msg.contains("nonexistent"),
        "the error must name the engine it could not honor, got: {msg}"
    );
}
