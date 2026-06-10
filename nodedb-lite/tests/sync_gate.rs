// SPDX-License-Identifier: Apache-2.0

//! Verifies that an installed `SyncGate` keeps rejected documents local-only:
//! they land in CRDT state (readable locally) but are excluded from the pending
//! CRDT delta stream pushed to Origin.

use std::collections::HashMap;
use std::sync::Arc;

use nodedb_client::NodeDb;
use nodedb_lite::{NodeDbLite, PagedbStorageMem, SyncGate};
use nodedb_types::document::Document;
use nodedb_types::value::Value;

/// Gate that withholds any document whose `share` field is not `"public"`.
struct PublicOnlyGate;

impl SyncGate for PublicOnlyGate {
    fn should_sync(&self, _collection: &str, fields: &HashMap<String, Value>) -> bool {
        match fields.get("share") {
            Some(Value::String(s)) => s == "public",
            _ => true,
        }
    }
}

#[tokio::test]
async fn gate_withholds_non_public_from_pending_deltas() {
    let s = PagedbStorageMem::open_in_memory().await.unwrap();
    let db = NodeDbLite::open(s, 1).await.unwrap();
    db.set_sync_gate(Arc::new(PublicOnlyGate));

    // Public doc → should sync.
    let mut pubdoc = Document::new("pub-1");
    pubdoc.set("share", Value::String("public".into()));
    pubdoc.set("content", Value::String("shareable".into()));
    db.document_put("entries", pubdoc).await.unwrap();

    // Private doc → must be withheld from the delta stream.
    let mut privdoc = Document::new("priv-1");
    privdoc.set("share", Value::String("private".into()));
    privdoc.set("content", Value::String("secret".into()));
    db.document_put("entries", privdoc).await.unwrap();

    let pending = db.pending_crdt_deltas().unwrap();
    let ids: Vec<&str> = pending.iter().map(|d| d.document_id.as_str()).collect();

    assert!(
        ids.contains(&"pub-1"),
        "public doc must be in the delta stream"
    );
    assert!(
        !ids.contains(&"priv-1"),
        "private doc must be withheld from the delta stream"
    );

    // But the private doc is still locally readable (kept in CRDT state).
    let got = db.document_get("entries", "priv-1").await.unwrap();
    assert!(got.is_some(), "private doc must remain in local CRDT state");
}

#[tokio::test]
async fn without_gate_everything_syncs() {
    let s = PagedbStorageMem::open_in_memory().await.unwrap();
    let db = NodeDbLite::open(s, 1).await.unwrap();

    let mut d = Document::new("priv-1");
    d.set("share", Value::String("private".into()));
    db.document_put("entries", d).await.unwrap();

    let pending = db.pending_crdt_deltas().unwrap();
    assert!(
        pending.iter().any(|d| d.document_id == "priv-1"),
        "with no gate installed, all documents sync (prior behavior)"
    );
}
