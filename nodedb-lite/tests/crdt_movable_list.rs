// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage for CRDT movable-list operations through the public
//! `NodeDb` trait.
//!
//! `list_insert` against a document with no pre-existing list at `list_path`
//! must auto-vivify the `LoroMovableList` (and any intermediate maps) rather
//! than erroring — this is only reachable now that
//! `nodedb_crdt::list_ops::get_or_create_movable_list` replaced the strict
//! `get_movable_list` lookup on the insert path. Without that fix there was
//! no way to bootstrap a movable list on a fresh row anywhere in the public
//! surface of either repo.
//!
//! The row itself is bootstrapped via the existing `document_put` API —
//! `list_insert` never creates rows, only the list container within one.
//!
//! `NodeDbLite`'s `document_get` intentionally reads rows shallowly (see
//! `nodedb_crdt::state::core::read_row`) to avoid a recursive
//! `get_deep_value()` clone, so nested containers like the block list come
//! back as opaque `LoroValue::Container` refs rather than expanded content.
//! To assert the actual block order/content, this test replays the CRDT
//! deltas produced by the public API (`NodeDbLite::pending_crdt_deltas`,
//! also public) into a fresh `nodedb_crdt::CrdtState` and reads the movable
//! list back with `nodedb_crdt::list_ops` directly — both public surfaces
//! of the already-published `nodedb-crdt` crate, no new API added.

use std::collections::HashMap;

use nodedb_client::NodeDb;
use nodedb_crdt::CrdtState;
use nodedb_lite::NodeDbLite;
use nodedb_lite::storage::pagedb_storage::PagedbStorageMem;
use nodedb_types::document::Document;
use nodedb_types::value::Value;

async fn open_db() -> NodeDbLite<PagedbStorageMem> {
    let storage = PagedbStorageMem::open_in_memory()
        .await
        .expect("open in-memory storage");
    NodeDbLite::open(storage, 1).await.expect("open NodeDbLite")
}

fn block_fields(id: &str, content: &str) -> Value {
    let mut map = HashMap::new();
    map.insert("id".to_string(), Value::String(id.to_string()));
    map.insert("content".to_string(), Value::String(content.to_string()));
    Value::Object(map)
}

/// Replay every CRDT delta the db has produced so far (in mutation-id order)
/// into a fresh, independent `CrdtState` and return it for direct inspection
/// via `nodedb_crdt::list_ops`.
fn replay_deltas<S: nodedb_lite::storage::engine::StorageEngine>(db: &NodeDbLite<S>) -> CrdtState {
    let mut deltas = db.pending_crdt_deltas().expect("pending_crdt_deltas");
    deltas.sort_by_key(|d| d.mutation_id);

    let replay = CrdtState::new(99).expect("create replay CrdtState");
    for delta in &deltas {
        replay
            .import(&delta.delta_bytes)
            .expect("import delta into replay state");
    }
    replay
}

/// Create a row with `document_put`, insert two blocks into a movable list
/// that does not exist yet, move one, delete one, and assert the final
/// order/content is exactly what survives.
#[tokio::test]
async fn list_insert_move_delete_round_trip_on_fresh_row() {
    let db = open_db().await;

    // Bootstrap the row. No "blocks" field is set — the movable list at
    // "blocks" does not exist yet.
    let mut doc = Document::new("page-1");
    doc.set("title", Value::String("Test Page".to_string()));
    db.document_put("pages", doc)
        .await
        .expect("document_put creates the row");

    // First insert must auto-vivify the movable list.
    db.list_insert(
        "pages",
        "page-1",
        "blocks",
        0,
        &block_fields("blk-a", "Alpha"),
    )
    .await
    .expect("list_insert auto-vivifies the missing movable list");

    db.list_insert(
        "pages",
        "page-1",
        "blocks",
        1,
        &block_fields("blk-b", "Beta"),
    )
    .await
    .expect("list_insert appends a second block");

    // [Alpha, Beta] -> move index 1 to 0 -> [Beta, Alpha]
    db.list_move("pages", "page-1", "blocks", 1, 0)
        .await
        .expect("list_move reorders the two blocks");

    // Delete index 1 (now Alpha) -> [Beta]
    db.list_delete("pages", "page-1", "blocks", 1)
        .await
        .expect("list_delete removes the trailing block");

    let replay = replay_deltas(&db);
    let len = nodedb_crdt::list_ops::list_length(replay.doc(), "pages", "page-1", "blocks")
        .expect("list_length on replayed state");
    assert_eq!(len, 1, "exactly one block should remain");

    let value = nodedb_crdt::list_ops::list_get(replay.doc(), "pages", "page-1", "blocks", 0)
        .expect("list_get on replayed state")
        .expect("block at index 0 must exist");

    let loro::LoroValue::Map(block) = value else {
        panic!("expected block map, got {value:?}");
    };
    assert_eq!(
        block.get("id"),
        Some(&loro::LoroValue::String("blk-b".into()))
    );
    assert_eq!(
        block.get("content"),
        Some(&loro::LoroValue::String("Beta".into()))
    );
}

/// A nested `list_path` (e.g. `content.blocks`) auto-creates the
/// intermediate map, not just the terminal movable list.
#[tokio::test]
async fn list_insert_auto_vivifies_nested_path() {
    let db = open_db().await;

    let doc = Document::new("page-2");
    db.document_put("pages", doc)
        .await
        .expect("document_put creates the row");

    // Neither "content" nor "content.blocks" exists on this row yet.
    db.list_insert(
        "pages",
        "page-2",
        "content.blocks",
        0,
        &block_fields("blk-x", "Nested"),
    )
    .await
    .expect("list_insert auto-vivifies intermediate maps and the list");

    let replay = replay_deltas(&db);
    let len = nodedb_crdt::list_ops::list_length(replay.doc(), "pages", "page-2", "content.blocks")
        .expect("list_length on replayed nested state");
    assert_eq!(len, 1);
}
