//! Compensation handling simulation tests — no Origin required.
//!
//! Tests the full compensation flow: delta rejected → rollback → callback.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use nodedb_client::NodeDb;
use nodedb_lite::sync::*;
use nodedb_lite::{NodeDbLite, RedbStorage};
use nodedb_types::document::Document;
use nodedb_types::sync::compensation::CompensationHint;
use nodedb_types::sync::wire::*;
use nodedb_types::value::Value;

async fn open_db() -> NodeDbLite<RedbStorage> {
    let s = RedbStorage::open_in_memory().unwrap();
    NodeDbLite::open(s, 1).await.unwrap()
}

#[tokio::test]
async fn reject_unique_violation_rolls_back_document() {
    let db = open_db().await;

    // Write a document.
    let mut doc = Document::new("user-alice");
    doc.set("username", Value::String("alice".into()));
    db.document_put("users", doc).await.unwrap();

    // Verify it exists.
    assert!(
        db.document_get("users", "user-alice")
            .await
            .unwrap()
            .is_some()
    );

    // Get the mutation ID.
    let deltas = db.pending_crdt_deltas().unwrap();
    let mid = deltas[0].mutation_id;

    // Simulate Origin rejection.
    db.reject_delta(mid).unwrap();

    // Document should be rolled back.
    assert!(
        db.document_get("users", "user-alice")
            .await
            .unwrap()
            .is_none(),
        "rejected document should not exist after rollback"
    );
}

#[tokio::test]
async fn compensation_handler_receives_typed_hint() {
    let count = Arc::new(AtomicU32::new(0));
    let last_code = Arc::new(std::sync::Mutex::new(String::new()));

    let count_c = count.clone();
    let code_c = last_code.clone();

    let client = Arc::new(SyncClient::new(
        SyncConfig::new("wss://localhost/sync", "jwt"),
        1,
    ));

    client.set_compensation_handler(Arc::new(move |event: CompensationEvent| {
        count_c.fetch_add(1, Ordering::Relaxed);
        *code_c.lock().unwrap() = event.hint.code().to_string();
    }));

    // Simulate different rejection types.
    client
        .handle_delta_reject(&DeltaRejectMsg {
            mutation_id: 1,
            reason: "unique".into(),
            compensation: Some(CompensationHint::UniqueViolation {
                field: "email".into(),
                conflicting_value: "a@b.com".into(),
            }),
        })
        .await;
    assert_eq!(count.load(Ordering::Relaxed), 1);
    assert_eq!(*last_code.lock().unwrap(), "UNIQUE_VIOLATION");

    client
        .handle_delta_reject(&DeltaRejectMsg {
            mutation_id: 2,
            reason: "fk".into(),
            compensation: Some(CompensationHint::ForeignKeyMissing {
                referenced_id: "user-999".into(),
            }),
        })
        .await;
    assert_eq!(count.load(Ordering::Relaxed), 2);
    assert_eq!(*last_code.lock().unwrap(), "FK_MISSING");

    client
        .handle_delta_reject(&DeltaRejectMsg {
            mutation_id: 3,
            reason: "rls".into(),
            compensation: Some(CompensationHint::PermissionDenied),
        })
        .await;
    assert_eq!(count.load(Ordering::Relaxed), 3);
    assert_eq!(*last_code.lock().unwrap(), "PERMISSION_DENIED");

    client
        .handle_delta_reject(&DeltaRejectMsg {
            mutation_id: 4,
            reason: "rate".into(),
            compensation: Some(CompensationHint::RateLimited {
                retry_after_ms: 5000,
            }),
        })
        .await;
    assert_eq!(count.load(Ordering::Relaxed), 4);
    assert_eq!(*last_code.lock().unwrap(), "RATE_LIMITED");
}

#[tokio::test]
async fn buffered_compensations_drain_to_late_handler() {
    let registry = CompensationRegistry::new();

    // Dispatch before handler is set — should buffer.
    registry.dispatch(CompensationEvent {
        mutation_id: 1,
        collection: "users".into(),
        document_id: "u1".into(),
        hint: CompensationHint::UniqueViolation {
            field: "email".into(),
            conflicting_value: "a@b.com".into(),
        },
    });
    registry.dispatch(CompensationEvent {
        mutation_id: 2,
        collection: "users".into(),
        document_id: "u2".into(),
        hint: CompensationHint::PermissionDenied,
    });

    assert_eq!(registry.buffered_count(), 2);

    // Set handler — should drain buffer.
    let count = Arc::new(AtomicU32::new(0));
    let count_c = count.clone();
    registry.set_handler(Arc::new(move |_: CompensationEvent| {
        count_c.fetch_add(1, Ordering::Relaxed);
    }));

    assert_eq!(count.load(Ordering::Relaxed), 2);
    assert_eq!(registry.buffered_count(), 0);
}
