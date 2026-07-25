// SPDX-License-Identifier: BUSL-1.1

use loro::LoroValue;

use super::types::CrdtEngine;

#[test]
fn create_engine() {
    let engine = CrdtEngine::new(1).unwrap();
    assert_eq!(engine.peer_id(), 1);
    assert_eq!(engine.pending_count(), 0);
}

#[test]
fn upsert_generates_delta() {
    let mut engine = CrdtEngine::new(1).unwrap();
    let mid = engine
        .upsert(
            "users",
            "u1",
            &[("name", LoroValue::String("Alice".into()))],
        )
        .unwrap();

    assert_eq!(mid, 1);
    assert_eq!(engine.pending_count(), 1);
    assert!(!engine.pending_deltas()[0].delta_bytes.is_empty());
}

#[test]
fn read_after_upsert() {
    let mut engine = CrdtEngine::new(1).unwrap();
    engine
        .upsert("users", "u1", &[("age", LoroValue::I64(30))])
        .unwrap();

    assert!(engine.exists("users", "u1"));
    let val = engine.read("users", "u1").unwrap();
    // The value should be a map containing "age": 30.
    assert!(format!("{val:?}").contains("30"));
}

#[test]
fn delete_generates_delta() {
    let mut engine = CrdtEngine::new(1).unwrap();
    engine
        .upsert("users", "u1", &[("name", LoroValue::String("X".into()))])
        .unwrap();
    let mid = engine.delete("users", "u1").unwrap();

    assert_eq!(mid, 2); // Second mutation.
    assert_eq!(engine.pending_count(), 2);
    assert!(!engine.exists("users", "u1"));
}

#[test]
fn acknowledge_removes_deltas() {
    let mut engine = CrdtEngine::new(1).unwrap();
    engine
        .upsert("a", "1", &[("x", LoroValue::I64(1))])
        .unwrap(); // mid=1
    engine
        .upsert("a", "2", &[("x", LoroValue::I64(2))])
        .unwrap(); // mid=2
    engine
        .upsert("a", "3", &[("x", LoroValue::I64(3))])
        .unwrap(); // mid=3

    assert_eq!(engine.pending_count(), 3);
    // Origin acknowledges mid=2. Acks are per-mutation, so only that delta is
    // retired — mid=1 has not been acknowledged and must stay queued.
    engine.acknowledge(2);
    let remaining: Vec<u64> = engine
        .pending_deltas()
        .iter()
        .map(|d| d.mutation_id)
        .collect();
    assert_eq!(remaining, vec![1, 3]);

    engine.acknowledge(1);
    engine.acknowledge(3);
    assert_eq!(engine.pending_count(), 0);
}

#[test]
fn reject_delta_rolls_back() {
    let mut engine = CrdtEngine::new(1).unwrap();
    let mid = engine
        .upsert(
            "users",
            "u1",
            &[("name", LoroValue::String("Alice".into()))],
        )
        .unwrap();

    assert!(engine.exists("users", "u1"));
    let rejected = engine.reject_delta(mid).unwrap();
    assert_eq!(rejected.collection, "users");
    assert!(!engine.exists("users", "u1"));
    assert_eq!(engine.pending_count(), 0);
}

#[test]
fn snapshot_and_restore() {
    let mut engine1 = CrdtEngine::new(1).unwrap();
    engine1
        .upsert(
            "docs",
            "d1",
            &[("title", LoroValue::String("Hello".into()))],
        )
        .unwrap();
    engine1
        .upsert(
            "docs",
            "d2",
            &[("title", LoroValue::String("World".into()))],
        )
        .unwrap();

    let snapshot = engine1.export_snapshot().unwrap();
    assert!(!snapshot.is_empty());

    let engine2 = CrdtEngine::from_snapshot(2, &snapshot).unwrap();
    assert!(engine2.exists("docs", "d1"));
    assert!(engine2.exists("docs", "d2"));
}

#[test]
fn import_remote_deltas() {
    let mut engine1 = CrdtEngine::new(1).unwrap();
    engine1
        .upsert("items", "i1", &[("val", LoroValue::I64(42))])
        .unwrap();

    // Export engine1's state as a snapshot and import into engine2.
    let snapshot = engine1.export_snapshot().unwrap();
    let engine2 = CrdtEngine::new(2).unwrap();
    engine2.import_remote(&snapshot).unwrap();

    assert!(engine2.exists("items", "i1"));
}

#[test]
fn pending_deltas_persistence() {
    let mut engine = CrdtEngine::new(1).unwrap();
    engine
        .upsert("a", "1", &[("x", LoroValue::I64(1))])
        .unwrap();
    engine
        .upsert("b", "2", &[("y", LoroValue::I64(2))])
        .unwrap();

    let bytes = engine.serialize_pending_deltas().unwrap();
    assert!(!bytes.is_empty());

    let mut engine2 = CrdtEngine::new(1).unwrap();
    engine2.restore_pending_deltas(&bytes);
    assert_eq!(engine2.pending_count(), 2);
    // Mutation ID counter should be advanced past restored deltas.
    let mid = engine2
        .upsert("c", "3", &[("z", LoroValue::I64(3))])
        .unwrap();
    assert!(mid > 2);
}

#[test]
fn vector_clock_export() {
    let mut engine = CrdtEngine::new(1).unwrap();
    engine
        .upsert("x", "1", &[("v", LoroValue::I64(1))])
        .unwrap();

    let clock = engine.export_vector_clock();
    assert!(!clock.is_empty());
    // Should contain our peer_id's entry.
    let our_key = format!("{:016x}", engine.peer_id());
    assert!(
        clock.contains_key(&our_key),
        "clock should contain peer {our_key}: {clock:?}"
    );
}

#[test]
fn compact_history_preserves_state() {
    let mut engine = CrdtEngine::new(1).unwrap();
    for i in 0..50 {
        engine
            .upsert(
                "items",
                &format!("i{i}"),
                &[("val", LoroValue::I64(i as i64))],
            )
            .unwrap();
    }

    let mem_before = engine.estimated_memory_bytes();
    engine.compact_history().unwrap();

    // State should be preserved.
    assert!(engine.exists("items", "i0"));
    assert!(engine.exists("items", "i49"));

    // New operations should still work.
    engine
        .upsert("items", "i50", &[("val", LoroValue::I64(50))])
        .unwrap();
    assert!(engine.exists("items", "i50"));

    // Memory should be reduced (or at least not much larger).
    let mem_after = engine.estimated_memory_bytes();
    // History compaction should not increase memory significantly.
    assert!(
        mem_after <= mem_before * 2,
        "memory after compact ({mem_after}) should not be much larger than before ({mem_before})"
    );
}

#[test]
fn list_ids() {
    let mut engine = CrdtEngine::new(1).unwrap();
    engine
        .upsert("col", "a", &[("x", LoroValue::I64(1))])
        .unwrap();
    engine
        .upsert("col", "b", &[("x", LoroValue::I64(2))])
        .unwrap();

    let mut ids = engine.list_ids("col");
    ids.sort();
    assert_eq!(ids, vec!["a", "b"]);
}

#[test]
fn acked_version_tracking() {
    let mut engine = CrdtEngine::new(1).unwrap();
    assert_eq!(engine.acked_version("users"), 0);

    engine.set_acked_version("users", 42);
    assert_eq!(engine.acked_version("users"), 42);
}

#[test]
fn memory_estimation() {
    let mut engine = CrdtEngine::new(1).unwrap();
    let before = engine.estimated_memory_bytes();

    for i in 0..100 {
        engine
            .upsert("big", &format!("k{i}"), &[("data", LoroValue::I64(i))])
            .unwrap();
    }

    let after = engine.estimated_memory_bytes();
    assert!(after > before);
}

/// `acknowledge` retires every pending delta with `mutation_id <= acked`,
/// not just the one that was acknowledged. Acks are per-mutation and can
/// arrive out of order, so retiring a range discards deltas Origin never
/// acknowledged — one late ack silently drops the whole backlog behind it.
#[test]
fn acknowledge_retires_only_the_acknowledged_delta() {
    let mut engine = CrdtEngine::new(1).unwrap();

    let first = engine
        .upsert("notes", "a", &[("v", LoroValue::I64(1))])
        .unwrap();
    let second = engine
        .upsert("notes", "b", &[("v", LoroValue::I64(2))])
        .unwrap();
    let third = engine
        .upsert("notes", "c", &[("v", LoroValue::I64(3))])
        .unwrap();
    assert_eq!(engine.pending_deltas().len(), 3);

    // Origin acknowledges only the middle mutation.
    engine.acknowledge(second);

    let remaining: Vec<u64> = engine
        .pending_deltas()
        .iter()
        .map(|d| d.mutation_id)
        .collect();
    assert!(
        remaining.contains(&first),
        "acknowledging {second} also retired the un-acknowledged delta \
         {first}; its write is lost. remaining: {remaining:?}"
    );
    assert!(remaining.contains(&third), "remaining: {remaining:?}");
    assert!(!remaining.contains(&second));
}
