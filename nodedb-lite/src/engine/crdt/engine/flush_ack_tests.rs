// SPDX-License-Identifier: BUSL-1.1

//! What a flush may still claim once it has let go of the engine.
//!
//! A flush plans and exports under the engine lock, releases it while its batch
//! commits, then re-takes it to record what is now durable. Anything that
//! happens in that window changed state the batch does not carry, so the
//! acknowledgement has to be able to tell the two apart. These assert that it
//! does, for each way the window can be used.

use loro::LoroValue;

use super::types::CrdtEngine;

/// A write queued while the batch was committing was not in it. Retiring its
/// dirty mark strands it: an append-only queue is only revisited when it
/// changes, so the entry would sit in memory until the process ended and the
/// write it carries would never reach Origin.
#[test]
fn a_delta_queued_during_a_flush_is_not_retired_unwritten() {
    let mut engine = CrdtEngine::new(1).unwrap();
    engine
        .upsert("users", "u1", &[("n", LoroValue::I64(1))])
        .unwrap();

    let planned: Vec<(u64, u64)> = engine
        .pending_deltas_needing_write()
        .map(|(delta, revision)| (delta.mutation_id, revision))
        .collect();
    assert_eq!(planned.len(), 1);

    // The batch is committing. A second write lands before it is acknowledged.
    let queued_during = engine
        .upsert("users", "u2", &[("n", LoroValue::I64(2))])
        .unwrap();

    engine.mark_pending_deltas_persisted(planned);

    let still_dirty: Vec<u64> = engine
        .pending_deltas_needing_write()
        .map(|(delta, _)| delta.mutation_id)
        .collect();
    assert_eq!(
        still_dirty,
        vec![queued_during],
        "the entry queued while the batch was in flight was not in it, so it must still be \
         waiting to be written"
    );
    assert_eq!(
        engine.pending_delta_write_count(),
        1,
        "only the entry the batch actually carried counts as written"
    );
}

/// The same window catches an *edit*, not only an insertion: assigning a stream
/// seq rewrites an entry that is already on disk, so its stored form goes stale
/// and it has to be written again.
#[test]
fn a_delta_resequenced_during_a_flush_stays_dirty() {
    let mut engine = CrdtEngine::new(1).unwrap();
    let mid = engine
        .upsert("users", "u1", &[("n", LoroValue::I64(1))])
        .unwrap();

    let planned: Vec<(u64, u64)> = engine
        .pending_deltas_needing_write()
        .map(|(delta, revision)| (delta.mutation_id, revision))
        .collect();

    // The batch is committing. The delta is sent and is assigned its seq.
    engine.set_pending_delta_seq(mid, 7);

    engine.mark_pending_deltas_persisted(planned);

    assert!(
        engine.has_unpersisted_deltas(),
        "the stored entry carries seq 0 while the queue carries seq 7 — a resend after a \
         restart would use the wrong seq, so it must be rewritten"
    );
}

/// An acknowledgement that arrives with nothing to report leaves the queue
/// alone: replaying one must not resurrect an entry or double-count a write.
#[test]
fn acknowledging_the_same_batch_twice_changes_nothing() {
    let mut engine = CrdtEngine::new(1).unwrap();
    engine
        .upsert("users", "u1", &[("n", LoroValue::I64(1))])
        .unwrap();

    let planned: Vec<(u64, u64)> = engine
        .pending_deltas_needing_write()
        .map(|(delta, revision)| (delta.mutation_id, revision))
        .collect();

    engine.mark_pending_deltas_persisted(planned.clone());
    engine.mark_pending_deltas_persisted(planned);

    assert!(!engine.has_unpersisted_deltas());
    assert_eq!(
        engine.pending_delta_write_count(),
        1,
        "the second report describes the same write, not another one"
    );
}

/// Compaction replaces a document without moving its frontier, so a flush that
/// planned against the previous form cannot be recognised as stale by frontier
/// alone. A compaction landing while the batch commits must leave the
/// collection needing a fresh checkpoint.
#[test]
fn a_compaction_during_a_flush_is_not_undone_by_its_acknowledgement() {
    let mut engine = CrdtEngine::new(1).unwrap();
    engine
        .upsert("users", "u1", &[("n", LoroValue::I64(1))])
        .unwrap();

    let plan = engine.plan_persistence().unwrap();
    assert_eq!(plan.len(), 1);
    let persisted: Vec<_> = plan.iter().map(|write| write.persisted()).collect();

    // The batch is committing. Compaction discards the history underneath it.
    engine.compact_history().unwrap();

    engine.mark_persisted(persisted);

    let replan = engine.plan_persistence().unwrap();
    assert_eq!(
        replan.len(),
        1,
        "the compacted document is not what the in-flight batch wrote, so it must still be \
         planned for"
    );
    assert!(
        matches!(replan[0].kind, super::CrdtWriteKind::Checkpoint { .. }),
        "and as a fresh checkpoint — an update exported from the discarded history does not \
         apply to the base on disk"
    );
}
