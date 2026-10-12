// SPDX-License-Identifier: BUSL-1.1

//! CRDT history compaction and retained memory accounting.

use crate::error::LiteError;

use super::types::CrdtEngine;

impl CrdtEngine {
    /// Compact Loro history on every collection to prevent unbounded growth.
    ///
    /// Replaces each internal LoroDoc with a shallow snapshot. Historical
    /// operations are discarded. Current state is fully preserved.
    pub fn compact_history(&mut self) -> Result<(), LiteError> {
        for (collection, state) in &mut self.states {
            state.compact_history().map_err(|e| {
                LiteError::crdt(&format!("history compaction for '{collection}'"), e)
            })?;
        }
        // Compaction rewrites the document without advancing its frontier, so
        // neither the persisted base nor the updates on top of it describe the
        // document any more, and the discarded history means an update export
        // from the old frontier may not even be possible. Dropping both marks
        // forces a fresh checkpoint, which also deletes the stale updates —
        // `next_delta_seq` is deliberately kept, since it is the count of the
        // entries that checkpoint has to delete.
        //
        // Advancing the epoch is what keeps a flush that is committing right
        // now from putting the marks back: its writes were exported from the
        // document this call just replaced.
        self.flushed_versions.clear();
        self.checkpoint_bytes.clear();
        self.delta_bytes.clear();
        let compacted: Vec<String> = self.states.keys().cloned().collect();
        for collection in compacted {
            self.advance_state_epoch(&collection);
        }
        Ok(())
    }

    /// Estimated memory usage includes retained live IDs, not a hard import ceiling.
    pub fn estimated_memory_bytes(&self) -> usize {
        let state_bytes: usize = self.states.values().fold(0usize, |bytes, state| {
            bytes.saturating_add(state.estimated_memory_bytes())
        });
        let delta_bytes: usize = self.pending_deltas.iter().fold(0usize, |bytes, delta| {
            bytes.saturating_add(delta.delta_bytes.len())
        });
        state_bytes
            .saturating_add(delta_bytes)
            .saturating_add(self.live_id_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::CrdtEngine;
    use loro::LoroValue;

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
}
