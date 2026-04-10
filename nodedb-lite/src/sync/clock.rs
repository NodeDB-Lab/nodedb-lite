//! Vector clock for sync handshake and delta tracking.
//!
//! Tracks per-peer versions so the edge can tell Origin "give me everything
//! since my last known state" and Origin can respond with exactly the
//! missing deltas.

use std::collections::HashMap;

/// Vector clock: maps peer IDs to their latest known counter.
///
/// On handshake, the edge sends its clock to Origin. Origin compares
/// against its own clock and sends back deltas for any peer whose
/// counter is ahead of the edge's view.
#[derive(Debug, Clone, Default)]
pub struct VectorClock {
    /// `peer_id → counter` (Loro version vector entries).
    entries: HashMap<u64, u64>,
}

impl VectorClock {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the counter for a peer.
    pub fn set(&mut self, peer_id: u64, counter: u64) {
        self.entries.insert(peer_id, counter);
    }

    /// Get the counter for a peer (0 if unknown).
    pub fn get(&self, peer_id: u64) -> u64 {
        self.entries.get(&peer_id).copied().unwrap_or(0)
    }

    /// Advance a peer's counter if the new value is greater.
    pub fn advance(&mut self, peer_id: u64, counter: u64) {
        let entry = self.entries.entry(peer_id).or_insert(0);
        if counter > *entry {
            *entry = counter;
        }
    }

    /// Merge another clock into this one (take max per peer).
    pub fn merge(&mut self, other: &VectorClock) {
        for (&peer, &counter) in &other.entries {
            self.advance(peer, counter);
        }
    }

    /// Check if this clock dominates another (all entries >= other's).
    pub fn dominates(&self, other: &VectorClock) -> bool {
        other
            .entries
            .iter()
            .all(|(&peer, &counter)| self.get(peer) >= counter)
    }

    /// Compute which peers have advanced beyond our knowledge.
    ///
    /// Returns `(peer_id, their_counter, our_counter)` for peers where
    /// `remote` is ahead of `self`.
    pub fn diff(&self, remote: &VectorClock) -> Vec<(u64, u64, u64)> {
        let mut diffs = Vec::new();
        for (&peer, &remote_counter) in &remote.entries {
            let local_counter = self.get(peer);
            if remote_counter > local_counter {
                diffs.push((peer, remote_counter, local_counter));
            }
        }
        diffs
    }

    /// Export as a map of `peer_id_hex → counter` for wire format.
    pub fn to_wire(&self) -> HashMap<String, u64> {
        self.entries
            .iter()
            .map(|(&peer, &counter)| (format!("{peer:016x}"), counter))
            .collect()
    }

    /// Import from wire format `peer_id_hex → counter`.
    pub fn from_wire(wire: &HashMap<String, u64>) -> Self {
        let entries = wire
            .iter()
            .filter_map(|(hex, &counter)| {
                u64::from_str_radix(hex, 16)
                    .ok()
                    .map(|peer| (peer, counter))
            })
            .collect();
        Self { entries }
    }

    /// Number of peers tracked.
    pub fn peer_count(&self) -> usize {
        self.entries.len()
    }

    /// All tracked peer IDs.
    pub fn peers(&self) -> Vec<u64> {
        self.entries.keys().copied().collect()
    }

    /// Export as HashMap<u64, u64> for direct access.
    pub fn as_map(&self) -> &HashMap<u64, u64> {
        &self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_clock_is_empty() {
        let c = VectorClock::new();
        assert_eq!(c.peer_count(), 0);
        assert_eq!(c.get(1), 0);
    }

    #[test]
    fn set_and_get() {
        let mut c = VectorClock::new();
        c.set(1, 42);
        assert_eq!(c.get(1), 42);
        assert_eq!(c.get(2), 0);
    }

    #[test]
    fn advance_only_increases() {
        let mut c = VectorClock::new();
        c.set(1, 10);
        c.advance(1, 5); // Should not decrease.
        assert_eq!(c.get(1), 10);
        c.advance(1, 15); // Should increase.
        assert_eq!(c.get(1), 15);
    }

    #[test]
    fn merge_takes_max() {
        let mut a = VectorClock::new();
        a.set(1, 10);
        a.set(2, 20);

        let mut b = VectorClock::new();
        b.set(1, 15);
        b.set(3, 30);

        a.merge(&b);
        assert_eq!(a.get(1), 15); // b was higher.
        assert_eq!(a.get(2), 20); // only in a.
        assert_eq!(a.get(3), 30); // only in b.
    }

    #[test]
    fn dominates() {
        let mut a = VectorClock::new();
        a.set(1, 10);
        a.set(2, 20);

        let mut b = VectorClock::new();
        b.set(1, 5);
        b.set(2, 20);

        assert!(a.dominates(&b));
        assert!(!b.dominates(&a)); // b.get(1) < a.get(1).
    }

    #[test]
    fn diff_finds_ahead_peers() {
        let mut local = VectorClock::new();
        local.set(1, 10);
        local.set(2, 20);

        let mut remote = VectorClock::new();
        remote.set(1, 15); // ahead
        remote.set(2, 20); // same
        remote.set(3, 5); // new peer

        let diffs = local.diff(&remote);
        assert_eq!(diffs.len(), 2);
        // peer 1: remote=15, local=10
        assert!(diffs.iter().any(|&(p, r, l)| p == 1 && r == 15 && l == 10));
        // peer 3: remote=5, local=0
        assert!(diffs.iter().any(|&(p, r, l)| p == 3 && r == 5 && l == 0));
    }

    #[test]
    fn wire_roundtrip() {
        let mut c = VectorClock::new();
        c.set(1, 42);
        c.set(255, 100);

        let wire = c.to_wire();
        let restored = VectorClock::from_wire(&wire);
        assert_eq!(restored.get(1), 42);
        assert_eq!(restored.get(255), 100);
    }
}
