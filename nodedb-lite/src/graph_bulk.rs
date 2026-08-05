//! Lazy bounded bridge from sorted edge records to storage writes.

use std::time::{Duration, Instant};

use crate::error::LiteError;
use crate::graph_diagnostics::SortDiagnostics;
use crate::graph_external_sort::{ExternalEdgeMerge, SortedEdge};
use crate::graph_storage::sorted_edge_write;
use crate::storage::engine::WriteOp;

const MERGE_BATCH_RECORDS: usize = 1_000_000;
const MERGE_BATCH_KEY_BYTES: usize = 128 * 1024 * 1024;

/// Bounded, fallible write stream for a fresh edge table.
pub(crate) struct SortedBulkEntries {
    merge: ExternalEdgeMerge,
    pending: PendingEntries,
    collection: String,
    edge_label: String,
    profile: bool,
    value_regeneration: Duration,
    exhausted: bool,
}

enum PendingEntries {
    Edges(std::vec::IntoIter<SortedEdge>),
    Writes(std::vec::IntoIter<Result<WriteOp, LiteError>>),
}

impl PendingEntries {
    fn next(&mut self, collection: &str, edge_label: &str) -> Option<Result<WriteOp, LiteError>> {
        match self {
            Self::Edges(edges) => edges
                .next()
                .map(|edge| sorted_edge_write(edge, collection, edge_label)),
            Self::Writes(writes) => writes.next(),
        }
    }
}

pub(crate) struct SortedBulkSummary {
    pub(crate) value_regeneration: Duration,
    pub(crate) sort: Option<SortDiagnostics>,
}

impl SortedBulkEntries {
    pub(crate) fn new(
        merge: ExternalEdgeMerge,
        collection: impl Into<String>,
        edge_label: impl Into<String>,
        profile: bool,
    ) -> Self {
        Self {
            merge,
            pending: PendingEntries::Edges(Vec::new().into_iter()),
            collection: collection.into(),
            edge_label: edge_label.into(),
            profile,
            value_regeneration: Duration::ZERO,
            exhausted: false,
        }
    }

    /// Release the merge's temporary files and return diagnostics after storage
    /// has finished consuming the iterator.
    pub(crate) fn finish(mut self) -> SortedBulkSummary {
        SortedBulkSummary {
            value_regeneration: self.value_regeneration,
            sort: self.merge.take_diagnostics(),
        }
    }

    fn refill(&mut self) -> Result<bool, LiteError> {
        if self.exhausted {
            return Ok(false);
        }
        let edges = self
            .merge
            .next_batch(MERGE_BATCH_RECORDS, MERGE_BATCH_KEY_BYTES)?;
        match edges {
            Some(edges) => {
                if self.profile {
                    let started = Instant::now();
                    let writes = edges
                        .into_iter()
                        .map(|edge| sorted_edge_write(edge, &self.collection, &self.edge_label))
                        .collect::<Vec<_>>();
                    self.value_regeneration += started.elapsed();
                    self.pending = PendingEntries::Writes(writes.into_iter());
                } else {
                    // Avoid materializing a million encoded WriteOps at once
                    // on the canonical diagnostics-off path.
                    self.pending = PendingEntries::Edges(edges.into_iter());
                }
                Ok(true)
            }
            None => {
                self.exhausted = true;
                Ok(false)
            }
        }
    }
}

impl Iterator for SortedBulkEntries {
    type Item = Result<WriteOp, LiteError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.pending.next(&self.collection, &self.edge_label) {
                Some(write) => return Some(write),
                None => match self.refill() {
                    Ok(true) => continue,
                    Ok(false) => return None,
                    Err(error) => {
                        self.exhausted = true;
                        return Some(Err(error));
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph_external_sort::{ExternalEdgeSorter, SortedEdge};
    use crate::graph_storage::sorted_edge_write;
    use crate::storage::engine::WriteOp;

    #[test]
    fn lazy_entries_keep_duplicate_winner_across_merge_batches() {
        let mut sorter = ExternalEdgeSorter::new(2, true).unwrap();
        sorter.push(edge_key("a"), 1.0, 0).unwrap();
        sorter.push(edge_key("b"), 2.0, 1).unwrap();
        sorter.push(edge_key("a"), 3.0, 2).unwrap();
        let merge = sorter.finish().unwrap();
        let mut entries = SortedBulkEntries::new(merge, "g", "EDGE", true);
        let writes: Vec<_> = entries.by_ref().collect::<Result<_, _>>().unwrap();
        let summary = entries.finish();
        assert_eq!(writes.len(), 2);
        assert_eq!(summary.sort.unwrap().merge_unique_records, 2);
        let keys: Vec<_> = writes
            .iter()
            .map(|write| match write {
                WriteOp::Put { key, .. } => key.clone(),
                WriteOp::Delete { .. } => panic!("bulk entries must be puts"),
            })
            .collect();
        assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
        let expected = sorted_edge_write(
            SortedEdge {
                key: edge_key("a"),
                weight: 3.0,
            },
            "g",
            "EDGE",
        )
        .unwrap();
        assert!(matches!(writes.first(), Some(actual) if same_put(actual, &expected)));
    }

    fn same_put(left: &WriteOp, right: &WriteOp) -> bool {
        matches!(
            (left, right),
            (
                WriteOp::Put { key: left_key, value: left_value, .. },
                WriteOp::Put { key: right_key, value: right_value, .. },
            ) if left_key == right_key && left_value == right_value
        )
    }

    fn edge_key(destination: &str) -> Vec<u8> {
        crate::query::graph_ops::edges::edge_store_key("g", "source", "EDGE", destination)
    }
}
