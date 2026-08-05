// SPDX-License-Identifier: Apache-2.0

//! `StorageEngine` implementation for native targets.

use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use pagedb::vfs::Vfs;

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::{
    CompactionOutcome, KvPair, StorageEngine, StorageWriteProfile, WriteOp,
};
use crate::storage::pagedb_storage::keys::{KeyBuf, ns_end, prefix_key, strip_prefix};
use crate::storage::pagedb_storage::types::PagedbStorage;

#[async_trait]
impl<V: Vfs + Clone + Send + Sync + 'static> StorageEngine for PagedbStorage<V>
where
    <V as Vfs>::LockHandle: Sync,
    <V as Vfs>::File: Sync,
{
    async fn get(&self, ns: Namespace, key: &[u8]) -> Result<Option<Vec<u8>>, LiteError> {
        let composite = KeyBuf::new(ns, key);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        // pagedb hands back a `Bytes` sharing the cached page; `StorageEngine`
        // is defined in owned `Vec<u8>`, so the borrow ends at this boundary.
        txn.get(composite.as_slice())
            .await
            .map(|opt| opt.map(|v| v.to_vec()))
            .map_err(LiteError::from)
    }

    async fn put(&self, ns: Namespace, key: &[u8], value: &[u8]) -> Result<(), LiteError> {
        let composite = prefix_key(ns, key);
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        txn.put(&composite, value).await.map_err(LiteError::from)?;
        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn delete(&self, ns: Namespace, key: &[u8]) -> Result<(), LiteError> {
        let composite = prefix_key(ns, key);
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        txn.delete(&composite).await.map_err(LiteError::from)?;
        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn scan_prefix(&self, ns: Namespace, prefix: &[u8]) -> Result<Vec<KvPair>, LiteError> {
        let ns_prefix = prefix_key(ns, prefix);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn.scan_prefix(&ns_prefix).await.map_err(LiteError::from)?;
        Ok(raw
            .into_iter()
            .map(|(k, v)| (strip_prefix(&k).to_vec(), v.to_vec()))
            .collect())
    }

    async fn batch_write(&self, ops: &[WriteOp]) -> Result<(), LiteError> {
        self.batch_write_inner(ops, None).await.map(|_| ())
    }

    async fn batch_write_profiled(
        &self,
        ops: &[WriteOp],
    ) -> Result<StorageWriteProfile, LiteError> {
        self.batch_write_inner(ops, Some(StorageWriteProfile::default()))
            .await
    }

    async fn count(&self, ns: Namespace) -> Result<u64, LiteError> {
        // No count primitive in pagedb B+ tree — scan the prefix and count.
        let ns_prefix = vec![ns as u8];
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn.scan_prefix(&ns_prefix).await.map_err(LiteError::from)?;
        Ok(raw.len() as u64)
    }

    async fn scan_range(
        &self,
        ns: Namespace,
        start: &[u8],
        limit: usize,
    ) -> Result<Vec<KvPair>, LiteError> {
        let start_key = prefix_key(ns, start);
        let end_key = ns_end(ns);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn
            .scan(&start_key, &end_key)
            .await
            .map_err(LiteError::from)?;
        Ok(raw
            .into_iter()
            .take(limit)
            .map(|(k, v)| (strip_prefix(&k).to_vec(), v.to_vec()))
            .collect())
    }

    async fn scan_range_bounded(
        &self,
        ns: Namespace,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: Option<usize>,
    ) -> Result<Vec<KvPair>, LiteError> {
        let start_key = match start {
            Some(s) => prefix_key(ns, s),
            None => vec![ns as u8],
        };
        let end_key = match end {
            Some(e) => prefix_key(ns, e),
            None => ns_end(ns),
        };
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;
        let raw = txn
            .scan(&start_key, &end_key)
            .await
            .map_err(LiteError::from)?;
        let effective_limit = limit.unwrap_or(usize::MAX);
        Ok(raw
            .into_iter()
            .take(effective_limit)
            .map(|(k, v)| (strip_prefix(&k).to_vec(), v.to_vec()))
            .collect())
    }

    async fn compact(&self) -> Result<CompactionOutcome, LiteError> {
        let stats = self.db.compact_now().await.map_err(LiteError::from)?;
        // `compact_now` repacks and truncates; it does not touch retired segment
        // files. Reclaiming those is `gc_now`, which picks up the retirements
        // that a reader pin deferred past their commit.
        let gc = self.db.gc_now().await.map_err(LiteError::from)?;
        Ok(CompactionOutcome {
            reclaimed_pages: stats.main_db_pages_reclaimed,
            segments_repacked: stats.segments_repacked,
            file_bytes_freed: stats.bytes_truncated,
            reclaimed_segments: gc.reclaimed_segments,
            segment_bytes_freed: gc.reclaimed_bytes,
        })
    }

    fn as_vector_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::vector_segment_ext::VectorSegmentExt> {
        Some(self)
    }

    fn as_array_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::array_segment_ext::ArraySegmentExt> {
        Some(self)
    }

    fn as_fts_segment_ext(&self) -> Option<&dyn crate::storage::fts_segment_ext::FtsSegmentExt> {
        Some(self)
    }

    fn as_columnar_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::columnar_segment_ext::ColumnarSegmentExt> {
        Some(self)
    }

    fn as_graph_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::graph_segment_ext::GraphSegmentExt> {
        Some(self)
    }

    fn as_spatial_segment_ext(
        &self,
    ) -> Option<&dyn crate::storage::spatial_segment_ext::SpatialSegmentExt> {
        Some(self)
    }
}

impl<V: Vfs + Clone + Send + Sync + 'static> PagedbStorage<V>
where
    <V as Vfs>::LockHandle: Sync,
    <V as Vfs>::File: Sync,
{
    async fn batch_write_inner(
        &self,
        ops: &[WriteOp],
        mut profile: Option<StorageWriteProfile>,
    ) -> Result<StorageWriteProfile, LiteError> {
        let total_started = profile.as_ref().map(|_| Instant::now());
        if let Some(profile) = profile.as_mut() {
            profile.operations = ops.len() as u64;
        }
        if ops.is_empty() {
            if let (Some(profile), Some(started)) = (profile.as_mut(), total_started) {
                profile.total = started.elapsed();
            }
            return Ok(profile.unwrap_or_default());
        }

        let begin_started = profile.as_ref().map(|_| Instant::now());
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        if let (Some(profile), Some(started)) = (profile.as_mut(), begin_started) {
            profile.begin = started.elapsed();
        }

        let prepare_started = profile.as_ref().map(|_| Instant::now());
        // Put-only batches are the dominant bulk-ingest path. Build and sort
        // their owned buffers once instead of first cloning every key solely
        // for duplicate detection and then rebuilding the same buffers.
        if ops.iter().all(|op| matches!(op, WriteOp::Put { .. })) {
            let mut puts: Vec<(Bytes, Bytes)> = ops
                .iter()
                .map(|op| match op {
                    WriteOp::Put { ns, key, value } => (
                        Bytes::from(prefix_key(*ns, key)),
                        Bytes::from(value.clone()),
                    ),
                    WriteOp::Delete { .. } => unreachable!("put-only batch"),
                })
                .collect();
            puts.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
            let unique = puts.windows(2).all(|pair| pair[0].0 != pair[1].0);
            if unique {
                if let (Some(profile), Some(started)) = (profile.as_mut(), prepare_started) {
                    profile.prepare = started.elapsed();
                }
                let apply_started = profile.as_ref().map(|_| Instant::now());
                txn.put_batch(puts).await.map_err(LiteError::from)?;
                if let (Some(profile), Some(started)) = (profile.as_mut(), apply_started) {
                    profile.apply = started.elapsed();
                }
                let commit_started = profile.as_ref().map(|_| Instant::now());
                txn.commit().await.map_err(LiteError::from)?;
                if let (Some(profile), Some(started)) = (profile.as_mut(), commit_started) {
                    profile.commit = started.elapsed();
                }
                if let (Some(profile), Some(started)) = (profile.as_mut(), total_started) {
                    profile.total = started.elapsed();
                }
                return Ok(profile.unwrap_or_default());
            }
        }

        // Detect duplicate keys (a key that appears in both a Put and a Delete,
        // or appears multiple times). When duplicates exist we fall through to
        // sequential per-op application to preserve original-order semantics.
        let all_keys: Vec<Vec<u8>> = ops
            .iter()
            .map(|op| match op {
                WriteOp::Put { ns, key, .. } => prefix_key(*ns, key),
                WriteOp::Delete { ns, key } => prefix_key(*ns, key),
            })
            .collect();
        let unique_count = {
            let mut dedup = all_keys.clone();
            dedup.sort_unstable();
            dedup.dedup();
            dedup.len()
        };

        if unique_count < all_keys.len() {
            if let (Some(profile), Some(started)) = (profile.as_mut(), prepare_started) {
                profile.prepare = started.elapsed();
            }
            let apply_started = profile.as_ref().map(|_| Instant::now());
            // Duplicate keys present — apply in order to preserve last-write semantics.
            for op in ops {
                match op {
                    WriteOp::Put { ns, key, value } => {
                        let composite = prefix_key(*ns, key);
                        txn.put(&composite, value).await.map_err(LiteError::from)?;
                    }
                    WriteOp::Delete { ns, key } => {
                        let composite = prefix_key(*ns, key);
                        txn.delete(&composite).await.map_err(LiteError::from)?;
                    }
                }
            }
            if let (Some(profile), Some(started)) = (profile.as_mut(), apply_started) {
                profile.apply = started.elapsed();
            }
        } else {
            // All keys distinct — partition into sorted puts + sorted deletes,
            // then call the batch APIs within the same WriteTxn (both commit atomically).
            let mut puts: Vec<(Bytes, Bytes)> = Vec::new();
            let mut deletes: Vec<Vec<u8>> = Vec::new();
            for op in ops {
                match op {
                    WriteOp::Put { ns, key, value } => puts.push((
                        Bytes::from(prefix_key(*ns, key)),
                        Bytes::from(value.clone()),
                    )),
                    WriteOp::Delete { ns, key } => deletes.push(prefix_key(*ns, key)),
                }
            }
            puts.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
            deletes.sort_unstable();
            if let (Some(profile), Some(started)) = (profile.as_mut(), prepare_started) {
                profile.prepare = started.elapsed();
            }
            let apply_started = profile.as_ref().map(|_| Instant::now());
            if !puts.is_empty() {
                txn.put_batch(puts).await.map_err(LiteError::from)?;
            }
            if !deletes.is_empty() {
                txn.delete_batch(deletes).await.map_err(LiteError::from)?;
            }
            if let (Some(profile), Some(started)) = (profile.as_mut(), apply_started) {
                profile.apply = started.elapsed();
            }
        }

        let commit_started = profile.as_ref().map(|_| Instant::now());
        txn.commit().await.map_err(LiteError::from)?;
        if let (Some(profile), Some(started)) = (profile.as_mut(), commit_started) {
            profile.commit = started.elapsed();
        }
        if let (Some(profile), Some(started)) = (profile.as_mut(), total_started) {
            profile.total = started.elapsed();
        }
        Ok(profile.unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pagedb_storage::PagedbStorage;

    #[tokio::test]
    async fn profiled_unique_put_batch_commits_values_and_reports_all_operations() {
        let storage = PagedbStorage::open_in_memory().await.unwrap();
        let profile = storage
            .batch_write_profiled(&[
                WriteOp::Put {
                    ns: Namespace::Graph,
                    key: b"b".to_vec(),
                    value: b"second".to_vec(),
                },
                WriteOp::Put {
                    ns: Namespace::Graph,
                    key: b"a".to_vec(),
                    value: b"first".to_vec(),
                },
            ])
            .await
            .unwrap();

        assert_eq!(profile.operations, 2);
        assert!(profile.total >= profile.begin + profile.prepare + profile.apply + profile.commit);
        assert_eq!(
            storage
                .get(Namespace::Graph, b"a")
                .await
                .unwrap()
                .as_deref(),
            Some(b"first".as_slice())
        );
        assert_eq!(
            storage
                .get(Namespace::Graph, b"b")
                .await
                .unwrap()
                .as_deref(),
            Some(b"second".as_slice())
        );
    }
}
