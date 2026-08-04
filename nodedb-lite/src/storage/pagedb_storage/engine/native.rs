// SPDX-License-Identifier: Apache-2.0

//! `StorageEngine` implementation for native targets.

use async_trait::async_trait;
use bytes::Bytes;
use pagedb::vfs::Vfs;

use nodedb_types::Namespace;

use crate::error::LiteError;
use crate::storage::engine::{CompactionOutcome, KvPair, StorageEngine, WriteOp};
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
        if ops.is_empty() {
            return Ok(());
        }

        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;

        // Detect duplicate keys (a key that appears in both a Put and a Delete,
        // or appears multiple times). When duplicates exist we fall through to
        // sequential per-op application to preserve original-order semantics.
        // Uniqueness check: if all keys are distinct we can use the fast batch path.
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
        } else {
            // All keys distinct — partition into sorted puts + sorted deletes,
            // then call the batch APIs within the same WriteTxn (both commit atomically).
            // `put_batch` takes `Bytes` so the tree can store the buffer without
            // re-copying it; `delete_batch` still takes owned key vectors.
            let mut puts: Vec<(Bytes, Bytes)> = Vec::new();
            let mut deletes: Vec<Vec<u8>> = Vec::new();

            for op in ops {
                match op {
                    WriteOp::Put { ns, key, value } => {
                        puts.push((
                            Bytes::from(prefix_key(*ns, key)),
                            Bytes::from(value.clone()),
                        ));
                    }
                    WriteOp::Delete { ns, key } => {
                        deletes.push(prefix_key(*ns, key));
                    }
                }
            }

            puts.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
            deletes.sort_unstable();

            if !puts.is_empty() {
                txn.put_batch(puts).await.map_err(LiteError::from)?;
            }
            if !deletes.is_empty() {
                txn.delete_batch(deletes).await.map_err(LiteError::from)?;
            }
        }

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
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
