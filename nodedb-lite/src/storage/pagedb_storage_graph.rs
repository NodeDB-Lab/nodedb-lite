// SPDX-License-Identifier: Apache-2.0

//! `GraphSegmentExt` implementation for `PagedbStorage<V>`.
//!
//! One pagedb segment per graph collection, storing the full CSR adjacency
//! checkpoint as a single blob.  Segment name: `graph/csr/{collection}`.
//!
//! ## Segment index (B+ tree)
//!
//! pagedb's `ReadTxn::list_segments` returns `SegmentMeta` structs but does
//! not expose the segment names.  Rather than adding a gap to pagedb, we
//! maintain a small auxiliary index in the B+ tree under `Namespace::Graph`:
//!
//! - Key: `graph:_seg_idx:{collection}` → empty value (presence = segment exists)
//!
//! `write_graph_segment` inserts this sentinel; `delete_graph_segment` removes
//! it.  The restore path in `nodedb/core/open.rs` can enumerate segments via
//! the existing `META_CSR_COLLECTIONS` B+ tree key and simply prefer the
//! segment path over the legacy KV blob.
//!
//! ## Page envelope
//!
//! Uses the same 8-byte little-endian length-prefix as the FTS and columnar
//! segment impls to survive pagedb's page-boundary zero-padding.

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use nodedb_types::Namespace;
#[cfg(not(target_arch = "wasm32"))]
use pagedb::vfs::traits::Vfs;
#[cfg(not(target_arch = "wasm32"))]
use pagedb::{RealmId, SegmentKind};

#[cfg(not(target_arch = "wasm32"))]
use crate::error::LiteError;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::graph_segment_ext::GraphSegmentExt;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::pagedb_storage::PagedbStorage;

/// pagedb page body capacity in bytes: 4096 - 40 bytes AEAD/header envelope.
#[cfg(not(target_arch = "wasm32"))]
const PAGE_BODY_CAP: usize = 4096 - 40;

/// pagedb segment name prefix for CSR adjacency segments.
#[cfg(not(target_arch = "wasm32"))]
const GRAPH_SEG_PREFIX: &str = "graph/csr/";

/// B+ tree key prefix for the auxiliary segment index sentinel keys.
#[cfg(not(target_arch = "wasm32"))]
const GRAPH_SEG_IDX_PREFIX: &str = "graph:_seg_idx:";

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl<V: Vfs + Clone + Send + Sync + 'static> GraphSegmentExt for PagedbStorage<V>
where
    <V as Vfs>::LockHandle: Sync,
    <V as Vfs>::File: Sync,
{
    async fn write_graph_segment(&self, collection: &str, bytes: &[u8]) -> Result<(), LiteError> {
        // Prepend an 8-byte little-endian length so reads can recover the
        // exact byte count after pagedb pads the last page to a full page size.
        let byte_len = bytes.len() as u64;
        let mut payload = Vec::with_capacity(8 + bytes.len());
        payload.extend_from_slice(&byte_len.to_le_bytes());
        payload.extend_from_slice(bytes);

        let chunks: Vec<&[u8]> = payload.chunks(PAGE_BODY_CAP).collect();

        let realm = RealmId::new([0u8; 16]);
        let segment_name = format!("{GRAPH_SEG_PREFIX}{collection}");

        let mut writer = self
            .db
            .create_segment(realm, SegmentKind::Unspecified)
            .await
            .map_err(LiteError::from)?;
        writer
            .append_extent(&chunks)
            .await
            .map_err(LiteError::from)?;
        let meta = writer.seal().await.map_err(LiteError::from)?;

        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;

        // Atomically replace if a segment already exists under this name.
        let link_result = txn.link_segment(&segment_name, &meta).await;
        match link_result {
            Ok(()) => {}
            Err(e) if matches!(e, pagedb::errors::PagedbError::AlreadyLinked) => {
                txn.replace_segment(&segment_name, &meta)
                    .await
                    .map_err(LiteError::from)?;
            }
            Err(e) => return Err(LiteError::from(e)),
        }

        // Write the auxiliary segment-index sentinel to the B+ tree.
        let sentinel_key = format!("{GRAPH_SEG_IDX_PREFIX}{collection}");
        txn.put(
            &crate::storage::pagedb_storage::prefix_key(Namespace::Graph, sentinel_key.as_bytes()),
            &[],
        )
        .await
        .map_err(LiteError::from)?;

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn open_graph_segment(&self, collection: &str) -> Result<Option<Box<[u8]>>, LiteError> {
        let segment_name = format!("{GRAPH_SEG_PREFIX}{collection}");
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;

        let reader = match txn.open_segment(&segment_name).await {
            Ok(r) => r,
            Err(pagedb::errors::PagedbError::NotFound) => return Ok(None),
            Err(e) => return Err(LiteError::from(e)),
        };

        // Read all data pages and concatenate.
        let meta_page_count = reader.meta().page_count;
        let index_pages = u64::from(reader.index_page_count());
        let data_page_count = meta_page_count
            .checked_sub(2 + index_pages)
            .ok_or_else(|| LiteError::Storage {
                detail: format!(
                    "graph segment '{collection}' page_count={meta_page_count} too small \
                     (index_pages={index_pages})"
                ),
            })?;

        if data_page_count == 0 {
            return Ok(Some(Box::default()));
        }

        let count_u32 = u32::try_from(data_page_count).map_err(|_| LiteError::Storage {
            detail: format!(
                "graph segment '{collection}' has too many data pages: {data_page_count}"
            ),
        })?;

        let pages = reader
            .read_range(1, count_u32)
            .await
            .map_err(|e| LiteError::Storage {
                detail: format!("pagedb graph segment read_range failed for '{collection}': {e}"),
            })?;

        let total: usize = pages.iter().map(|p| p.len()).sum();
        let mut flat = Vec::with_capacity(total);
        for page in pages {
            flat.extend_from_slice(&page);
        }

        // Strip the 8-byte length prefix.
        if flat.len() < 8 {
            return Err(LiteError::Storage {
                detail: format!(
                    "graph segment '{collection}' too small to contain length prefix: {} bytes",
                    flat.len()
                ),
            });
        }
        let byte_len = u64::from_le_bytes(flat[..8].try_into().expect("8-byte slice")) as usize;
        let end = 8_usize
            .checked_add(byte_len)
            .ok_or_else(|| LiteError::Storage {
                detail: format!("graph segment '{collection}' length prefix overflows: {byte_len}"),
            })?;
        if end > flat.len() {
            return Err(LiteError::Storage {
                detail: format!(
                    "graph segment '{collection}' declared byte_len={byte_len} exceeds \
                     available data ({} bytes after prefix)",
                    flat.len() - 8
                ),
            });
        }

        Ok(Some(flat[8..end].to_vec().into_boxed_slice()))
    }

    async fn delete_graph_segment(&self, collection: &str) -> Result<(), LiteError> {
        let segment_name = format!("{GRAPH_SEG_PREFIX}{collection}");
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        match txn.unlink_segment(&segment_name).await {
            Ok(()) => {}
            Err(pagedb::errors::PagedbError::NotLinked) => {}
            Err(e) => return Err(LiteError::from(e)),
        }

        // Remove the auxiliary segment-index sentinel from the B+ tree.
        let sentinel_key = format!("{GRAPH_SEG_IDX_PREFIX}{collection}");
        txn.delete(&crate::storage::pagedb_storage::prefix_key(
            Namespace::Graph,
            sentinel_key.as_bytes(),
        ))
        .await
        .map_err(LiteError::from)?;

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::storage::engine::StorageEngine;
    use crate::storage::graph_segment_ext::GraphSegmentExt;
    use crate::storage::pagedb_storage::PagedbStorage;
    use pagedb::vfs::memory::MemVfs;

    async fn make_storage() -> PagedbStorage<MemVfs> {
        PagedbStorage::open_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn graph_segment_roundtrip() {
        let s = make_storage().await;
        let payload: Vec<u8> = (0u8..=255).cycle().take(5000).collect();

        s.write_graph_segment("social_graph", &payload)
            .await
            .unwrap();

        let got = s
            .open_graph_segment("social_graph")
            .await
            .unwrap()
            .expect("segment must exist after write");

        assert_eq!(
            got.as_ref(),
            payload.as_slice(),
            "round-trip bytes must match"
        );
    }

    #[tokio::test]
    async fn graph_segment_open_missing_returns_none() {
        let s = make_storage().await;
        let result = s.open_graph_segment("nonexistent").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn graph_segment_delete_removes_segment() {
        let s = make_storage().await;
        s.write_graph_segment("col", b"some csr data")
            .await
            .unwrap();
        s.delete_graph_segment("col").await.unwrap();

        let result = s.open_graph_segment("col").await.unwrap();
        assert!(result.is_none(), "segment must be gone after delete");
    }

    #[tokio::test]
    async fn graph_segment_delete_nonexistent_is_noop() {
        let s = make_storage().await;
        s.delete_graph_segment("ghost").await.unwrap();
    }

    #[tokio::test]
    async fn graph_segment_replace_updates_content() {
        let s = make_storage().await;
        s.write_graph_segment("col", b"original").await.unwrap();
        s.write_graph_segment("col", b"updated").await.unwrap();

        let got = s
            .open_graph_segment("col")
            .await
            .unwrap()
            .expect("segment must exist");
        assert_eq!(got.as_ref(), b"updated", "second write must replace first");
    }

    #[tokio::test]
    async fn graph_segment_roundtrip_large_payload() {
        let s = make_storage().await;
        // Payload large enough to span multiple pagedb pages.
        let payload: Vec<u8> = (0u8..=255).cycle().take(128 * 1024).collect();
        s.write_graph_segment("large_col", &payload).await.unwrap();
        let got = s
            .open_graph_segment("large_col")
            .await
            .unwrap()
            .expect("segment must exist");
        assert_eq!(got.len(), payload.len());
        assert_eq!(got.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn graph_segment_as_graph_segment_ext_is_some() {
        let s = make_storage().await;
        assert!(
            s.as_graph_segment_ext().is_some(),
            "PagedbStorage must return Some from as_graph_segment_ext"
        );
    }
}
