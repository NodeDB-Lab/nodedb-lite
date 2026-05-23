// SPDX-License-Identifier: Apache-2.0

//! `SpatialSegmentExt` implementation for `PagedbStorage<V>`.
//!
//! One pagedb segment per (collection, field) pair, storing the full
//! CRC32C-wrapped R-tree checkpoint as a single blob.
//! Segment name: `spatial/rtree/{collection}/{field}`.
//!
//! ## CRC32C envelope
//!
//! The R-tree bytes stored here are already wrapped with `checksum::wrap` by
//! the caller (`checkpoint.rs`). The pagedb segment path does NOT apply a
//! second wrap — it stores the already-wrapped bytes verbatim. On read,
//! `checkpoint.rs` calls `checksum::unwrap` as it always has. This matches
//! the legacy KV path exactly: no double-wrap, no missed unwrap.
//!
//! ## Page envelope
//!
//! Uses the same 8-byte little-endian length-prefix as FTS, columnar, and graph
//! segment impls to survive pagedb's page-boundary zero-padding.
//!
//! ## Sentinel index
//!
//! `_collections` on the B+ tree already catalogs all (collection, field) pairs,
//! so no auxiliary sentinel index is needed here (unlike graph/FTS). The restore
//! path iterates `_collections` and tries `open_spatial_segment` per pair,
//! falling back to the legacy KV blob if the segment is absent.

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use pagedb::vfs::traits::Vfs;
#[cfg(not(target_arch = "wasm32"))]
use pagedb::{RealmId, SegmentKind};

#[cfg(not(target_arch = "wasm32"))]
use crate::error::LiteError;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::pagedb_storage::PagedbStorage;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::spatial_segment_ext::SpatialSegmentExt;

/// pagedb page body capacity in bytes: 4096 - 40 bytes AEAD/header envelope.
#[cfg(not(target_arch = "wasm32"))]
const PAGE_BODY_CAP: usize = 4096 - 40;

/// pagedb segment name prefix for R-tree checkpoint segments.
#[cfg(not(target_arch = "wasm32"))]
const SPATIAL_SEG_PREFIX: &str = "spatial/rtree/";

/// Build the segment name for a (collection, field) pair.
///
/// Uses `/` as the separator between collection and field so that the full
/// path is `spatial/rtree/{collection}/{field}` — consistent with the
/// established `{engine}/{kind}/{identifier}` convention.
#[cfg(not(target_arch = "wasm32"))]
fn segment_name(collection: &str, field: &str) -> String {
    format!("{SPATIAL_SEG_PREFIX}{collection}/{field}")
}

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl<V: Vfs + Clone + Send + Sync + 'static> SpatialSegmentExt for PagedbStorage<V>
where
    <V as Vfs>::LockHandle: Sync,
    <V as Vfs>::File: Sync,
{
    async fn write_spatial_segment(
        &self,
        collection: &str,
        field: &str,
        bytes: &[u8],
    ) -> Result<(), LiteError> {
        // Prepend an 8-byte little-endian length so reads can recover the
        // exact byte count after pagedb pads the last page to a full page size.
        let byte_len = bytes.len() as u64;
        let mut payload = Vec::with_capacity(8 + bytes.len());
        payload.extend_from_slice(&byte_len.to_le_bytes());
        payload.extend_from_slice(bytes);

        let chunks: Vec<&[u8]> = payload.chunks(PAGE_BODY_CAP).collect();

        let realm = RealmId::new([0u8; 16]);
        let name = segment_name(collection, field);

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
        let link_result = txn.link_segment(&name, &meta).await;
        match link_result {
            Ok(()) => {}
            Err(e) if matches!(e, pagedb::errors::PagedbError::AlreadyLinked) => {
                txn.replace_segment(&name, &meta)
                    .await
                    .map_err(LiteError::from)?;
            }
            Err(e) => return Err(LiteError::from(e)),
        }

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn open_spatial_segment(
        &self,
        collection: &str,
        field: &str,
    ) -> Result<Option<Box<[u8]>>, LiteError> {
        let name = segment_name(collection, field);
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;

        let reader = match txn.open_segment(&name).await {
            Ok(r) => r,
            Err(pagedb::errors::PagedbError::NotFound) => return Ok(None),
            Err(e) => return Err(LiteError::from(e)),
        };

        let meta_page_count = reader.meta().page_count;
        let index_pages = u64::from(reader.index_page_count());
        let data_page_count = meta_page_count
            .checked_sub(2 + index_pages)
            .ok_or_else(|| LiteError::Storage {
                detail: format!(
                    "spatial segment '{name}' page_count={meta_page_count} too small \
                     (index_pages={index_pages})"
                ),
            })?;

        if data_page_count == 0 {
            return Ok(Some(Box::default()));
        }

        let count_u32 = u32::try_from(data_page_count).map_err(|_| LiteError::Storage {
            detail: format!("spatial segment '{name}' has too many data pages: {data_page_count}"),
        })?;

        let pages = reader
            .read_range(1, count_u32)
            .await
            .map_err(|e| LiteError::Storage {
                detail: format!("pagedb spatial segment read_range failed for '{name}': {e}"),
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
                    "spatial segment '{name}' too small to contain length prefix: {} bytes",
                    flat.len()
                ),
            });
        }
        let byte_len = u64::from_le_bytes(flat[..8].try_into().expect("8-byte slice")) as usize;
        let end = 8_usize
            .checked_add(byte_len)
            .ok_or_else(|| LiteError::Storage {
                detail: format!("spatial segment '{name}' length prefix overflows: {byte_len}"),
            })?;
        if end > flat.len() {
            return Err(LiteError::Storage {
                detail: format!(
                    "spatial segment '{name}' declared byte_len={byte_len} exceeds \
                     available data ({} bytes after prefix)",
                    flat.len() - 8
                ),
            });
        }

        Ok(Some(flat[8..end].to_vec().into_boxed_slice()))
    }

    async fn delete_spatial_segment(&self, collection: &str, field: &str) -> Result<(), LiteError> {
        let name = segment_name(collection, field);
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        match txn.unlink_segment(&name).await {
            Ok(()) => {}
            Err(pagedb::errors::PagedbError::NotLinked) => {}
            Err(e) => return Err(LiteError::from(e)),
        }
        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::storage::engine::StorageEngine;
    use crate::storage::pagedb_storage::PagedbStorage;
    use crate::storage::spatial_segment_ext::SpatialSegmentExt;
    use pagedb::vfs::memory::MemVfs;

    async fn make_storage() -> PagedbStorage<MemVfs> {
        PagedbStorage::open_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn spatial_segment_roundtrip() {
        let s = make_storage().await;
        let payload: Vec<u8> = (0u8..=255).cycle().take(5000).collect();

        s.write_spatial_segment("orders", "location", &payload)
            .await
            .unwrap();

        let got = s
            .open_spatial_segment("orders", "location")
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
    async fn spatial_segment_open_missing_returns_none() {
        let s = make_storage().await;
        let result = s.open_spatial_segment("nonexistent", "geom").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn spatial_segment_delete_removes_segment() {
        let s = make_storage().await;
        s.write_spatial_segment("col", "field", b"some rtree data")
            .await
            .unwrap();
        s.delete_spatial_segment("col", "field").await.unwrap();

        let result = s.open_spatial_segment("col", "field").await.unwrap();
        assert!(result.is_none(), "segment must be gone after delete");
    }

    #[tokio::test]
    async fn spatial_segment_delete_nonexistent_is_noop() {
        let s = make_storage().await;
        s.delete_spatial_segment("ghost", "geom").await.unwrap();
    }

    #[tokio::test]
    async fn spatial_segment_replace_updates_content() {
        let s = make_storage().await;
        s.write_spatial_segment("col", "field", b"original")
            .await
            .unwrap();
        s.write_spatial_segment("col", "field", b"updated")
            .await
            .unwrap();

        let got = s
            .open_spatial_segment("col", "field")
            .await
            .unwrap()
            .expect("segment must exist");
        assert_eq!(got.as_ref(), b"updated", "second write must replace first");
    }

    #[tokio::test]
    async fn spatial_segment_roundtrip_large_payload() {
        let s = make_storage().await;
        // Payload large enough to span multiple pagedb pages.
        let payload: Vec<u8> = (0u8..=255).cycle().take(128 * 1024).collect();
        s.write_spatial_segment("large_col", "geom", &payload)
            .await
            .unwrap();
        let got = s
            .open_spatial_segment("large_col", "geom")
            .await
            .unwrap()
            .expect("segment must exist");
        assert_eq!(got.len(), payload.len());
        assert_eq!(got.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn spatial_segment_multiple_fields_same_collection() {
        let s = make_storage().await;
        let payload_a: Vec<u8> = vec![1u8; 100];
        let payload_b: Vec<u8> = vec![2u8; 200];

        s.write_spatial_segment("places", "point", &payload_a)
            .await
            .unwrap();
        s.write_spatial_segment("places", "polygon", &payload_b)
            .await
            .unwrap();

        let got_a = s
            .open_spatial_segment("places", "point")
            .await
            .unwrap()
            .expect("point segment must exist");
        let got_b = s
            .open_spatial_segment("places", "polygon")
            .await
            .unwrap()
            .expect("polygon segment must exist");

        assert_eq!(got_a.as_ref(), payload_a.as_slice());
        assert_eq!(got_b.as_ref(), payload_b.as_slice());
    }

    #[tokio::test]
    async fn spatial_segment_as_spatial_segment_ext_is_some() {
        let s = make_storage().await;
        assert!(
            s.as_spatial_segment_ext().is_some(),
            "PagedbStorage must return Some from as_spatial_segment_ext"
        );
    }
}
