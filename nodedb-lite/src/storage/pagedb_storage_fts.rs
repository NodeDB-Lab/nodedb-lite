// SPDX-License-Identifier: Apache-2.0

//! `FtsSegmentExt` implementation for `PagedbStorage<V>`.
//!
//! One pagedb segment per FTS index key, storing all term postings for that
//! index as a single blob.  Segment name: `fts/seg/{index_key}`.
//!
//! ## Segment index (B+ tree)
//!
//! pagedb's `ReadTxn::list_segments` returns `SegmentMeta` structs but does
//! not expose the segment names.  Rather than adding a gap to pagedb, we
//! maintain a small auxiliary index in the B+ tree under `Namespace::Fts`:
//!
//! - Key: `fts:_seg_idx:{index_key}` → empty value (presence = segment exists)
//!
//! `write_fts_segment` inserts this sentinel; `delete_fts_segment` removes it.
//! `list_fts_segments(prefix)` scans `Namespace::Fts` with key prefix
//! `fts:_seg_idx:{prefix}` and strips the leading `fts:_seg_idx:` tag.
//!
//! ## Page envelope
//!
//! Uses the same 8-byte little-endian length-prefix as the array segment impl
//! to survive pagedb's page-boundary zero-padding.

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
use crate::storage::engine::StorageEngine;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::fts_segment_ext::FtsSegmentExt;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::pagedb_storage::PagedbStorage;

/// pagedb page body capacity in bytes: 4096 - 40 bytes AEAD/header envelope.
#[cfg(not(target_arch = "wasm32"))]
const PAGE_BODY_CAP: usize = 4096 - 40;

/// pagedb segment name prefix for FTS posting segments.
#[cfg(not(target_arch = "wasm32"))]
const FTS_SEG_PREFIX: &str = "fts/seg/";

/// B+ tree key prefix for the auxiliary segment index sentinel keys.
#[cfg(not(target_arch = "wasm32"))]
const FTS_SEG_IDX_PREFIX: &str = "fts:_seg_idx:";

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl<V: Vfs + Clone + Send + Sync + 'static> FtsSegmentExt for PagedbStorage<V>
where
    <V as Vfs>::LockHandle: Sync,
    <V as Vfs>::File: Sync,
{
    async fn write_fts_segment(&self, index_key: &str, bytes: &[u8]) -> Result<(), LiteError> {
        // Prepend an 8-byte little-endian length so reads can recover the
        // exact byte count after pagedb pads the last page to a full page size.
        let byte_len = bytes.len() as u64;
        let mut payload = Vec::with_capacity(8 + bytes.len());
        payload.extend_from_slice(&byte_len.to_le_bytes());
        payload.extend_from_slice(bytes);

        let chunks: Vec<&[u8]> = payload.chunks(PAGE_BODY_CAP).collect();

        let realm = RealmId::new([0u8; 16]);
        let segment_name = format!("{FTS_SEG_PREFIX}{index_key}");

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
            Err(pagedb::errors::PagedbError::AlreadyLinked) => {
                txn.replace_segment(&segment_name, &meta)
                    .await
                    .map_err(LiteError::from)?;
            }
            Err(e) => return Err(LiteError::from(e)),
        }

        // Write the auxiliary segment-index sentinel to the B+ tree so that
        // list_fts_segments can enumerate index keys without pagedb name enumeration.
        let sentinel_key = format!("{FTS_SEG_IDX_PREFIX}{index_key}");
        txn.put(
            &crate::storage::pagedb_storage::prefix_key(Namespace::Fts, sentinel_key.as_bytes()),
            &[],
        )
        .await
        .map_err(LiteError::from)?;

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn open_fts_segment(&self, index_key: &str) -> Result<Option<Box<[u8]>>, LiteError> {
        let segment_name = format!("{FTS_SEG_PREFIX}{index_key}");
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
                    "fts segment '{index_key}' page_count={meta_page_count} too small \
                     (index_pages={index_pages})"
                ),
            })?;

        if data_page_count == 0 {
            return Ok(Some(Box::default()));
        }

        let count_u32 = u32::try_from(data_page_count).map_err(|_| LiteError::Storage {
            detail: format!("fts segment '{index_key}' has too many data pages: {data_page_count}"),
        })?;

        let pages = reader
            .read_range(1, count_u32)
            .await
            .map_err(|e| LiteError::Storage {
                detail: format!("pagedb fts segment read_range failed for '{index_key}': {e}"),
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
                    "fts segment '{index_key}' too small to contain length prefix: {} bytes",
                    flat.len()
                ),
            });
        }
        let byte_len = u64::from_le_bytes(flat[..8].try_into().expect("8-byte slice")) as usize;
        let end = 8_usize
            .checked_add(byte_len)
            .ok_or_else(|| LiteError::Storage {
                detail: format!("fts segment '{index_key}' length prefix overflows: {byte_len}"),
            })?;
        if end > flat.len() {
            return Err(LiteError::Storage {
                detail: format!(
                    "fts segment '{index_key}' declared byte_len={byte_len} exceeds \
                     available data ({} bytes after prefix)",
                    flat.len() - 8
                ),
            });
        }

        Ok(Some(flat[8..end].to_vec().into_boxed_slice()))
    }

    async fn delete_fts_segment(&self, index_key: &str) -> Result<(), LiteError> {
        let segment_name = format!("{FTS_SEG_PREFIX}{index_key}");
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        match txn.unlink_segment(&segment_name).await {
            Ok(()) => {}
            Err(pagedb::errors::PagedbError::NotLinked) => {}
            Err(e) => return Err(LiteError::from(e)),
        }

        // Remove the auxiliary segment-index sentinel from the B+ tree.
        let sentinel_key = format!("{FTS_SEG_IDX_PREFIX}{index_key}");
        txn.delete(&crate::storage::pagedb_storage::prefix_key(
            Namespace::Fts,
            sentinel_key.as_bytes(),
        ))
        .await
        .map_err(LiteError::from)?;

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn list_fts_segments(&self, prefix: &str) -> Result<Vec<String>, LiteError> {
        // Scan the auxiliary B+ tree index for sentinel keys that match the prefix.
        let scan_prefix = format!("{FTS_SEG_IDX_PREFIX}{prefix}");
        let pairs = self
            .scan_prefix(Namespace::Fts, scan_prefix.as_bytes())
            .await?;
        // Strip the sentinel prefix to recover bare index_key strings.
        let keys = pairs
            .into_iter()
            .filter_map(|(k, _)| {
                let s = String::from_utf8(k).ok()?;
                s.strip_prefix(FTS_SEG_IDX_PREFIX)
                    .map(|rest| rest.to_owned())
            })
            .collect();
        Ok(keys)
    }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use crate::storage::engine::StorageEngine;
    use crate::storage::fts_segment_ext::FtsSegmentExt;
    use crate::storage::pagedb_storage::PagedbStorage;
    use pagedb::vfs::memory::MemVfs;

    async fn make_storage() -> PagedbStorage<MemVfs> {
        PagedbStorage::open_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn fts_segment_roundtrip() {
        let s = make_storage().await;
        let payload: Vec<u8> = (0u8..=255).cycle().take(5000).collect();

        s.write_fts_segment("articles:_doc", &payload)
            .await
            .unwrap();

        let got = s
            .open_fts_segment("articles:_doc")
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
    async fn fts_segment_open_missing_returns_none() {
        let s = make_storage().await;
        let result = s.open_fts_segment("nonexistent:_doc").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn fts_segment_delete_removes_segment() {
        let s = make_storage().await;
        s.write_fts_segment("col:field", b"some posting data")
            .await
            .unwrap();
        s.delete_fts_segment("col:field").await.unwrap();

        let result = s.open_fts_segment("col:field").await.unwrap();
        assert!(result.is_none(), "segment must be gone after delete");
    }

    #[tokio::test]
    async fn fts_segment_delete_nonexistent_is_noop() {
        let s = make_storage().await;
        s.delete_fts_segment("ghost:_doc").await.unwrap();
    }

    #[tokio::test]
    async fn fts_segment_replace_updates_content() {
        let s = make_storage().await;
        s.write_fts_segment("col:_doc", b"original").await.unwrap();
        s.write_fts_segment("col:_doc", b"updated").await.unwrap();

        let got = s
            .open_fts_segment("col:_doc")
            .await
            .unwrap()
            .expect("segment must exist");
        assert_eq!(got.as_ref(), b"updated", "second write must replace first");
    }

    #[tokio::test]
    async fn fts_segment_list_by_prefix() {
        let s = make_storage().await;
        s.write_fts_segment("news:_doc", b"a").await.unwrap();
        s.write_fts_segment("news:title", b"b").await.unwrap();
        s.write_fts_segment("blog:_doc", b"c").await.unwrap();

        let mut news_segs = s.list_fts_segments("news:").await.unwrap();
        news_segs.sort();
        assert_eq!(news_segs.len(), 2, "expected 2 news segments");
        assert!(news_segs.contains(&"news:_doc".to_owned()));
        assert!(news_segs.contains(&"news:title".to_owned()));

        let blog_segs = s.list_fts_segments("blog:").await.unwrap();
        assert_eq!(blog_segs.len(), 1);
        assert_eq!(blog_segs[0], "blog:_doc");
    }

    #[tokio::test]
    async fn fts_segment_list_empty_prefix_returns_all() {
        let s = make_storage().await;
        s.write_fts_segment("a:_doc", b"x").await.unwrap();
        s.write_fts_segment("b:_doc", b"y").await.unwrap();

        let all = s.list_fts_segments("").await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn fts_segment_list_after_delete_shrinks() {
        let s = make_storage().await;
        s.write_fts_segment("col:_doc", b"a").await.unwrap();
        s.write_fts_segment("col:field", b"b").await.unwrap();

        s.delete_fts_segment("col:_doc").await.unwrap();

        let remaining = s.list_fts_segments("col:").await.unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0], "col:field");
    }

    #[tokio::test]
    async fn fts_segment_as_fts_segment_ext_is_some() {
        let s = make_storage().await;
        assert!(
            s.as_fts_segment_ext().is_some(),
            "PagedbStorage must return Some from as_fts_segment_ext"
        );
    }

    #[tokio::test]
    async fn fts_segment_roundtrip_large_payload() {
        let s = make_storage().await;
        // Payload large enough to span multiple pagedb pages.
        let payload: Vec<u8> = (0u8..=255).cycle().take(128 * 1024).collect();
        s.write_fts_segment("large:_doc", &payload).await.unwrap();
        let got = s
            .open_fts_segment("large:_doc")
            .await
            .unwrap()
            .expect("segment must exist");
        assert_eq!(got.len(), payload.len());
        assert_eq!(got.as_ref(), payload.as_slice());
    }
}
