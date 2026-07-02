// SPDX-License-Identifier: Apache-2.0

//! `ColumnarSegmentExt` implementation for `PagedbStorage<V>`.
//!
//! One pagedb segment per `(collection, segment_id)` pair, storing the
//! compressed columnar bytes produced by `nodedb_columnar::SegmentWriter`.
//! Segment name: `col/seg/{collection}/{segment_id}`.
//!
//! ## Key split
//!
//! | Data | Storage |
//! |------|---------|
//! | `{collection}:seg:{id}` bytes | pagedb segment (`col/seg/{collection}/{id}`) |
//! | `{collection}:del:{id}` delete bitmap | B+ tree (`Namespace::Columnar`) |
//! | `{collection}:meta` segment list | B+ tree (`Namespace::Columnar`) |
//! | Schema in `Namespace::Meta` | B+ tree |
//!
//! Segment metadata already carries the full ordered list of segment IDs, so
//! no auxiliary sentinel index is needed (unlike the FTS path).
//!
//! ## Page envelope
//!
//! Uses the same 8-byte little-endian length-prefix as the FTS and array
//! segment impls to survive pagedb's page-boundary zero-padding.  The bytes
//! the columnar engine sees are pristine `nodedb_columnar` NDBS bytes —
//! format-bit-identical to what Origin writes to plain NDBS files.

#[cfg(not(target_arch = "wasm32"))]
use async_trait::async_trait;
#[cfg(not(target_arch = "wasm32"))]
use pagedb::vfs::traits::Vfs;
#[cfg(not(target_arch = "wasm32"))]
use pagedb::{RealmId, SegmentKind};

#[cfg(not(target_arch = "wasm32"))]
use crate::error::LiteError;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::columnar_segment_ext::ColumnarSegmentExt;
#[cfg(not(target_arch = "wasm32"))]
use crate::storage::pagedb_storage::PagedbStorage;

/// pagedb page body capacity in bytes: 4096 - 40 bytes AEAD/header envelope.
#[cfg(not(target_arch = "wasm32"))]
const PAGE_BODY_CAP: usize = 4096 - 40;

/// pagedb segment name prefix for columnar segments.
#[cfg(not(target_arch = "wasm32"))]
const COL_SEG_PREFIX: &str = "col/seg/";

#[cfg(not(target_arch = "wasm32"))]
#[async_trait]
impl<V: Vfs + Clone + Send + Sync + 'static> ColumnarSegmentExt for PagedbStorage<V>
where
    <V as Vfs>::LockHandle: Sync,
    <V as Vfs>::File: Sync,
{
    async fn write_columnar_segment(
        &self,
        collection: &str,
        segment_id: u32,
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
        let segment_name = format!("{COL_SEG_PREFIX}{collection}/{segment_id}");

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

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn open_columnar_segment(
        &self,
        collection: &str,
        segment_id: u32,
    ) -> Result<Option<Box<[u8]>>, LiteError> {
        let segment_name = format!("{COL_SEG_PREFIX}{collection}/{segment_id}");
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
                    "columnar segment '{collection}/{segment_id}' page_count={meta_page_count} \
                     too small (index_pages={index_pages})"
                ),
            })?;

        if data_page_count == 0 {
            return Ok(Some(Box::default()));
        }

        let count_u32 = u32::try_from(data_page_count).map_err(|_| LiteError::Storage {
            detail: format!(
                "columnar segment '{collection}/{segment_id}' has too many data pages: \
                 {data_page_count}"
            ),
        })?;

        let pages = reader
            .read_range(1, count_u32)
            .await
            .map_err(|e| LiteError::Storage {
                detail: format!(
                    "pagedb columnar segment read_range failed for \
                     '{collection}/{segment_id}': {e}"
                ),
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
                    "columnar segment '{collection}/{segment_id}' too small to contain \
                     length prefix: {} bytes",
                    flat.len()
                ),
            });
        }
        let byte_len = u64::from_le_bytes(flat[..8].try_into().expect("8-byte slice")) as usize;
        let end = 8_usize
            .checked_add(byte_len)
            .ok_or_else(|| LiteError::Storage {
                detail: format!(
                    "columnar segment '{collection}/{segment_id}' length prefix overflows: \
                 {byte_len}"
                ),
            })?;
        if end > flat.len() {
            return Err(LiteError::Storage {
                detail: format!(
                    "columnar segment '{collection}/{segment_id}' declared byte_len={byte_len} \
                     exceeds available data ({} bytes after prefix)",
                    flat.len() - 8
                ),
            });
        }

        Ok(Some(flat[8..end].to_vec().into_boxed_slice()))
    }

    async fn delete_columnar_segment(
        &self,
        collection: &str,
        segment_id: u32,
    ) -> Result<(), LiteError> {
        let segment_name = format!("{COL_SEG_PREFIX}{collection}/{segment_id}");
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        match txn.unlink_segment(&segment_name).await {
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
    use crate::storage::columnar_segment_ext::ColumnarSegmentExt;
    use crate::storage::engine::StorageEngine;
    use crate::storage::pagedb_storage::PagedbStorage;
    use pagedb::vfs::memory::MemVfs;

    async fn make_storage() -> PagedbStorage<MemVfs> {
        PagedbStorage::open_in_memory().await.unwrap()
    }

    #[tokio::test]
    async fn columnar_segment_as_columnar_segment_ext_is_some() {
        let s = make_storage().await;
        assert!(
            s.as_columnar_segment_ext().is_some(),
            "PagedbStorage must return Some from as_columnar_segment_ext"
        );
    }

    #[tokio::test]
    async fn columnar_segment_roundtrip() {
        let s = make_storage().await;
        let payload: Vec<u8> = (0u8..=255).cycle().take(5000).collect();

        s.write_columnar_segment("metrics", 1, &payload)
            .await
            .unwrap();

        let got = s
            .open_columnar_segment("metrics", 1)
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
    async fn columnar_segment_open_missing_returns_none() {
        let s = make_storage().await;
        let result = s.open_columnar_segment("nonexistent", 99).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn columnar_segment_delete_removes_segment() {
        let s = make_storage().await;
        s.write_columnar_segment("events", 1, b"some column data")
            .await
            .unwrap();
        s.delete_columnar_segment("events", 1).await.unwrap();

        let result = s.open_columnar_segment("events", 1).await.unwrap();
        assert!(result.is_none(), "segment must be gone after delete");
    }

    #[tokio::test]
    async fn columnar_segment_delete_nonexistent_is_noop() {
        let s = make_storage().await;
        s.delete_columnar_segment("ghost", 42).await.unwrap();
    }

    #[tokio::test]
    async fn columnar_segment_replace_updates_content() {
        let s = make_storage().await;
        s.write_columnar_segment("readings", 1, b"original data")
            .await
            .unwrap();
        s.write_columnar_segment("readings", 1, b"updated data")
            .await
            .unwrap();

        let got = s
            .open_columnar_segment("readings", 1)
            .await
            .unwrap()
            .expect("segment must exist");
        assert_eq!(
            got.as_ref(),
            b"updated data",
            "second write must replace first"
        );
    }

    #[tokio::test]
    async fn columnar_segment_roundtrip_large_payload() {
        let s = make_storage().await;
        // Payload large enough to span multiple pagedb pages.
        let payload: Vec<u8> = (0u8..=255).cycle().take(128 * 1024).collect();
        s.write_columnar_segment("large", 5, &payload)
            .await
            .unwrap();
        let got = s
            .open_columnar_segment("large", 5)
            .await
            .unwrap()
            .expect("segment must exist");
        assert_eq!(got.len(), payload.len());
        assert_eq!(got.as_ref(), payload.as_slice());
    }

    #[tokio::test]
    async fn columnar_segment_multiple_collections_isolated() {
        let s = make_storage().await;
        s.write_columnar_segment("col_a", 1, b"data for col_a seg 1")
            .await
            .unwrap();
        s.write_columnar_segment("col_b", 1, b"data for col_b seg 1")
            .await
            .unwrap();
        s.write_columnar_segment("col_a", 2, b"data for col_a seg 2")
            .await
            .unwrap();

        let a1 = s
            .open_columnar_segment("col_a", 1)
            .await
            .unwrap()
            .expect("col_a/1 must exist");
        let b1 = s
            .open_columnar_segment("col_b", 1)
            .await
            .unwrap()
            .expect("col_b/1 must exist");
        let a2 = s
            .open_columnar_segment("col_a", 2)
            .await
            .unwrap()
            .expect("col_a/2 must exist");

        assert_eq!(a1.as_ref(), b"data for col_a seg 1");
        assert_eq!(b1.as_ref(), b"data for col_b seg 1");
        assert_eq!(a2.as_ref(), b"data for col_a seg 2");
    }
}
