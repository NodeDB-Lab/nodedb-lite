// SPDX-License-Identifier: Apache-2.0

//! `ArraySegmentExt` over pagedb segments.

use async_trait::async_trait;
use pagedb::PagedbError;
use pagedb::vfs::Vfs;

use crate::error::LiteError;
use crate::storage::pagedb_storage::types::PagedbStorage;

#[async_trait]
impl<V: Vfs + Clone + Send + Sync + 'static> crate::storage::array_segment_ext::ArraySegmentExt
    for PagedbStorage<V>
where
    <V as Vfs>::LockHandle: Sync,
    <V as Vfs>::File: Sync,
{
    async fn write_array_segment(
        &self,
        array_name: &str,
        seg_id: u64,
        bytes: &[u8],
    ) -> Result<(), LiteError> {
        use pagedb::{RealmId, SegmentKind};

        // Prepend an 8-byte little-endian length so reads can recover the
        // exact byte count after pagedb pads the last page to a full page size.
        let byte_len = bytes.len() as u64;
        let mut payload = Vec::with_capacity(8 + bytes.len());
        payload.extend_from_slice(&byte_len.to_le_bytes());
        payload.extend_from_slice(bytes);

        // Chunk the length-prefixed payload into pagedb page-body-sized pieces.
        // Page body capacity = 4096 - ENVELOPE_OVERHEAD (40).
        const PAGE_BODY_CAP: usize = 4096 - 40;
        let chunks: Vec<&[u8]> = payload.chunks(PAGE_BODY_CAP).collect();

        let realm = RealmId::new([0u8; 16]);
        let segment_name = format!("arr/tile/{array_name}/{seg_id}");

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
            Err(PagedbError::AlreadyLinked) => {
                txn.replace_segment(&segment_name, &meta)
                    .await
                    .map_err(LiteError::from)?;
            }
            Err(e) => return Err(LiteError::from(e)),
        }

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn open_array_segment(
        &self,
        array_name: &str,
        seg_id: u64,
    ) -> Result<Option<Box<[u8]>>, LiteError> {
        let segment_name = format!("arr/tile/{array_name}/{seg_id}");
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;

        let reader = match txn.open_segment(&segment_name).await {
            Ok(r) => r,
            Err(PagedbError::NotFound) => return Ok(None),
            Err(e) => return Err(LiteError::from(e)),
        };

        // Read all data pages and concatenate into a flat byte buffer.
        let meta_page_count = reader.meta().page_count;
        let index_pages = u64::from(reader.index_page_count());
        let data_page_count = meta_page_count
            .checked_sub(2 + index_pages)
            .ok_or_else(|| LiteError::Storage {
                detail: format!(
                    "array segment page_count={meta_page_count} too small \
                     (index_pages={index_pages})"
                ),
            })?;

        if data_page_count == 0 {
            return Ok(Some(Box::default()));
        }

        let count_u32 = u32::try_from(data_page_count).map_err(|_| LiteError::Storage {
            detail: format!("array segment has too many data pages: {data_page_count}"),
        })?;

        let pages = reader
            .read_range(1, count_u32)
            .await
            .map_err(|e| LiteError::Storage {
                detail: format!("pagedb array segment read_range failed: {e}"),
            })?;

        let total: usize = pages.iter().map(|p| p.len()).sum();
        let mut flat = Vec::with_capacity(total);
        for page in pages {
            flat.extend_from_slice(&page);
        }

        // Strip the 8-byte length prefix written by `write_array_segment`.
        if flat.len() < 8 {
            return Err(LiteError::Storage {
                detail: format!(
                    "array segment {array_name}/{seg_id} too small to contain length prefix: \
                     {} bytes",
                    flat.len()
                ),
            });
        }
        let byte_len = u64::from_le_bytes(flat[..8].try_into().expect("8-byte slice")) as usize;
        let end = 8_usize
            .checked_add(byte_len)
            .ok_or_else(|| LiteError::Storage {
                detail: format!(
                    "array segment {array_name}/{seg_id} length prefix overflows: {byte_len}"
                ),
            })?;
        if end > flat.len() {
            return Err(LiteError::Storage {
                detail: format!(
                    "array segment {array_name}/{seg_id} declared byte_len={byte_len} \
                     exceeds available data ({} bytes after prefix)",
                    flat.len() - 8
                ),
            });
        }
        let segment_bytes = flat[8..end].to_vec();

        Ok(Some(segment_bytes.into_boxed_slice()))
    }

    async fn delete_array_segment(&self, array_name: &str, seg_id: u64) -> Result<(), LiteError> {
        let segment_name = format!("arr/tile/{array_name}/{seg_id}");
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        match txn.unlink_segment(&segment_name).await {
            Ok(()) => {}
            Err(PagedbError::NotLinked) => return Ok(()), // already gone
            Err(e) => return Err(LiteError::from(e)),
        }
        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }
}
