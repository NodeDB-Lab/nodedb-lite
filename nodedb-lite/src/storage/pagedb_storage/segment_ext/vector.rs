// SPDX-License-Identifier: Apache-2.0

//! `VectorSegmentExt` over pagedb segments.

use async_trait::async_trait;
use pagedb::PagedbError;
use pagedb::vfs::Vfs;

use crate::error::LiteError;
use crate::storage::pagedb_storage::types::PagedbStorage;

#[async_trait]
impl<V: Vfs + Clone + Send + Sync + 'static> crate::storage::vector_segment_ext::VectorSegmentExt
    for PagedbStorage<V>
where
    <V as Vfs>::LockHandle: Sync,
    <V as Vfs>::File: Sync,
{
    async fn write_vector_segment(
        &self,
        collection_name: &str,
        dim: usize,
        vectors: &[Vec<f32>],
        surrogate_ids: &[u64],
    ) -> Result<(), LiteError> {
        use crate::engine::vector::pagedb_backing::build_ndvs_bytes;
        use pagedb::{RealmId, SegmentKind};

        let ndvs = build_ndvs_bytes(dim, vectors, surrogate_ids)?;

        // Chunk the NDVS bytes into page-sized pieces.
        // Page body capacity = 4096 - ENVELOPE_OVERHEAD (40).
        let chunks: Vec<&[u8]> = ndvs.chunks(self.page_body_capacity()).collect();

        let realm = RealmId::new([0u8; 16]);
        let segment_name = format!("vec/hnsw/{collection_name}");

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

        // Replace if already linked (atomic swap).
        let already_exists = txn.link_segment(&segment_name, &meta).await;
        match already_exists {
            Ok(()) => {}
            Err(PagedbError::AlreadyLinked) => {
                // Use replace_segment to atomically swap old → new.
                txn.replace_segment(&segment_name, &meta)
                    .await
                    .map_err(LiteError::from)?;
            }
            Err(e) => return Err(LiteError::from(e)),
        }

        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }

    async fn open_vector_segment(
        &self,
        collection_name: &str,
    ) -> Result<Option<crate::engine::vector::pagedb_backing::PagedbBacking>, LiteError> {
        use crate::engine::vector::pagedb_backing::PagedbBacking;

        let segment_name = format!("vec/hnsw/{collection_name}");
        let txn = self.db.begin_read().await.map_err(LiteError::from)?;

        let reader = match txn.open_segment(&segment_name).await {
            Ok(r) => r,
            Err(PagedbError::NotFound) => return Ok(None),
            Err(e) => return Err(LiteError::from(e)),
        };

        let backing = PagedbBacking::open(reader).await?;
        Ok(Some(backing))
    }

    async fn delete_vector_segment(&self, collection_name: &str) -> Result<(), LiteError> {
        let segment_name = format!("vec/hnsw/{collection_name}");
        let mut txn = self.db.begin_write().await.map_err(LiteError::from)?;
        match txn.unlink_segment(&segment_name).await {
            Ok(()) => {}
            Err(PagedbError::NotLinked) => return Ok(()), // already gone
            Err(e) => return Err(LiteError::from(e)),
        }
        txn.commit().await.map(|_| ()).map_err(LiteError::from)
    }
}
