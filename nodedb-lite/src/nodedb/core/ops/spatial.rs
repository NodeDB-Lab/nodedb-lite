// SPDX-License-Identifier: Apache-2.0

//! Spatial index public API: insert, delete, bbox search, nearest-neighbor.

use crate::nodedb::core::types::NodeDbLite;
use crate::nodedb::lock_ext::LockExt;
use crate::storage::engine::StorageEngine;

impl<S: StorageEngine> NodeDbLite<S> {
    // ── Spatial public API ────────────────────────────────────────────────────

    /// Index a geometry in a collection's spatial index.
    ///
    /// `field` identifies which geometry field is being indexed (allows a
    /// collection to carry multiple spatial fields).  If the document was
    /// previously indexed under the same `(collection, doc_id)`, the old entry
    /// is replaced (upsert semantics).
    pub fn spatial_insert(
        &self,
        collection: &str,
        field: &str,
        doc_id: &str,
        geometry: &nodedb_types::geometry::Geometry,
    ) {
        let mut spatial = self.spatial.lock_or_recover();
        spatial.index_document(collection, field, doc_id, geometry);
        drop(spatial);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.spatial_outbound {
            q.stage_insert(collection, field, doc_id, geometry);
        }
    }

    /// Remove a document's geometry from the spatial index.
    pub fn spatial_delete(&self, collection: &str, field: &str, doc_id: &str) {
        let mut spatial = self.spatial.lock_or_recover();
        spatial.remove_document(collection, field, doc_id);
        drop(spatial);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(q) = &self.spatial_outbound {
            q.stage_delete(collection, field, doc_id);
        }
    }

    /// Bounding-box range search: returns all doc entry IDs whose bbox
    /// intersects the query rectangle.
    pub fn spatial_search_bbox(
        &self,
        collection: &str,
        field: &str,
        query: &nodedb_types::BoundingBox,
    ) -> Vec<nodedb_spatial::rtree::RTreeEntry> {
        let spatial = self.spatial.lock_or_recover();
        spatial
            .search(collection, field, query)
            .into_iter()
            .cloned()
            .collect()
    }

    /// Nearest-neighbor search: returns the `k` closest spatial entries to
    /// the given `(lng, lat)` point.
    pub fn spatial_nearest(
        &self,
        collection: &str,
        field: &str,
        lng: f64,
        lat: f64,
        k: usize,
    ) -> Vec<nodedb_spatial::rtree::NnResult> {
        let spatial = self.spatial.lock_or_recover();
        spatial.nearest(collection, field, lng, lat, k)
    }
}
