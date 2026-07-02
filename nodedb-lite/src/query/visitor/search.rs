// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for search-shaped SqlPlan variants:
//! MultiVectorSearch, HybridSearch, HybridSearchTriple, SpatialScan.

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::VectorOp;
use nodedb_physical::physical_plan::spatial::SpatialPredicate as PhysSpatialPredicate;
use nodedb_physical::physical_plan::{SpatialOp, TextOp};
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::query::Projection;
use nodedb_sql::types::query::SpatialPredicate as SqlSpatialPredicate;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::filter_convert::sql_filters_to_metadata;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::storage::engine::StorageEngine;

use super::adapter::LiteFut;

fn encode_attribute_filters(filters: &[Filter]) -> Result<Vec<u8>, LiteError> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    // Complex QExpr predicates are evaluated post-scan; only primitive conditions
    // are pushed to the physical visitor via serialized MetadataFilter.
    match sql_filters_to_metadata(filters, &[])?.meta {
        None => Ok(Vec::new()),
        Some(mf) => zerompk::to_msgpack_vec(&mf).map_err(|e| LiteError::Serialization {
            detail: format!("encode attribute filters: {e}"),
        }),
    }
}

fn map_spatial_predicate(p: &SqlSpatialPredicate) -> PhysSpatialPredicate {
    match p {
        SqlSpatialPredicate::DWithin => PhysSpatialPredicate::DWithin,
        SqlSpatialPredicate::Contains => PhysSpatialPredicate::Contains,
        SqlSpatialPredicate::Intersects => PhysSpatialPredicate::Intersects,
        SqlSpatialPredicate::Within => PhysSpatialPredicate::Within,
    }
}

// ── MultiVectorSearch ────────────────────────────────────────────────────────

/// Lower `SqlPlan::MultiVectorSearch` to `VectorOp::MultiSearch`.
///
/// Lite has no multi-field HNSW RRF fusion path: all named fields on Lite
/// share a single in-memory HNSW index keyed by `collection` (or
/// `collection:field`). Multi-vector search would require per-field indexes
/// and a merge step that is absent from the Lite vector state. This is an
/// Origin-only feature. The closure rule requires `unreachable!` where the
/// deployment shape makes execution structurally impossible; Lite's single
/// shared HNSW index is exactly that mismatch — callers targeting a
/// vector-primary collection with multiple embedding fields must route to
/// Origin.
pub(super) fn lower_multi_vector_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    query_vector: &[f32],
    top_k: usize,
    ef_search: usize,
) -> Result<LiteFut<'a>, LiteError> {
    let op = VectorOp::MultiSearch {
        collection: collection.to_string(),
        query_vector: query_vector.to_vec(),
        top_k,
        ef_search,
        filter_bitmap: None,
        rls_filters: Vec::new(),
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.vector(&op)?;
    Ok(Box::pin(fut))
}

// ── HybridSearch ─────────────────────────────────────────────────────────────

/// Lower `SqlPlan::HybridSearch` to `TextOp::HybridSearch`.
#[allow(clippy::too_many_arguments)]
pub(super) fn lower_hybrid_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    query_vector: &[f32],
    query_text: &str,
    top_k: usize,
    ef_search: usize,
    vector_weight: f32,
    fuzzy: bool,
    score_alias: Option<&str>,
) -> Result<LiteFut<'a>, LiteError> {
    let op = TextOp::HybridSearch {
        collection: collection.to_string(),
        query_vector: query_vector.to_vec(),
        query_text: query_text.to_string(),
        top_k,
        ef_search,
        fuzzy,
        vector_weight,
        filter_bitmap: None,
        rls_filters: Vec::new(),
        score_alias: score_alias.map(|s| s.to_string()),
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.text(&op)?;
    Ok(Box::pin(fut))
}

// ── HybridSearchTriple ────────────────────────────────────────────────────────

/// Lower `SqlPlan::HybridSearchTriple` to `TextOp::HybridSearchTriple`.
#[allow(clippy::too_many_arguments)]
pub(super) fn lower_hybrid_search_triple<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    query_vector: &[f32],
    query_text: &str,
    graph_seed_id: &str,
    graph_depth: usize,
    graph_edge_label: Option<&str>,
    top_k: usize,
    ef_search: usize,
    fuzzy: bool,
    rrf_k: (f64, f64, f64),
    score_alias: Option<&str>,
) -> Result<LiteFut<'a>, LiteError> {
    let op = TextOp::HybridSearchTriple {
        collection: collection.to_string(),
        query_vector: query_vector.to_vec(),
        query_text: query_text.to_string(),
        graph_seed_id: graph_seed_id.to_string(),
        graph_depth,
        graph_edge_label: graph_edge_label.map(|s| s.to_string()),
        top_k,
        ef_search,
        fuzzy,
        rrf_k,
        filter_bitmap: None,
        rls_filters: Vec::new(),
        score_alias: score_alias.map(|s| s.to_string()),
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.text(&op)?;
    Ok(Box::pin(fut))
}

// ── SpatialScan ───────────────────────────────────────────────────────────────

/// Lower `SqlPlan::SpatialScan` to `SpatialOp::Scan`.
#[allow(clippy::too_many_arguments)]
pub(super) fn lower_spatial_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    predicate: &SqlSpatialPredicate,
    query_geometry: &nodedb_types::geometry::Geometry,
    distance_meters: f64,
    attribute_filters: &[Filter],
    limit: usize,
    projection: &[Projection],
) -> Result<LiteFut<'a>, LiteError> {
    let attr_bytes = encode_attribute_filters(attribute_filters)?;
    let proj_cols: Vec<String> = projection
        .iter()
        .filter_map(|p| match p {
            Projection::Column(name) => Some(name.clone()),
            Projection::Computed { alias, .. } => Some(alias.clone()),
            _ => None,
        })
        .collect();

    let op = SpatialOp::Scan {
        collection: collection.to_string(),
        field: field.to_string(),
        predicate: map_spatial_predicate(predicate),
        query_geometry: query_geometry.clone(),
        distance_meters,
        attribute_filters: attr_bytes,
        limit,
        projection: proj_cols,
        rls_filters: Vec::new(),
        prefilter: None,
    };
    let mut phys = LiteDataPlaneVisitor { engine };
    let fut = phys.spatial(&op)?;
    Ok(Box::pin(fut))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_sql::types::query::SpatialPredicate as SqlSpatialPredicate;
    use nodedb_types::geometry::Geometry;

    use crate::PagedbStorageMem;
    use crate::query::engine::LiteQueryEngine;

    async fn make_engine() -> LiteQueryEngine<PagedbStorageMem> {
        use std::sync::Mutex;
        let storage = Arc::new(
            PagedbStorageMem::open_in_memory()
                .await
                .expect("in-memory pagedb"),
        );
        let crdt = Arc::new(Mutex::new(
            crate::engine::crdt::CrdtEngine::new(1).expect("crdt"),
        ));
        let strict = Arc::new(crate::engine::strict::StrictEngine::new(Arc::clone(
            &storage,
        )));
        let columnar = Arc::new(crate::engine::columnar::ColumnarEngine::new(Arc::clone(
            &storage,
        )));
        let htap = Arc::new(crate::engine::htap::HtapBridge::new());
        let timeseries = Arc::new(Mutex::new(
            crate::engine::timeseries::engine::TimeseriesEngine::new(),
        ));
        let vector_state = Arc::new(crate::engine::vector::VectorState::new(
            Arc::clone(&storage),
            100,
        ));
        let array_state = Arc::new(tokio::sync::Mutex::new(
            crate::engine::array::engine::ArrayEngineState::open(&storage)
                .await
                .expect("array"),
        ));
        let fts_state = Arc::new(crate::engine::fts::FtsState::new());
        let spatial = Arc::new(Mutex::new(
            crate::engine::spatial::SpatialIndexManager::new(),
        ));
        LiteQueryEngine::new(
            crdt,
            strict,
            columnar,
            htap,
            storage,
            timeseries,
            vector_state,
            array_state,
            fts_state,
            spatial,
            Arc::new(Mutex::new(std::collections::HashMap::new())),
        )
    }

    #[tokio::test]
    async fn test_hybrid_search_returns_result() {
        let engine = make_engine().await;
        let result = super::lower_hybrid_search(
            &engine,
            "test_col",
            &[0.1f32, 0.2, 0.3],
            "hello world",
            5,
            50,
            0.5,
            false,
            None,
        );
        // Collection doesn't exist; expect a result (possibly empty or error from engine).
        // The lowering itself must not panic or return a LiteError at plan time.
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_spatial_scan_returns_result() {
        let engine = make_engine().await;
        let point = Geometry::point(0.0, 0.0);
        let result = super::lower_spatial_scan(
            &engine,
            "geo_col",
            "location",
            &SqlSpatialPredicate::DWithin,
            &point,
            1000.0,
            &[],
            10,
            &[],
        );
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_hybrid_search_triple_returns_result() {
        let engine = make_engine().await;
        let result = super::lower_hybrid_search_triple(
            &engine,
            "test_col",
            &[0.1f32, 0.2],
            "some query",
            "node-1",
            2,
            None,
            5,
            40,
            false,
            (60.0, 60.0, 60.0),
            Some("score"),
        );
        assert!(result.is_ok());
    }
}
