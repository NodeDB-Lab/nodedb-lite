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
use nodedb_sql::{HybridSearchTripleVisitArgs, HybridSearchVisitArgs};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::filter_convert::sql_filters_to_metadata;
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::scan_filter_convert::encode_scan_filters;
use crate::storage::engine::StorageEngine;

use super::adapter::LiteFut;
use super::projection::projection_names;

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
/// Lite rejects multi-field vector fusion because its executor lacks the merge path.
pub(super) fn lower_multi_vector_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    query_vector: &[f32],
    top_k: usize,
    ef_search: usize,
) -> Result<LiteFut<'a>, LiteError> {
    let op = VectorOp::MultiSearch {
        // Lite holds a bare collection name; DatabaseId::DEFAULT keeps it unqualified.
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            collection,
        ),
        query_vector: query_vector.to_vec(),
        top_k,
        ef_search,
        filter_bitmap: None,
        rls_filters: Vec::new(),
    };
    let mut phys = LiteDataPlaneVisitor::new(engine);
    let fut = phys.vector(&op)?;
    Ok(Box::pin(fut))
}

// ── SparseSearch ─────────────────────────────────────────────────────────────

/// Lower `SqlPlan::SparseSearch` to `VectorOp::SparseSearch`.
///
/// Scoring is a dot product over the sparse inverted index; results come back
/// as `["id", "score"]` ordered by score descending.
pub(super) fn lower_sparse_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    query_entries: &[(u32, f32)],
    top_k: usize,
) -> Result<LiteFut<'a>, LiteError> {
    let op = VectorOp::SparseSearch {
        // Lite holds a bare collection name; DatabaseId::DEFAULT keeps it unqualified.
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            collection,
        ),
        field_name: field.to_string(),
        query_entries: query_entries.to_vec(),
        top_k,
    };
    let mut phys = LiteDataPlaneVisitor::new(engine);
    let fut = phys.vector(&op)?;
    Ok(Box::pin(fut))
}

// ── HybridSearch ─────────────────────────────────────────────────────────────

/// Lower `SqlPlan::HybridSearch` to `TextOp::HybridSearch`.
pub(super) fn lower_hybrid_search<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: HybridSearchVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let HybridSearchVisitArgs {
        collection,
        vector_field,
        query_vector,
        text_field,
        query_text,
        filters,
        top_k,
        ef_search,
        vector_weight,
        mode,
        fuzzy,
        score_alias,
    } = args;
    let op = TextOp::HybridSearch {
        // Lite holds a bare collection name; DatabaseId::DEFAULT keeps it unqualified.
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            collection,
        ),
        vector_field: vector_field.to_string(),
        query_vector: query_vector.to_vec(),
        text_field: text_field.map(str::to_string),
        query_text: query_text.to_string(),
        filters: encode_scan_filters(filters)?,
        top_k,
        ef_search,
        mode,
        fuzzy,
        vector_weight,
        filter_bitmap: None,
        rls_filters: Vec::new(),
        score_alias: score_alias.map(str::to_string),
    };
    let mut phys = LiteDataPlaneVisitor::new(engine);
    let fut = phys.text(&op)?;
    Ok(Box::pin(fut))
}

// ── HybridSearchTriple ────────────────────────────────────────────────────────

/// Lower `SqlPlan::HybridSearchTriple` to `TextOp::HybridSearchTriple`.
pub(super) fn lower_hybrid_search_triple<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: HybridSearchTripleVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let HybridSearchTripleVisitArgs {
        collection,
        vector_field,
        query_vector,
        text_field,
        query_text,
        filters,
        graph_seed_id,
        graph_depth,
        graph_edge_label,
        top_k,
        ef_search,
        mode,
        fuzzy,
        rrf_k,
        score_alias,
    } = args;
    let op = TextOp::HybridSearchTriple {
        // Lite holds a bare collection name; DatabaseId::DEFAULT keeps it unqualified.
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            collection,
        ),
        vector_field: vector_field.to_string(),
        query_vector: query_vector.to_vec(),
        text_field: text_field.map(str::to_string),
        query_text: query_text.to_string(),
        filters: encode_scan_filters(filters)?,
        graph_seed_id: graph_seed_id.to_string(),
        graph_depth,
        graph_edge_label: graph_edge_label.map(str::to_string),
        top_k,
        ef_search,
        mode,
        fuzzy,
        rrf_k,
        filter_bitmap: None,
        rls_filters: Vec::new(),
        score_alias: score_alias.map(str::to_string),
    };
    let mut phys = LiteDataPlaneVisitor::new(engine);
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
    let proj_cols = projection_names(projection, "spatial scan")?;

    let op = SpatialOp::Scan {
        // Lite holds a bare collection name; DatabaseId::DEFAULT keeps it unqualified.
        collection: nodedb_types::QualifiedCollection::new(
            nodedb_types::DatabaseId::DEFAULT,
            collection,
        ),
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
    let mut phys = LiteDataPlaneVisitor::new(engine);
    let fut = phys.spatial(&op)?;
    Ok(Box::pin(fut))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_sql::types::query::SpatialPredicate as SqlSpatialPredicate;
    use nodedb_types::geometry::Geometry;

    use crate::PagedbStorageMem;
    use crate::query::engine::{LiteQueryEngine, LiteQueryEngineParams};

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
        let governor = crate::query::engine::test_governor();
        let strict = Arc::new(crate::engine::strict::StrictEngine::new(Arc::clone(
            &storage,
        )));
        let columnar = Arc::new(crate::engine::columnar::ColumnarEngine::new(
            Arc::clone(&storage),
            crate::query::engine::test_scoped_memory(&governor, nodedb_mem::EngineId::Columnar),
        ));
        let htap = Arc::new(crate::engine::htap::HtapBridge::new());
        let timeseries = Arc::new(Mutex::new(
            crate::engine::timeseries::engine::TimeseriesEngine::new(),
        ));
        let vector_state = Arc::new(crate::engine::vector::VectorState::new(
            Arc::clone(&storage),
            100,
            crate::query::engine::test_scoped_memory(&governor, nodedb_mem::EngineId::Vector),
        ));
        let array_state = Arc::new(tokio::sync::Mutex::new(
            crate::engine::array::engine::ArrayEngineState::open(&storage)
                .await
                .expect("array"),
        ));
        let fts_state = Arc::new(crate::engine::fts::FtsState::new(Arc::clone(&governor)));
        let spatial = Arc::new(Mutex::new(
            crate::engine::spatial::SpatialIndexManager::new(
                crate::query::engine::test_scoped_memory(&governor, nodedb_mem::EngineId::Spatial),
            ),
        ));
        LiteQueryEngine::new(LiteQueryEngineParams {
            crdt,
            strict,
            columnar,
            htap,
            storage,
            timeseries,
            vector_state,
            array_state,
            fts_state,
            sparse_state: Arc::new(crate::engine::sparse_vector::SparseVectorState::new()),
            spatial,
            csr: Arc::new(Mutex::new(std::collections::HashMap::new())),
            governor,
            kv_local: crate::query::engine::test_kv_local(),
        })
    }

    #[tokio::test]
    async fn test_hybrid_search_returns_result() {
        let engine = make_engine().await;
        let result = super::lower_hybrid_search(
            &engine,
            nodedb_sql::HybridSearchVisitArgs {
                collection: "test_col",
                vector_field: "",
                query_vector: &[0.1f32, 0.2, 0.3],
                text_field: None,
                query_text: "hello world",
                filters: &[],
                top_k: 5,
                ef_search: 50,
                vector_weight: 0.5,
                mode: nodedb_types::text_search::QueryMode::Or,
                fuzzy: false,
                score_alias: None,
            },
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
            nodedb_sql::HybridSearchTripleVisitArgs {
                collection: "test_col",
                vector_field: "",
                query_vector: &[0.1f32, 0.2],
                text_field: None,
                query_text: "some query",
                filters: &[],
                graph_seed_id: "node-1",
                graph_depth: 2,
                graph_edge_label: None,
                top_k: 5,
                ef_search: 40,
                mode: nodedb_types::text_search::QueryMode::Or,
                fuzzy: false,
                rrf_k: (60.0, 60.0, 60.0),
                score_alias: Some("score"),
            },
        );
        assert!(result.is_ok());
    }
}
