// SPDX-License-Identifier: Apache-2.0

//! `LiteVisitor` struct + `PlanVisitor` trait impl. Every method body
//! delegates to a sibling lowering function — adding a new `SqlPlan` variant
//! becomes a hard compile error here, which is the intended forcing function
//! for exhaustive coverage.

use std::future::Future;
use std::pin::Pin;

use nodedb_sql::PlanVisitor;
use nodedb_sql::fts_types::FtsQuery;
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types::SqlValue;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::plan::VectorAnnOptions;
use nodedb_sql::types::query::{EngineType, Projection, SortKey, WindowSpec};
use nodedb_sql::types_expr::{SqlExpr, SqlPayloadAtom};
use nodedb_types::result::QueryResult;
use nodedb_types::vector_distance::DistanceMetric;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::visitor::array::{
    lower_alter_array, lower_array_agg, lower_array_compact, lower_array_elementwise,
    lower_array_flush, lower_array_project, lower_array_slice, lower_create_array,
    lower_delete_array, lower_drop_array, lower_insert_array,
};
use crate::query::visitor::dml::{lower_insert_select, lower_merge, lower_update_from};
use crate::query::visitor::kv::lower_kv_insert;
use crate::query::visitor::lateral::{lower_lateral_loop, lower_lateral_top_k};
use crate::query::visitor::queries::{
    lower_aggregate, lower_cte, lower_document_index_lookup, lower_join, lower_range_scan,
};
use crate::query::visitor::recursive::{lower_recursive_scan, lower_recursive_value};
use crate::query::visitor::search::{
    lower_hybrid_search, lower_hybrid_search_triple, lower_multi_vector_search, lower_spatial_scan,
};
use crate::query::visitor::set_ops::{lower_except, lower_intersect, lower_union};
use crate::query::visitor::timeseries::{lower_timeseries_ingest, lower_timeseries_scan};
use crate::query::visitor::vector_primary::lower_vector_primary_insert;
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::basic::{
    lower_constant_result, lower_create_index, lower_delete, lower_drop_index, lower_insert,
    lower_point_get, lower_scan, lower_truncate, lower_update,
};
use super::text_search::lower_text_search;
use super::vector_search::lower_vector_search;

pub(crate) type LiteFut<'a> =
    Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + Send + 'a>>;

pub(crate) struct LiteVisitor<'a, S: StorageEngine + StorageEngineSync> {
    pub(crate) engine: &'a LiteQueryEngine<S>,
}

impl<'a, S: StorageEngine + StorageEngineSync + 'a> PlanVisitor for LiteVisitor<'a, S> {
    type Output = LiteFut<'a>;
    type Error = LiteError;

    fn constant_result(
        &mut self,
        columns: &[String],
        values: &[SqlValue],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_constant_result(self.engine, columns, values)
    }

    fn scan(
        &mut self,
        collection: &str,
        _alias: Option<&str>,
        engine_type: EngineType,
        filters: &[Filter],
        _projection: &[Projection],
        sort_keys: &[SortKey],
        limit: Option<usize>,
        offset: usize,
        distinct: bool,
        window_functions: &[WindowSpec],
        temporal: &TemporalScope,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_scan(
            self.engine,
            collection,
            engine_type,
            filters,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
            temporal,
        )
    }

    fn point_get(
        &mut self,
        collection: &str,
        _alias: Option<&str>,
        engine_type: EngineType,
        _key_column: &str,
        key_value: &SqlValue,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_point_get(self.engine, collection, engine_type, key_value)
    }

    fn insert(
        &mut self,
        collection: &str,
        engine_type: EngineType,
        rows: &[Vec<(String, SqlValue)>],
        _column_defaults: &[(String, String)],
        if_absent: bool,
        _column_schema: &[(String, String)],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_insert(self.engine, collection, engine_type, rows, if_absent)
    }

    fn upsert(
        &mut self,
        collection: &str,
        engine_type: EngineType,
        rows: &[Vec<(String, SqlValue)>],
        _column_defaults: &[(String, String)],
        _on_conflict_updates: &[(String, SqlExpr)],
        _column_schema: &[(String, String)],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_insert(self.engine, collection, engine_type, rows, true)
    }

    fn update(
        &mut self,
        collection: &str,
        engine_type: EngineType,
        assignments: &[(String, SqlExpr)],
        _filters: &[Filter],
        target_keys: &[SqlValue],
        _returning: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_update(
            self.engine,
            collection,
            engine_type,
            assignments,
            target_keys,
        )
    }

    fn delete(
        &mut self,
        collection: &str,
        engine_type: EngineType,
        _filters: &[Filter],
        target_keys: &[SqlValue],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_delete(self.engine, collection, engine_type, target_keys)
    }

    fn truncate(
        &mut self,
        collection: &str,
        _restart_identity: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_truncate(self.engine, collection)
    }

    fn vector_search(
        &mut self,
        collection: &str,
        field: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
        metric: DistanceMetric,
        filters: &[Filter],
        array_prefilter: Option<&nodedb_sql::types::plan::ArrayPrefilter>,
        ann_options: &VectorAnnOptions,
        skip_payload_fetch: bool,
        payload_filters: &[SqlPayloadAtom],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_vector_search(
            self.engine,
            collection,
            field,
            query_vector,
            top_k,
            ef_search,
            metric,
            filters,
            array_prefilter,
            ann_options,
            skip_payload_fetch,
            payload_filters,
        )
    }

    fn text_search(
        &mut self,
        collection: &str,
        query: &FtsQuery,
        top_k: usize,
        filters: &[Filter],
        score_alias: Option<&str>,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_text_search(self.engine, collection, query, top_k, filters, score_alias)
    }

    fn document_index_lookup(
        &mut self,
        collection: &str,
        alias: Option<&str>,
        engine_type: EngineType,
        field: &str,
        value: &SqlValue,
        filters: &[Filter],
        projection: &[Projection],
        sort_keys: &[SortKey],
        limit: Option<usize>,
        offset: usize,
        distinct: bool,
        window_functions: &[WindowSpec],
        case_insensitive: bool,
        temporal: &TemporalScope,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_document_index_lookup(
            self.engine,
            collection,
            alias,
            engine_type,
            field,
            value,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
            case_insensitive,
            temporal,
        )
    }

    fn range_scan(
        &mut self,
        collection: &str,
        field: &str,
        lower: Option<&SqlValue>,
        upper: Option<&SqlValue>,
        limit: usize,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_range_scan(self.engine, collection, field, lower, upper, limit)
    }

    fn insert_select(
        &mut self,
        target: &str,
        source: &nodedb_sql::types::SqlPlan,
        limit: usize,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_insert_select(self.engine, target, source, limit)
    }

    fn update_from(
        &mut self,
        collection: &str,
        engine: EngineType,
        source: &nodedb_sql::types::SqlPlan,
        target_join_col: &str,
        source_join_col: &str,
        assignments: &[(String, SqlExpr)],
        target_filters: &[Filter],
        returning: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_update_from(
            self.engine,
            collection,
            engine,
            source,
            target_join_col,
            source_join_col,
            assignments,
            target_filters,
            returning,
        )
    }

    fn join(
        &mut self,
        left: &nodedb_sql::types::SqlPlan,
        right: &nodedb_sql::types::SqlPlan,
        on: &[(String, String)],
        join_type: nodedb_sql::types::query::JoinType,
        condition: Option<&SqlExpr>,
        limit: usize,
        projection: &[Projection],
        filters: &[Filter],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_join(
            self.engine,
            left,
            right,
            on,
            join_type,
            condition,
            limit,
            projection,
            filters,
        )
    }

    fn aggregate(
        &mut self,
        input: &nodedb_sql::types::SqlPlan,
        group_by: &[SqlExpr],
        aggregates: &[nodedb_sql::types::query::AggregateExpr],
        having: &[Filter],
        limit: usize,
        grouping_sets: Option<&[Vec<usize>]>,
        sort_keys: &[SortKey],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_aggregate(
            self.engine,
            input,
            group_by,
            aggregates,
            having,
            limit,
            grouping_sets,
            sort_keys,
        )
    }

    fn union(
        &mut self,
        inputs: &[nodedb_sql::types::SqlPlan],
        distinct: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_union(self.engine, inputs, distinct)
    }

    fn intersect(
        &mut self,
        left: &nodedb_sql::types::SqlPlan,
        right: &nodedb_sql::types::SqlPlan,
        all: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_intersect(self.engine, left, right, all)
    }

    fn except(
        &mut self,
        left: &nodedb_sql::types::SqlPlan,
        right: &nodedb_sql::types::SqlPlan,
        all: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_except(self.engine, left, right, all)
    }

    fn cte(
        &mut self,
        definitions: &[(String, nodedb_sql::types::SqlPlan)],
        outer: &nodedb_sql::types::SqlPlan,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_cte(self.engine, definitions, outer)
    }

    fn merge(
        &mut self,
        target: &str,
        engine: EngineType,
        source: &nodedb_sql::types::SqlPlan,
        target_join_col: &str,
        source_join_col: &str,
        source_alias: &str,
        clauses: &[nodedb_sql::types::plan::MergePlanClause],
        returning: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_merge(
            self.engine,
            target,
            engine,
            source,
            target_join_col,
            source_join_col,
            source_alias,
            clauses,
            returning,
        )
    }

    fn multi_vector_search(
        &mut self,
        collection: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_multi_vector_search(self.engine, collection, query_vector, top_k, ef_search)
    }

    fn hybrid_search(
        &mut self,
        collection: &str,
        query_vector: &[f32],
        query_text: &str,
        top_k: usize,
        ef_search: usize,
        vector_weight: f32,
        fuzzy: bool,
        score_alias: Option<&str>,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_hybrid_search(
            self.engine,
            collection,
            query_vector,
            query_text,
            top_k,
            ef_search,
            vector_weight,
            fuzzy,
            score_alias,
        )
    }

    fn hybrid_search_triple(
        &mut self,
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
        lower_hybrid_search_triple(
            self.engine,
            collection,
            query_vector,
            query_text,
            graph_seed_id,
            graph_depth,
            graph_edge_label,
            top_k,
            ef_search,
            fuzzy,
            rrf_k,
            score_alias,
        )
    }

    fn spatial_scan(
        &mut self,
        collection: &str,
        field: &str,
        predicate: &nodedb_sql::types::query::SpatialPredicate,
        query_geometry: &nodedb_types::geometry::Geometry,
        distance_meters: f64,
        attribute_filters: &[Filter],
        limit: usize,
        projection: &[Projection],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_spatial_scan(
            self.engine,
            collection,
            field,
            predicate,
            query_geometry,
            distance_meters,
            attribute_filters,
            limit,
            projection,
        )
    }

    fn timeseries_scan(
        &mut self,
        collection: &str,
        time_range: (i64, i64),
        bucket_interval_ms: i64,
        group_by: &[String],
        aggregates: &[nodedb_sql::types::query::AggregateExpr],
        filters: &[Filter],
        projection: &[Projection],
        gap_fill: &str,
        limit: usize,
        tiered: bool,
        temporal: &TemporalScope,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_timeseries_scan(
            self.engine,
            collection,
            time_range,
            bucket_interval_ms,
            group_by,
            aggregates,
            filters,
            projection,
            gap_fill,
            limit,
            tiered,
            temporal,
        )
    }

    fn timeseries_ingest(
        &mut self,
        collection: &str,
        rows: &[Vec<(String, SqlValue)>],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_timeseries_ingest(self.engine, collection, rows)
    }

    fn vector_primary_insert(
        &mut self,
        collection: &str,
        field: &str,
        quantization: &nodedb_types::VectorQuantization,
        storage_dtype: &nodedb_types::VectorStorageDtype,
        payload_indexes: &[(String, nodedb_types::PayloadIndexKind)],
        rows: &[nodedb_sql::types::plan::VectorPrimaryRow],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_vector_primary_insert(
            self.engine,
            collection,
            field,
            quantization,
            storage_dtype,
            payload_indexes,
            rows,
        )
    }

    fn recursive_scan(
        &mut self,
        collection: &str,
        base_filters: &[Filter],
        recursive_filters: &[Filter],
        join_link: Option<&(String, String)>,
        max_iterations: usize,
        distinct: bool,
        limit: usize,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_recursive_scan(
            self.engine,
            collection,
            base_filters,
            recursive_filters,
            join_link,
            max_iterations,
            distinct,
            limit,
        )
    }

    fn recursive_value(
        &mut self,
        cte_name: &str,
        columns: &[String],
        init_exprs: &[String],
        step_exprs: &[String],
        condition: Option<&str>,
        max_depth: usize,
        distinct: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_recursive_value(
            self.engine,
            cte_name,
            columns,
            init_exprs,
            step_exprs,
            condition,
            max_depth,
            distinct,
        )
    }

    fn lateral_top_k(
        &mut self,
        outer: &nodedb_sql::types::SqlPlan,
        outer_alias: Option<&str>,
        inner_collection: &str,
        inner_filters: &[Filter],
        inner_order_by: &[SortKey],
        inner_limit: usize,
        correlation_keys: &[(String, String)],
        lateral_alias: &str,
        projection: &[Projection],
        left_join: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_lateral_top_k(
            self.engine,
            outer,
            outer_alias,
            inner_collection,
            inner_filters,
            inner_order_by,
            inner_limit,
            correlation_keys,
            lateral_alias,
            projection,
            left_join,
        )
    }

    fn lateral_loop(
        &mut self,
        outer: &nodedb_sql::types::SqlPlan,
        outer_alias: Option<&str>,
        inner: &nodedb_sql::types::SqlPlan,
        correlation_predicates: &[(String, String)],
        lateral_alias: &str,
        projection: &[Projection],
        outer_row_cap: usize,
        left_join: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_lateral_loop(
            self.engine,
            outer,
            outer_alias,
            inner,
            correlation_predicates,
            lateral_alias,
            projection,
            outer_row_cap,
            left_join,
        )
    }

    fn kv_insert(
        &mut self,
        collection: &str,
        entries: &[(SqlValue, Vec<(String, SqlValue)>)],
        ttl_secs: u64,
        intent: nodedb_sql::types::plan::KvInsertIntent,
        on_conflict_updates: &[(String, SqlExpr)],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_kv_insert(
            self.engine,
            collection,
            entries,
            ttl_secs,
            intent,
            on_conflict_updates,
        )
    }

    fn create_array(
        &mut self,
        name: &str,
        dims: &[nodedb_sql::types_array::ArrayDimAst],
        attrs: &[nodedb_sql::types_array::ArrayAttrAst],
        tile_extents: &[i64],
        cell_order: nodedb_sql::types_array::ArrayCellOrderAst,
        tile_order: nodedb_sql::types_array::ArrayTileOrderAst,
        prefix_bits: u8,
        audit_retain_ms: Option<u64>,
        minimum_audit_retain_ms: Option<u64>,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_create_array(
            self.engine,
            name,
            dims,
            attrs,
            tile_extents,
            cell_order,
            tile_order,
            prefix_bits,
            audit_retain_ms,
            minimum_audit_retain_ms,
        )
    }

    fn drop_array(&mut self, name: &str, if_exists: bool) -> Result<LiteFut<'a>, LiteError> {
        lower_drop_array(self.engine, name, if_exists)
    }

    fn alter_array(
        &mut self,
        name: &str,
        audit_retain_ms: Option<Option<i64>>,
        minimum_audit_retain_ms: Option<u64>,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_alter_array(self.engine, name, audit_retain_ms, minimum_audit_retain_ms)
    }

    fn insert_array(
        &mut self,
        name: &str,
        rows: &[nodedb_sql::types_array::ArrayInsertRow],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_insert_array(self.engine, name, rows)
    }

    fn delete_array(
        &mut self,
        name: &str,
        coords: &[Vec<nodedb_sql::types_array::ArrayCoordLiteral>],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_delete_array(self.engine, name, coords)
    }

    fn array_slice(
        &mut self,
        name: &str,
        slice: &nodedb_sql::types_array::ArraySliceAst,
        attr_projection: &[String],
        limit: u32,
        temporal: &TemporalScope,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_array_slice(self.engine, name, slice, attr_projection, limit, temporal)
    }

    fn array_project(
        &mut self,
        name: &str,
        attr_projection: &[String],
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_array_project(self.engine, name, attr_projection)
    }

    fn array_agg(
        &mut self,
        name: &str,
        attr: &str,
        reducer: &nodedb_sql::types_array::ArrayReducerAst,
        group_by_dim: Option<&str>,
        temporal: &TemporalScope,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_array_agg(self.engine, name, attr, reducer, group_by_dim, temporal)
    }

    fn array_elementwise(
        &mut self,
        left: &str,
        right: &str,
        op: nodedb_sql::types_array::ArrayBinaryOpAst,
        attr: &str,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_array_elementwise(self.engine, left, right, op, attr)
    }

    fn array_flush(&mut self, name: &str) -> Result<LiteFut<'a>, LiteError> {
        lower_array_flush(self.engine, name)
    }

    fn array_compact(&mut self, name: &str) -> Result<LiteFut<'a>, LiteError> {
        lower_array_compact(self.engine, name)
    }

    fn create_index(
        &mut self,
        _index_name: Option<&str>,
        collection: &str,
        field: &str,
        unique: bool,
        _if_not_exists: bool,
        case_insensitive: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_create_index(self.engine, collection, field, unique, case_insensitive)
    }

    fn drop_index(
        &mut self,
        index_name: &str,
        collection: Option<&str>,
        _if_exists: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        lower_drop_index(self.engine, index_name, collection)
    }
}
