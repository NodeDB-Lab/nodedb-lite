// SPDX-License-Identifier: Apache-2.0

//! `LiteVisitor` struct + `PlanVisitor` trait impl. Every method is a
//! one-line delegation to a sibling module split by statement family — see
//! `mod.rs` for the concern-to-module map. Adding a new `SqlPlan` variant
//! becomes a hard compile error here, which is the intended forcing
//! function for exhaustive coverage.

use std::future::Future;
use std::pin::Pin;

use nodedb_sql::PlanVisitor;
use nodedb_sql::fts_types::FtsQuery;
use nodedb_sql::temporal::TemporalScope;
use nodedb_sql::types::SqlValue;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::query::EngineType;
use nodedb_sql::types_expr::SqlExpr;
use nodedb_sql::{
    AggregateVisitArgs, CreateArrayVisitArgs, DocumentIndexLookupVisitArgs,
    HybridSearchTripleVisitArgs, HybridSearchVisitArgs, InsertVisitArgs, JoinVisitArgs,
    LateralLoopVisitArgs, LateralTopKVisitArgs, MergeVisitArgs, RecursiveScanVisitArgs,
    RecursiveValueVisitArgs, ScanVisitArgs, SpatialScanVisitArgs, SubqueryVisitArgs,
    TimeseriesScanVisitArgs, UpdateFromVisitArgs, UpsertVisitArgs, VectorPrimaryDeleteVisitArgs,
    VectorPrimaryInsertVisitArgs, VectorPrimaryUpdateVisitArgs, VectorSearchVisitArgs,
};
use nodedb_types::result::QueryResult;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::{admin, array, ddl, dml, kv, reads_combine, reads_scan, reads_search, vector};

// On wasm32 the StorageEngine futures are `!Send`, so we cannot require Send
// on the visitor future type.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) type LiteFut<'a> =
    Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + Send + 'a>>;

#[cfg(target_arch = "wasm32")]
pub(crate) type LiteFut<'a> = Pin<Box<dyn Future<Output = Result<QueryResult, LiteError>> + 'a>>;

pub(crate) struct LiteVisitor<'a, S: StorageEngine> {
    pub(crate) engine: &'a LiteQueryEngine<S>,
}

impl<'a, S: StorageEngine + 'a> PlanVisitor for LiteVisitor<'a, S> {
    type Output = LiteFut<'a>;
    type Error = LiteError;

    fn constant_result(
        &mut self,
        columns: &[String],
        values: &[SqlValue],
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_scan::constant_result(self.engine, columns, values)
    }

    fn scan(&mut self, args: ScanVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        reads_scan::scan(self.engine, args)
    }

    fn point_get(
        &mut self,
        collection: &str,
        alias: Option<&str>,
        engine_type: EngineType,
        key_column: &str,
        key_value: &SqlValue,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_scan::point_get(
            self.engine,
            collection,
            alias,
            engine_type,
            key_column,
            key_value,
        )
    }

    fn insert(&mut self, args: InsertVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        dml::insert(self.engine, args)
    }

    fn upsert(&mut self, args: UpsertVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        dml::upsert(self.engine, args)
    }

    fn update(
        &mut self,
        collection: &str,
        engine_type: EngineType,
        assignments: &[(String, SqlExpr)],
        filters: &[Filter],
        target_keys: &[SqlValue],
        returning: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        dml::update(
            self.engine,
            collection,
            engine_type,
            assignments,
            filters,
            target_keys,
            returning,
        )
    }

    fn delete(
        &mut self,
        collection: &str,
        engine_type: EngineType,
        filters: &[Filter],
        target_keys: &[SqlValue],
    ) -> Result<LiteFut<'a>, LiteError> {
        dml::delete(self.engine, collection, engine_type, filters, target_keys)
    }

    fn truncate(
        &mut self,
        collection: &str,
        engine: EngineType,
        restart_identity: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        admin::truncate(self.engine, collection, engine, restart_identity)
    }

    fn vector_search(&mut self, args: VectorSearchVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        reads_search::vector_search(self.engine, args)
    }

    fn text_search(
        &mut self,
        collection: &str,
        query: &FtsQuery,
        top_k: usize,
        filters: &[Filter],
        score_alias: Option<&str>,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_search::text_search(self.engine, collection, query, top_k, filters, score_alias)
    }

    fn document_index_lookup(
        &mut self,
        args: DocumentIndexLookupVisitArgs<'_>,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_scan::document_index_lookup(self.engine, args)
    }

    fn range_scan(
        &mut self,
        collection: &str,
        field: &str,
        lower: Option<&SqlValue>,
        upper: Option<&SqlValue>,
        limit: usize,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_scan::range_scan(self.engine, collection, field, lower, upper, limit)
    }

    fn insert_select(
        &mut self,
        target: &str,
        source: &nodedb_sql::types::SqlPlan,
        limit: usize,
        column_map: &[(String, SqlExpr)],
    ) -> Result<LiteFut<'a>, LiteError> {
        dml::insert_select(self.engine, target, source, limit, column_map)
    }

    fn update_from(&mut self, args: UpdateFromVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        dml::update_from(self.engine, args)
    }

    fn join(&mut self, args: JoinVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        reads_combine::join(self.engine, args)
    }

    fn aggregate(&mut self, args: AggregateVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        reads_combine::aggregate(self.engine, args)
    }

    fn union(
        &mut self,
        inputs: &[nodedb_sql::types::SqlPlan],
        distinct: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_combine::union(self.engine, inputs, distinct)
    }

    fn intersect(
        &mut self,
        left: &nodedb_sql::types::SqlPlan,
        right: &nodedb_sql::types::SqlPlan,
        all: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_combine::intersect(self.engine, left, right, all)
    }

    fn except(
        &mut self,
        left: &nodedb_sql::types::SqlPlan,
        right: &nodedb_sql::types::SqlPlan,
        all: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_combine::except(self.engine, left, right, all)
    }

    fn cte(
        &mut self,
        definitions: &[(String, nodedb_sql::types::SqlPlan)],
        outer: &nodedb_sql::types::SqlPlan,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_combine::cte(self.engine, definitions, outer)
    }

    fn subquery(&mut self, args: SubqueryVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        reads_combine::subquery(self.engine, args)
    }

    fn merge(&mut self, args: MergeVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        dml::merge(self.engine, args)
    }

    fn multi_vector_search(
        &mut self,
        collection: &str,
        query_vector: &[f32],
        top_k: usize,
        ef_search: usize,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_search::multi_vector_search(self.engine, collection, query_vector, top_k, ef_search)
    }

    fn sparse_search(
        &mut self,
        collection: &str,
        field: &str,
        query_entries: &[(u32, f32)],
        top_k: usize,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_search::sparse_search(self.engine, collection, field, query_entries, top_k)
    }

    fn hybrid_search(&mut self, args: HybridSearchVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        reads_search::hybrid_search(self.engine, args)
    }

    fn hybrid_search_triple(
        &mut self,
        args: HybridSearchTripleVisitArgs<'_>,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_search::hybrid_search_triple(self.engine, args)
    }

    fn spatial_scan(&mut self, args: SpatialScanVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        reads_scan::spatial_scan(self.engine, args)
    }

    fn timeseries_scan(
        &mut self,
        args: TimeseriesScanVisitArgs<'_>,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_scan::timeseries_scan(self.engine, args)
    }

    fn timeseries_ingest(
        &mut self,
        collection: &str,
        rows: &[Vec<(String, SqlValue)>],
    ) -> Result<LiteFut<'a>, LiteError> {
        dml::timeseries_ingest(self.engine, collection, rows)
    }

    fn vector_primary_insert(
        &mut self,
        args: VectorPrimaryInsertVisitArgs<'_>,
    ) -> Result<LiteFut<'a>, LiteError> {
        vector::vector_primary_insert(self.engine, args)
    }

    fn vector_primary_delete(
        &mut self,
        args: VectorPrimaryDeleteVisitArgs<'_>,
    ) -> Result<LiteFut<'a>, LiteError> {
        vector::vector_primary_delete(self.engine, args)
    }

    fn vector_primary_truncate(
        &mut self,
        collection: &str,
        field: &str,
        restart_identity: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        vector::vector_primary_truncate(self.engine, collection, field, restart_identity)
    }

    fn vector_primary_update(
        &mut self,
        args: VectorPrimaryUpdateVisitArgs<'_>,
    ) -> Result<LiteFut<'a>, LiteError> {
        vector::vector_primary_update(self.engine, args)
    }

    fn recursive_scan(
        &mut self,
        args: RecursiveScanVisitArgs<'_>,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_scan::recursive_scan(self.engine, args)
    }

    fn recursive_value(
        &mut self,
        args: RecursiveValueVisitArgs<'_>,
    ) -> Result<LiteFut<'a>, LiteError> {
        reads_scan::recursive_value(self.engine, args)
    }

    fn lateral_top_k(&mut self, args: LateralTopKVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        reads_scan::lateral_top_k(self.engine, args)
    }

    fn lateral_loop(&mut self, args: LateralLoopVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        reads_scan::lateral_loop(self.engine, args)
    }

    fn kv_insert(
        &mut self,
        collection: &str,
        entries: &[(SqlValue, Vec<(String, SqlValue)>)],
        ttl_secs: u64,
        intent: nodedb_sql::types::plan::KvInsertIntent,
        on_conflict_updates: &[(String, SqlExpr)],
    ) -> Result<LiteFut<'a>, LiteError> {
        kv::kv_insert(
            self.engine,
            collection,
            entries,
            ttl_secs,
            intent,
            on_conflict_updates,
        )
    }

    fn create_array(&mut self, args: CreateArrayVisitArgs<'_>) -> Result<LiteFut<'a>, LiteError> {
        ddl::create_array(self.engine, args)
    }

    fn drop_array(&mut self, name: &str, if_exists: bool) -> Result<LiteFut<'a>, LiteError> {
        ddl::drop_array(self.engine, name, if_exists)
    }

    fn alter_array(
        &mut self,
        name: &str,
        audit_retain_ms: Option<Option<i64>>,
        minimum_audit_retain_ms: Option<u64>,
    ) -> Result<LiteFut<'a>, LiteError> {
        ddl::alter_array(self.engine, name, audit_retain_ms, minimum_audit_retain_ms)
    }

    fn insert_array(
        &mut self,
        name: &str,
        rows: &[nodedb_sql::types_array::ArrayInsertRow],
    ) -> Result<LiteFut<'a>, LiteError> {
        array::insert_array(self.engine, name, rows)
    }

    fn delete_array(
        &mut self,
        name: &str,
        coords: &[Vec<nodedb_sql::types_array::ArrayCoordLiteral>],
    ) -> Result<LiteFut<'a>, LiteError> {
        array::delete_array(self.engine, name, coords)
    }

    fn array_slice(
        &mut self,
        name: &str,
        slice: &nodedb_sql::types_array::ArraySliceAst,
        attr_projection: &[String],
        limit: u32,
        temporal: &TemporalScope,
    ) -> Result<LiteFut<'a>, LiteError> {
        array::array_slice(self.engine, name, slice, attr_projection, limit, temporal)
    }

    fn array_project(
        &mut self,
        name: &str,
        attr_projection: &[String],
    ) -> Result<LiteFut<'a>, LiteError> {
        array::array_project(self.engine, name, attr_projection)
    }

    fn array_agg(
        &mut self,
        name: &str,
        attr: &str,
        reducer: &nodedb_sql::types_array::ArrayReducerAst,
        group_by_dim: Option<&str>,
        temporal: &TemporalScope,
    ) -> Result<LiteFut<'a>, LiteError> {
        array::array_agg(self.engine, name, attr, reducer, group_by_dim, temporal)
    }

    fn array_elementwise(
        &mut self,
        left: &str,
        right: &str,
        op: nodedb_sql::types_array::ArrayBinaryOpAst,
        attr: &str,
    ) -> Result<LiteFut<'a>, LiteError> {
        array::array_elementwise(self.engine, left, right, op, attr)
    }

    fn array_flush(&mut self, name: &str) -> Result<LiteFut<'a>, LiteError> {
        array::array_flush(self.engine, name)
    }

    fn array_compact(&mut self, name: &str) -> Result<LiteFut<'a>, LiteError> {
        array::array_compact(self.engine, name)
    }

    fn create_index(
        &mut self,
        index_name: Option<&str>,
        collection: &str,
        field: &str,
        unique: bool,
        if_not_exists: bool,
        case_insensitive: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        admin::create_index(
            self.engine,
            index_name,
            collection,
            field,
            unique,
            if_not_exists,
            case_insensitive,
        )
    }

    fn drop_index(
        &mut self,
        index_name: &str,
        collection: Option<&str>,
        if_exists: bool,
    ) -> Result<LiteFut<'a>, LiteError> {
        admin::drop_index(self.engine, index_name, collection, if_exists)
    }
}
