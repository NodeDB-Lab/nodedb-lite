// SPDX-License-Identifier: Apache-2.0
//! Macro that expands to 34 PlanVisitor method stubs returning `LiteError::Unsupported`.
//! Invoked once from `adapter.rs` inside the single `impl PlanVisitor for LiteVisitor` block.

macro_rules! impl_unsupported_lite_visitor_methods {
    () => {
        fn document_index_lookup(
            &mut self,
            _collection: &str,
            _alias: Option<&str>,
            _engine: nodedb_sql::types::query::EngineType,
            _field: &str,
            _value: &nodedb_sql::types::SqlValue,
            _filters: &[nodedb_sql::types::filter::Filter],
            _projection: &[nodedb_sql::types::query::Projection],
            _sort_keys: &[nodedb_sql::types::query::SortKey],
            _limit: Option<usize>,
            _offset: usize,
            _distinct: bool,
            _window_functions: &[nodedb_sql::types::query::WindowSpec],
            _case_insensitive: bool,
            _temporal: &nodedb_sql::temporal::TemporalScope,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("DocumentIndexLookup")
        }

        fn range_scan(
            &mut self,
            _collection: &str,
            _field: &str,
            _lower: Option<&nodedb_sql::types::SqlValue>,
            _upper: Option<&nodedb_sql::types::SqlValue>,
            _limit: usize,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("RangeScan")
        }

        fn kv_insert(
            &mut self,
            _collection: &str,
            _entries: &[(
                nodedb_sql::types::SqlValue,
                Vec<(String, nodedb_sql::types::SqlValue)>,
            )],
            _ttl_secs: u64,
            _intent: nodedb_sql::types::plan::KvInsertIntent,
            _on_conflict_updates: &[(String, nodedb_sql::types_expr::SqlExpr)],
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("KvInsert")
        }

        fn insert_select(
            &mut self,
            _target: &str,
            _source: &nodedb_sql::types::SqlPlan,
            _limit: usize,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("InsertSelect")
        }

        fn update_from(
            &mut self,
            _collection: &str,
            _engine: nodedb_sql::types::query::EngineType,
            _source: &nodedb_sql::types::SqlPlan,
            _target_join_col: &str,
            _source_join_col: &str,
            _assignments: &[(String, nodedb_sql::types_expr::SqlExpr)],
            _target_filters: &[nodedb_sql::types::filter::Filter],
            _returning: bool,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("UpdateFrom")
        }

        fn join(
            &mut self,
            _left: &nodedb_sql::types::SqlPlan,
            _right: &nodedb_sql::types::SqlPlan,
            _on: &[(String, String)],
            _join_type: nodedb_sql::types::query::JoinType,
            _condition: Option<&nodedb_sql::types_expr::SqlExpr>,
            _limit: usize,
            _projection: &[nodedb_sql::types::query::Projection],
            _filters: &[nodedb_sql::types::filter::Filter],
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("Join")
        }

        fn aggregate(
            &mut self,
            _input: &nodedb_sql::types::SqlPlan,
            _group_by: &[nodedb_sql::types_expr::SqlExpr],
            _aggregates: &[nodedb_sql::types::query::AggregateExpr],
            _having: &[nodedb_sql::types::filter::Filter],
            _limit: usize,
            _grouping_sets: Option<&[Vec<usize>]>,
            _sort_keys: &[nodedb_sql::types::query::SortKey],
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("Aggregate")
        }

        fn timeseries_scan(
            &mut self,
            _collection: &str,
            _time_range: (i64, i64),
            _bucket_interval_ms: i64,
            _group_by: &[String],
            _aggregates: &[nodedb_sql::types::query::AggregateExpr],
            _filters: &[nodedb_sql::types::filter::Filter],
            _projection: &[nodedb_sql::types::query::Projection],
            _gap_fill: &str,
            _limit: usize,
            _tiered: bool,
            _temporal: &nodedb_sql::temporal::TemporalScope,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("TimeseriesScan")
        }

        fn timeseries_ingest(
            &mut self,
            _collection: &str,
            _rows: &[Vec<(String, nodedb_sql::types::SqlValue)>],
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("TimeseriesIngest")
        }

        fn multi_vector_search(
            &mut self,
            _collection: &str,
            _query_vector: &[f32],
            _top_k: usize,
            _ef_search: usize,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("MultiVectorSearch")
        }

        fn hybrid_search(
            &mut self,
            _collection: &str,
            _query_vector: &[f32],
            _query_text: &str,
            _top_k: usize,
            _ef_search: usize,
            _vector_weight: f32,
            _fuzzy: bool,
            _score_alias: Option<&str>,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("HybridSearch")
        }

        fn hybrid_search_triple(
            &mut self,
            _collection: &str,
            _query_vector: &[f32],
            _query_text: &str,
            _graph_seed_id: &str,
            _graph_depth: usize,
            _graph_edge_label: Option<&str>,
            _top_k: usize,
            _ef_search: usize,
            _fuzzy: bool,
            _rrf_k: (f64, f64, f64),
            _score_alias: Option<&str>,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("HybridSearchTriple")
        }

        fn spatial_scan(
            &mut self,
            _collection: &str,
            _field: &str,
            _predicate: &nodedb_sql::types::query::SpatialPredicate,
            _query_geometry: &nodedb_types::geometry::Geometry,
            _distance_meters: f64,
            _attribute_filters: &[nodedb_sql::types::filter::Filter],
            _limit: usize,
            _projection: &[nodedb_sql::types::query::Projection],
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("SpatialScan")
        }

        fn union(
            &mut self,
            _inputs: &[nodedb_sql::types::SqlPlan],
            _distinct: bool,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("Union")
        }

        fn intersect(
            &mut self,
            _left: &nodedb_sql::types::SqlPlan,
            _right: &nodedb_sql::types::SqlPlan,
            _all: bool,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("Intersect")
        }

        fn except(
            &mut self,
            _left: &nodedb_sql::types::SqlPlan,
            _right: &nodedb_sql::types::SqlPlan,
            _all: bool,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("Except")
        }

        fn recursive_scan(
            &mut self,
            _collection: &str,
            _base_filters: &[nodedb_sql::types::filter::Filter],
            _recursive_filters: &[nodedb_sql::types::filter::Filter],
            _join_link: Option<&(String, String)>,
            _max_iterations: usize,
            _distinct: bool,
            _limit: usize,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("RecursiveScan")
        }

        fn recursive_value(
            &mut self,
            _cte_name: &str,
            _columns: &[String],
            _init_exprs: &[String],
            _step_exprs: &[String],
            _condition: Option<&str>,
            _max_depth: usize,
            _distinct: bool,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("RecursiveValue")
        }

        fn cte(
            &mut self,
            _definitions: &[(String, nodedb_sql::types::SqlPlan)],
            _outer: &nodedb_sql::types::SqlPlan,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("Cte")
        }

        fn create_array(
            &mut self,
            _name: &str,
            _dims: &[nodedb_sql::types_array::ArrayDimAst],
            _attrs: &[nodedb_sql::types_array::ArrayAttrAst],
            _tile_extents: &[i64],
            _cell_order: nodedb_sql::types_array::ArrayCellOrderAst,
            _tile_order: nodedb_sql::types_array::ArrayTileOrderAst,
            _prefix_bits: u8,
            _audit_retain_ms: Option<u64>,
            _minimum_audit_retain_ms: Option<u64>,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("CreateArray")
        }

        fn drop_array(&mut self, _name: &str, _if_exists: bool) -> Result<LiteFut<'a>, LiteError> {
            u!("DropArray")
        }

        fn alter_array(
            &mut self,
            _name: &str,
            _audit_retain_ms: Option<Option<i64>>,
            _minimum_audit_retain_ms: Option<u64>,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("AlterArray")
        }

        fn insert_array(
            &mut self,
            _name: &str,
            _rows: &[nodedb_sql::types_array::ArrayInsertRow],
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("InsertArray")
        }

        fn delete_array(
            &mut self,
            _name: &str,
            _coords: &[Vec<nodedb_sql::types_array::ArrayCoordLiteral>],
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("DeleteArray")
        }

        fn array_slice(
            &mut self,
            _name: &str,
            _slice: &nodedb_sql::types_array::ArraySliceAst,
            _attr_projection: &[String],
            _limit: u32,
            _temporal: &nodedb_sql::temporal::TemporalScope,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("ArraySlice")
        }

        fn array_project(
            &mut self,
            _name: &str,
            _attr_projection: &[String],
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("ArrayProject")
        }

        fn array_agg(
            &mut self,
            _name: &str,
            _attr: &str,
            _reducer: &nodedb_sql::types_array::ArrayReducerAst,
            _group_by_dim: Option<&str>,
            _temporal: &nodedb_sql::temporal::TemporalScope,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("ArrayAgg")
        }

        fn array_elementwise(
            &mut self,
            _left: &str,
            _right: &str,
            _op: nodedb_sql::types_array::ArrayBinaryOpAst,
            _attr: &str,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("ArrayElementwise")
        }

        fn array_flush(&mut self, _name: &str) -> Result<LiteFut<'a>, LiteError> {
            u!("ArrayFlush")
        }

        fn array_compact(&mut self, _name: &str) -> Result<LiteFut<'a>, LiteError> {
            u!("ArrayCompact")
        }

        fn merge(
            &mut self,
            _target: &str,
            _engine: nodedb_sql::types::query::EngineType,
            _source: &nodedb_sql::types::SqlPlan,
            _target_join_col: &str,
            _source_join_col: &str,
            _source_alias: &str,
            _clauses: &[nodedb_sql::types::plan::MergePlanClause],
            _returning: bool,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("Merge")
        }

        fn lateral_top_k(
            &mut self,
            _outer: &nodedb_sql::types::SqlPlan,
            _outer_alias: Option<&str>,
            _inner_collection: &str,
            _inner_filters: &[nodedb_sql::types::filter::Filter],
            _inner_order_by: &[nodedb_sql::types::query::SortKey],
            _inner_limit: usize,
            _correlation_keys: &[(String, String)],
            _lateral_alias: &str,
            _projection: &[nodedb_sql::types::query::Projection],
            _left_join: bool,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("LateralTopK")
        }

        fn lateral_loop(
            &mut self,
            _outer: &nodedb_sql::types::SqlPlan,
            _outer_alias: Option<&str>,
            _inner: &nodedb_sql::types::SqlPlan,
            _correlation_predicates: &[(String, String)],
            _lateral_alias: &str,
            _projection: &[nodedb_sql::types::query::Projection],
            _outer_row_cap: usize,
            _left_join: bool,
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("LateralLoop")
        }

        fn vector_primary_insert(
            &mut self,
            _collection: &str,
            _field: &str,
            _quantization: &nodedb_types::VectorQuantization,
            _storage_dtype: &nodedb_types::VectorStorageDtype,
            _payload_indexes: &[(String, nodedb_types::PayloadIndexKind)],
            _rows: &[nodedb_sql::types::plan::VectorPrimaryRow],
        ) -> Result<LiteFut<'a>, LiteError> {
            u!("VectorPrimaryInsert")
        }
    };
}

pub(super) use impl_unsupported_lite_visitor_methods;
