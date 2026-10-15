// SPDX-License-Identifier: Apache-2.0
//! SQL-visitor lowering for query-shaped SqlPlan variants:
//! Aggregate, Join, DocumentIndexLookup, RangeScan, Cte, Subquery.

use std::collections::HashMap;

use nodedb_physical::PhysicalTaskVisitor;
use nodedb_physical::physical_plan::document::DocumentOp;
use nodedb_physical::physical_plan::query::AggregateSpec;
use nodedb_physical::physical_plan::{PARAMETERIZED_AGGREGATE_FUNCTIONS, SortKeySpec};
use nodedb_query::expr::GroupKeySpec;
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::filter::Filter;
use nodedb_sql::types::query::{AggregateExpr, Projection, SortKey, WindowSpec};
use nodedb_sql::types_expr::{SqlExpr, SqlValue};
use nodedb_sql::{
    AggregateVisitArgs, DocumentIndexLookupVisitArgs, JoinVisitArgs, SubqueryVisitArgs,
};
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::expr_convert::convert_sql_expr;
use crate::query::filter_convert::{sql_filters_to_metadata, sql_value_to_value};
use crate::query::physical_visitor::LiteDataPlaneVisitor;
use crate::query::query_ops::aggregate::execute_aggregate;
use crate::query::query_ops::joins::inline_hash::execute_inline_hash_join;
use crate::query::query_ops::joins::spec::JoinSpec;
use crate::storage::engine::StorageEngine;

use super::adapter::LiteFut;
use super::having_eval::{apply_having_result, make_agg_alias_map};
use super::projection::join_projections;
use super::scan_post::{
    ScanPostArgs, apply_scan_post_processing, apply_window_functions, distinct_rows, filter_rows,
    project_rows, sort_rows,
};

/// Convert a `nodedb_sql` `AggregateExpr` to a physical `AggregateSpec`.
///
/// A computed argument (including a `FILTER` clause the planner folded into
/// a `CASE`) is evaluated per row. `DISTINCT` selects the deduplicating
/// accumulator. A parameterized aggregate (`approx_topk`,
/// `approx_percentile`, `percentile_cont`) is refused: Lite has no
/// accumulator for one.
pub(super) fn sql_agg_to_spec(agg: &AggregateExpr) -> Result<AggregateSpec, LiteError> {
    let function = nodedb_sql::planner::agg_naming::aggregate_function_name(agg);
    if PARAMETERIZED_AGGREGATE_FUNCTIONS.contains(&function.as_str()) {
        return Err(LiteError::Unsupported {
            detail: format!(
                "aggregate function {}() is not supported in Lite",
                agg.function
            ),
        });
    }
    let (field, expr) = match agg.args.first() {
        None | Some(SqlExpr::Wildcard) => ("*".to_string(), None),
        Some(SqlExpr::Column { name, .. }) => (name.clone(), None),
        Some(other) => ("*".to_string(), Some(convert_sql_expr(other)?)),
    };
    Ok(AggregateSpec {
        function,
        alias: agg.alias.clone(),
        user_alias: None,
        field,
        expr,
        param: None,
    })
}

fn convert_aggregates(aggs: &[AggregateExpr]) -> Result<Vec<AggregateSpec>, LiteError> {
    aggs.iter().map(sql_agg_to_spec).collect()
}

/// Convert a SQL `SortKey` into the physical-plan [`SortKeySpec`].
///
/// A bare column becomes a pushable key. Anything computed becomes a
/// deliberately NON-column spec, so `SortKeySpec::as_column()` reports `None`
/// and every consumer skips it — which is what already happened in practice:
/// the previous code stringified the expression with `format!("{other:?}")`
/// and used that as a column name, which could never match a real column. Same
/// observable ordering, but the limitation is now explicit in the type instead
/// of hidden behind an unmatchable name.
fn sort_key_to_spec(k: &SortKey) -> SortKeySpec {
    match &k.expr {
        SqlExpr::Column { name, .. } => SortKeySpec::column(name.clone(), k.ascending),
        _ => SortKeySpec {
            expr: nodedb_query::expr::SqlExpr::Literal(nodedb_types::value::Value::Null),
            ascending: k.ascending,
            // Same PostgreSQL default `SortKeySpec::column` applies:
            // ASC → NULLS LAST, DESC → NULLS FIRST.
            nulls_first: !k.ascending,
        },
    }
}

/// Encode `Vec<Filter>` → msgpack bytes via `ScanFilter`.
fn encode_filters(filters: &[Filter]) -> Result<Vec<u8>, LiteError> {
    if filters.is_empty() {
        return Ok(Vec::new());
    }
    // Complex QExpr predicates are evaluated post-scan; only primitive conditions
    // are pushed to the physical visitor via serialized MetadataFilter.
    match sql_filters_to_metadata(filters, &[])?.meta {
        None => Ok(Vec::new()),
        Some(mf) => zerompk::to_msgpack_vec(&mf).map_err(|e| LiteError::Serialization {
            detail: format!("encode filters: {e}"),
        }),
    }
}

/// Convert `QueryResult` rows to `Vec<HashMap<String, Value>>`.
fn result_to_maps(result: QueryResult) -> Vec<HashMap<String, Value>> {
    let cols = result.columns;
    result
        .rows
        .into_iter()
        .map(|row| cols.iter().cloned().zip(row).collect())
        .collect()
}

/// Encode a `QueryResult` as msgpack bytes for inline hash join.
fn encode_result_msgpack(result: &QueryResult) -> Result<Vec<u8>, LiteError> {
    let maps: Vec<HashMap<String, Value>> = result
        .rows
        .iter()
        .map(|row| {
            result
                .columns
                .iter()
                .cloned()
                .zip(row.iter().cloned())
                .collect()
        })
        .collect();
    zerompk::to_msgpack_vec(&maps).map_err(|e| LiteError::Serialization {
        detail: format!("encode join side msgpack: {e}"),
    })
}

// ── Aggregate ────────────────────────────────────────────────────────────────

pub(super) fn lower_aggregate<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    args: AggregateVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let AggregateVisitArgs {
        input,
        group_by,
        aggregates,
        having,
        limit,
        grouping_sets,
        sort_keys,
    } = args;
    let input = input.clone();
    // A bare column groups by field extraction; anything else is a computed key
    // (e.g. `GROUP BY date_trunc('day', ts)`) that must be evaluated per row.
    // Its output name is the SQL text the planner assigned, so HAVING/ORDER BY
    // and the result columns all agree on one label.
    let group_cols: Vec<GroupKeySpec> = group_by
        .iter()
        .enumerate()
        .map(|(i, e)| match e {
            SqlExpr::Column { name, .. } => Ok(GroupKeySpec::column(name.clone())),
            other => {
                let expr = convert_sql_expr(other)?;
                // `SqlPlan::Aggregate` carries `group_by_aliases`, but the
                // upstream dispatcher discards them before the visitor sees
                // them, so the positional label is the stable name available.
                Ok(GroupKeySpec {
                    output_name: format!("group_key_{i}"),
                    field: None,
                    expr: Some(expr),
                })
            }
        })
        .collect::<Result<Vec<_>, LiteError>>()?;
    let agg_specs = convert_aggregates(aggregates)?;
    // Build aggregate-function → alias lookup for HAVING post-filter.
    let agg_alias_map = make_agg_alias_map(aggregates);
    // HAVING predicates always reference aggregate results (e.g. SUM(salary) > 100)
    // which are not present as named columns until after aggregation.
    // apply_having_result handles all predicate shapes via having_eval, including
    // function-call resolution through agg_alias_map. We always do the post-filter
    // and pass empty bytes to execute_aggregate (no pushdown for HAVING).
    let having_post = having.to_vec();
    let sort_pairs: Vec<SortKeySpec> = sort_keys.iter().map(sort_key_to_spec).collect();
    let gs: Vec<Vec<u32>> = grouping_sets
        .unwrap_or(&[])
        .iter()
        .map(|s| s.iter().map(|&i| i as u32).collect())
        .collect();

    Ok(Box::pin(async move {
        let source_result = engine.execute_plan_admitted(&input, permit).await?;
        let rows = result_to_maps(source_result);
        let result = execute_aggregate(rows, &group_cols, &agg_specs, &[], &[], &sort_pairs, &gs)?;
        // LIMIT counts the groups HAVING keeps, in the sorted order.
        let mut result = apply_having_result(result, &having_post, &agg_alias_map);
        result.rows.truncate(limit);
        Ok(result)
    }))
}

// ── Join ─────────────────────────────────────────────────────────────────────

pub(super) fn lower_join<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    args: JoinVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let JoinVisitArgs {
        left,
        right,
        on,
        join_type,
        limit,
        projection,
        filters,
        ..
    } = args;
    let left = left.clone();
    let right = right.clone();
    let on = on.to_vec();
    let limit = limit.unwrap_or(usize::MAX);
    // JoinType debug output: Inner, Left, Right, Full — lower to string for hash join.
    let join_type_str = format!("{join_type:?}").to_lowercase();
    let proj = join_projections(projection, "JOIN")?;
    let post_filters_bytes = encode_filters(filters)?;

    Ok(Box::pin(async move {
        let left_result = engine.execute_plan_admitted(&left, permit).await?;
        let right_result = engine.execute_plan_admitted(&right, permit).await?;

        let left_bytes = encode_result_msgpack(&left_result)?;
        let right_bytes = encode_result_msgpack(&right_result)?;

        execute_inline_hash_join(
            &left_bytes,
            &right_bytes,
            None,
            &JoinSpec {
                on: &on,
                join_type: &join_type_str,
                limit,
                projection: &proj,
                post_filters: &post_filters_bytes,
            },
        )
    }))
}

// ── DocumentIndexLookup ──────────────────────────────────────────────────────

pub(super) fn lower_document_index_lookup<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: DocumentIndexLookupVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let DocumentIndexLookupVisitArgs {
        collection,
        field,
        value,
        filters,
        projection,
        sort_keys,
        limit,
        offset,
        distinct,
        window_functions,
        ..
    } = args;
    let collection = collection.to_string();
    let field = field.to_string();
    // The planner already lowercased the value for a case-insensitive index.
    let probe = sql_value_to_value(value)?;
    let filters = filters.to_vec();
    let sort_keys = sort_keys.to_vec();
    let window_functions = window_functions.to_vec();
    let projection = projection.to_vec();

    // The fetch returns every indexed match in full: residual filters,
    // ORDER BY, projection, OFFSET and LIMIT all run in post-processing.
    Ok(Box::pin(async move {
        let raw = crate::query::document_ops::index_reads::indexed_fetch(
            engine,
            &collection,
            &field,
            &probe,
            usize::MAX,
            0,
        )
        .await?;
        apply_scan_post_processing(
            raw,
            ScanPostArgs {
                filters: &filters,
                sort_keys: &sort_keys,
                window_specs: &window_functions,
                projection: &projection,
                sequences: engine.sequences(),
                limit,
                offset,
                distinct,
            },
        )
    }))
}

// ── RangeScan ────────────────────────────────────────────────────────────────

pub(super) fn lower_range_scan<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    field: &str,
    lower: Option<&SqlValue>,
    upper: Option<&SqlValue>,
    limit: usize,
) -> Result<LiteFut<'a>, LiteError> {
    // Lite holds a bare collection name; DatabaseId::DEFAULT keeps it unqualified.
    let col = nodedb_types::QualifiedCollection::new(nodedb_types::DatabaseId::DEFAULT, collection);
    let fld = field.to_string();

    let encode_bound = |v: &SqlValue| -> Result<Vec<u8>, LiteError> {
        let ndb_val = sql_value_to_value(v)?;
        zerompk::to_msgpack_vec(&ndb_val).map_err(|e| LiteError::Serialization {
            detail: format!("encode range bound: {e}"),
        })
    };

    let lo_bytes: Option<Vec<u8>> = lower.map(encode_bound).transpose()?;
    let hi_bytes: Option<Vec<u8>> = upper.map(encode_bound).transpose()?;

    let op = DocumentOp::RangeScan {
        collection: col,
        field: fld,
        lower: lo_bytes,
        upper: hi_bytes,
        limit,
        // Lite lowers this scan itself and applies no row-level security, so it
        // declares none rather than inventing filters it cannot enforce.
        rls_filters: Vec::new(),
    };

    let mut phys = LiteDataPlaneVisitor::new(engine);
    let fut = phys.document(&op)?;

    Ok(Box::pin(fut))
}

// ── Cte ──────────────────────────────────────────────────────────────────────

/// Non-recursive CTE lowering for Lite single-node.
///
/// Each CTE definition is executed in order (surfacing any errors it would
/// produce), then the outer query — which the planner has already resolved
/// against the CTE bodies — is executed. On Lite, the SQL planner inlines
/// non-recursive CTEs into the outer plan, so the outer query is always
/// self-contained; the definition executions here serve as an eager
/// validation pass.
pub(super) fn lower_cte<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    definitions: &[(String, SqlPlan)],
    outer: &SqlPlan,
) -> Result<LiteFut<'a>, LiteError> {
    let definitions = definitions.to_vec();
    let outer = outer.clone();

    Ok(Box::pin(async move {
        for (_name, def_plan) in &definitions {
            let _ = engine.execute_plan_admitted(def_plan, permit).await?;
        }
        engine.execute_plan_admitted(&outer, permit).await
    }))
}

// ── Subquery ─────────────────────────────────────────────────────────────────

/// Relational post-processing over a subquery / derived-table body, for Lite
/// single-node.
///
/// The body is materialized by executing `input`, then the outer constraints
/// the body's leaf could not absorb are applied in the same order the
/// distributed engine's `ProviderScan` tail uses — filter → window functions
/// → offset → sort → distinct → project → limit — so both engines answer an
/// `ORDER BY` / `OFFSET` / `DISTINCT` over a subquery identically. Lite has no shards, so
/// there is no gather step: the body already produces the full row stream.
pub(super) fn lower_subquery<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    args: SubqueryVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let SubqueryVisitArgs {
        input,
        filters,
        projection,
        window_functions,
        sort_keys,
        offset,
        distinct,
        limit,
    } = args;
    let input = input.clone();
    let filters = filters.to_vec();
    let projection = subquery_projection_columns(projection, window_functions)?;
    let window_functions = window_functions.to_vec();
    let sort_keys = sort_keys.to_vec();

    Ok(Box::pin(async move {
        let mut result = engine.execute_plan_admitted(&input, permit).await?;

        filter_rows(&mut result, &filters)?;

        apply_window_functions(&mut result, &window_functions)?;

        sort_rows(&mut result, &sort_keys)?;

        if distinct {
            distinct_rows(&mut result, &projection);
        }

        project_rows(&mut result, &projection);

        // OFFSET skips rows of the sorted result, never of the input.
        if offset > 0 {
            result.rows = result.rows.into_iter().skip(offset).collect();
        }

        if let Some(n) = limit {
            result.rows.truncate(n);
        }

        Ok(result)
    }))
}

/// Lower an outer target list to bare column names. `Star` (and a qualified
/// star) means "inherit the body's columns" and yields an empty list.
///
/// A window call is the column the window pass appended, named by its alias.
/// Any other computed projection is rejected rather than silently dropped:
/// the tail reshapes materialized rows and has no expression evaluator, so
/// the expression must be projected inside the subquery instead.
fn subquery_projection_columns(
    projection: &[Projection],
    window_functions: &[WindowSpec],
) -> Result<Vec<String>, LiteError> {
    let mut names = Vec::with_capacity(projection.len());
    for p in projection {
        match p {
            Projection::Computed { alias, .. }
                if window_functions.iter().any(|spec| spec.alias == *alias) =>
            {
                names.push(alias.clone());
            }
            Projection::Column(qname) => {
                names.push(qname.rsplit('.').next().unwrap_or(qname).to_string());
            }
            Projection::Star | Projection::QualifiedStar(_) => return Ok(Vec::new()),
            Projection::Computed { .. } | Projection::CpComputed { .. } => {
                return Err(LiteError::BadRequest {
                    detail: "a computed projection over an ORDER BY / OFFSET / DISTINCT subquery \
                             is not supported; select the base columns in the subquery and \
                             compute them in an outer SELECT"
                        .into(),
                });
            }
        }
    }
    Ok(names)
}
