// SPDX-License-Identifier: Apache-2.0
//! QueryOp dispatch for the Lite physical visitor.
//!
//! Routes all 16 QueryOp variants. The distributed-only variants (`Exchange`,
//! `ProviderScan`, `PartialAggregateState`, `ShuffleJoinConsume`,
//! `ShuffleAggregateConsume`, `PostProcess`, `SetOp`) have no single-node equivalent and can never be
//! produced by Lite's own planner, so they return `LiteError::Unsupported`
//! defensively if one ever reaches this dispatcher.

use nodedb_physical::physical_plan::QueryOp;
use nodedb_types::RlsWriteCheck;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::query_ops::joins::common::scan_collection;
use crate::query::query_ops::{
    aggregate::{execute_aggregate, execute_partial_aggregate},
    facets::execute_facet_counts,
    joins::{
        hash::execute_hash_join,
        nested_loop::execute_nested_loop_join,
        sort_merge::execute_sort_merge_join,
        spec::{CollectionSide, JoinSpec},
    },
    lateral_loop::{LateralLoopSpec, execute_lateral_loop},
    lateral_top_k::{LateralTopKSpec, execute_lateral_top_k},
    recursive_scan::{RecursiveScanSpec, execute_recursive_scan},
    recursive_value::execute_recursive_value,
};
use crate::storage::engine::StorageEngine;

use super::LitePhysicalFut;
use super::policy::deny_policy;

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &QueryOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        QueryOp::Aggregate {
            collection,
            input,
            group_by,
            aggregates,
            filters,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            grouping_sets,
            sort_keys,
        } => {
            // Lite aggregates the rows of the named collection. An
            // input-sourced aggregate or a nested sub-aggregation would read
            // other rows, so either is refused.
            if input.is_some() {
                return Err(LiteError::Unsupported {
                    detail: "QueryOp::Aggregate over a sub-plan input is not supported in Lite"
                        .into(),
                });
            }
            if !sub_group_by.is_empty() || !sub_aggregates.is_empty() {
                return Err(LiteError::Unsupported {
                    detail: "QueryOp::Aggregate with a sub-aggregation is not supported in Lite"
                        .into(),
                });
            }
            let collection = collection.clone();
            let group_by = group_by.clone();
            let aggregates = aggregates.clone();
            let filters = filters.clone();
            let having = having.clone();
            let limit = *limit;
            let sort_keys = sort_keys.clone();
            let grouping_sets = grouping_sets.clone();
            Ok(Box::pin(async move {
                let rows = scan_collection(engine, collection.as_str()).await?;
                let mut result = execute_aggregate(
                    rows,
                    &group_by,
                    &aggregates,
                    &filters,
                    &having,
                    &sort_keys,
                    &grouping_sets,
                )?;
                result.rows.truncate(limit);
                Ok(result)
            }))
        }

        QueryOp::PartialAggregate {
            collection,
            group_by,
            aggregates,
            filters,
        } => {
            let collection = collection.clone();
            let group_by = group_by.clone();
            let aggregates = aggregates.clone();
            let filters = filters.clone();
            Ok(Box::pin(async move {
                let rows = scan_collection(engine, collection.as_str()).await?;
                execute_partial_aggregate(rows, &group_by, &aggregates, &filters)
            }))
        }

        QueryOp::PartialAggregateState { .. } => Err(LiteError::Unsupported {
            detail: "PartialAggregateState is a distributed shuffle-map op; unsupported on the single-node Lite engine".into(),
        }),

        QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            on,
            join_type,
            limit,
            projection,
            computed_projection,
            join_filters,
            post_filters,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            left_rls_filters,
            right_rls_filters,
            left_scan_filters,
            right_scan_filters,
        } => {
            // HashJoin carries no returning/rls_write_check slots: a join
            // never writes. Only the per-side read filters can hide a policy.
            deny_policy(
                "QueryOp::HashJoin",
                None,
                &[left_rls_filters.as_slice(), right_rls_filters.as_slice()],
                &RlsWriteCheck::NoPolicyApplies,
            )?;
            // Lite's hash join scans each side by collection name and applies
            // only the equi-keys, the post-join filter, and the column
            // projection. Every other stage changes the result, so a plan
            // that carries one is refused.
            let unsupported = [
                (!computed_projection.is_empty(), "a computed projection"),
                (!join_filters.is_empty(), "residual ON predicates"),
                (left_input.is_some() || right_input.is_some(), "a resolved side input"),
                (left_bitmap.is_some() || right_bitmap.is_some(), "a bitmap pushdown"),
                (
                    !left_scan_filters.is_empty() || !right_scan_filters.is_empty(),
                    "per-side scan filters",
                ),
            ];
            if let Some((_, what)) = unsupported.iter().find(|(present, _)| *present) {
                return Err(LiteError::Unsupported {
                    detail: format!("QueryOp::HashJoin with {what} is not supported in Lite"),
                });
            }
            let lc = left_collection.clone();
            let rc = right_collection.clone();
            let la = left_alias.clone();
            let ra = right_alias.clone();
            let on = on.clone();
            let jt = join_type.clone();
            let lim = *limit;
            let proj = projection.clone();
            let pf = post_filters.clone();
            Ok(Box::pin(async move {
                execute_hash_join(
                    engine,
                    CollectionSide {
                        collection: lc.as_str(),
                        alias: la.as_deref(),
                    },
                    CollectionSide {
                        collection: rc.as_str(),
                        alias: ra.as_deref(),
                    },
                    &JoinSpec {
                        on: &on,
                        join_type: &jt,
                        limit: lim,
                        projection: &proj,
                        post_filters: &pf,
                    },
                )
                .await
            }))
        }

        QueryOp::Exchange(_) => Err(LiteError::Unsupported {
            detail: "Exchange is a coordinator-resolved data-movement wrapper; unsupported on the single-node Lite engine".into(),
        }),

        QueryOp::ProviderScan { .. } => Err(LiteError::Unsupported {
            detail: "ProviderScan is coordinator-materialized catalog scan; unsupported on the single-node Lite engine".into(),
        }),

        // PostProcess is the gather-then-relational-tail wrapper the coordinator
        // resolves onto a ProviderScan. Lite never builds one: it lowers
        // `SqlPlan::Subquery` directly in the SQL visitor, where the same
        // filter → offset → sort → distinct → project → limit tail runs against
        // the materialized body.
        QueryOp::PostProcess { .. } => Err(LiteError::Unsupported {
            detail: "PostProcess is a coordinator-resolved subquery tail; unsupported on the single-node Lite engine".into(),
        }),

        // Origin lowers a derived-table set operation to this op so the
        // coordinator can gather sharded children. Lite executes
        // `SqlPlan::{Union, Intersect, Except}` in the SQL visitor
        // (`visitor/set_ops.rs`) and never builds one.
        QueryOp::SetOp { .. } => Err(LiteError::Unsupported {
            detail: "SetOp is a coordinator-resolved derived-table set operation; unsupported on the single-node Lite engine".into(),
        }),

        QueryOp::ShuffleJoinConsume { .. } => Err(LiteError::Unsupported {
            detail: "ShuffleJoinConsume is a cross-node shuffle-join consumer; unsupported on the single-node Lite engine".into(),
        }),

        QueryOp::ShuffleAggregateConsume { .. } => Err(LiteError::Unsupported {
            detail: "ShuffleAggregateConsume is a cross-node shuffle-aggregate consumer; unsupported on the single-node Lite engine".into(),
        }),

        QueryOp::NestedLoopJoin {
            left_collection,
            right_collection,
            condition,
            join_type,
            limit,
            left_rls_filters,
            right_rls_filters,
            left_input,
            right_input,
        } => {
            // Lite's join executors fetch each side by collection name and have
            // no per-side filter stage, so row-level-security filters cannot be
            // applied here. They are refused rather than dropped: ignoring an
            // RLS filter would return rows the caller may not see. Empty
            // filters pass through unchanged.
            if !left_rls_filters.is_empty() || !right_rls_filters.is_empty() {
                return Err(LiteError::Unsupported {
                    detail: "row-level security filters on NestedLoopJoin are not supported on the \
                             Lite engine"
                        .into(),
                });
            }
            // A resolved side carries rows the coordinator read, such as a
            // transaction's staged writes. Lite reads each side by name only.
            if left_input.is_some() || right_input.is_some() {
                return Err(LiteError::Unsupported {
                    detail: "QueryOp::NestedLoopJoin with a resolved side input is not \
                             supported in Lite"
                        .into(),
                });
            }
            let lc = left_collection.clone();
            let rc = right_collection.clone();
            let cond = condition.clone();
            let jt = join_type.clone();
            let lim = *limit;
            Ok(Box::pin(async move {
                execute_nested_loop_join(engine, lc.as_str(), rc.as_str(), &cond, &jt, lim).await
            }))
        }

        QueryOp::SortMergeJoin {
            left_collection,
            right_collection,
            on,
            join_type,
            limit,
            pre_sorted,
            left_rls_filters,
            right_rls_filters,
            left_input,
            right_input,
        } => {
            // See `NestedLoopJoin` above: refuse rather than silently bypass an
            // RLS filter Lite cannot apply.
            if !left_rls_filters.is_empty() || !right_rls_filters.is_empty() {
                return Err(LiteError::Unsupported {
                    detail: "row-level security filters on SortMergeJoin are not supported on the \
                             Lite engine"
                        .into(),
                });
            }
            if left_input.is_some() || right_input.is_some() {
                return Err(LiteError::Unsupported {
                    detail: "QueryOp::SortMergeJoin with a resolved side input is not \
                             supported in Lite"
                        .into(),
                });
            }
            let lc = left_collection.clone();
            let rc = right_collection.clone();
            let on = on.clone();
            let jt = join_type.clone();
            let lim = *limit;
            let ps = *pre_sorted;
            Ok(Box::pin(async move {
                execute_sort_merge_join(engine, lc.as_str(), rc.as_str(), &on, &jt, lim, ps).await
            }))
        }

        QueryOp::FacetCounts {
            collection,
            filters,
            fields,
            limit_per_facet,
        } => {
            let col = collection.clone();
            let filt = filters.clone();
            let fields = fields.clone();
            let lpf = *limit_per_facet;
            Ok(Box::pin(async move {
                execute_facet_counts(engine, col.as_str(), &filt, &fields, lpf).await
            }))
        }

        QueryOp::RecursiveScan {
            collection,
            base_filters,
            recursive_filters,
            join_link,
            max_iterations,
            distinct,
            limit,
        } => {
            let col = collection.clone();
            let bf = base_filters.clone();
            let rf = recursive_filters.clone();
            let jl = join_link.clone();
            let mi = *max_iterations;
            let dist = *distinct;
            let lim = *limit;
            Ok(Box::pin(async move {
                execute_recursive_scan(
                    engine,
                    RecursiveScanSpec {
                        collection: col.as_str(),
                        base_filters: &bf,
                        recursive_filters: &rf,
                        join_link: jl.as_ref(),
                        max_iterations: mi,
                        distinct: dist,
                        limit: lim,
                    },
                )
                .await
            }))
        }

        QueryOp::RecursiveValue {
            cte_name,
            columns,
            init_exprs,
            step_exprs,
            condition,
            max_depth,
            distinct,
        } => {
            let cte = cte_name.clone();
            let cols = columns.clone();
            let init = init_exprs.clone();
            let step = step_exprs.clone();
            let cond = condition.clone();
            let md = *max_depth;
            let dist = *distinct;
            Ok(Box::pin(async move {
                execute_recursive_value(&cte, &cols, &init, &step, cond.as_deref(), md, dist).await
            }))
        }

        QueryOp::LateralTopK {
            outer_plan,
            outer_alias,
            inner_collection,
            inner_filters,
            inner_order_by,
            inner_limit,
            correlation_keys,
            lateral_alias,
            projection,
            left_join,
        } => {
            let op_clone = outer_plan.as_ref().clone();
            let oa = outer_alias.clone();
            let ic = inner_collection.clone();
            let inf = inner_filters.clone();
            let iob = inner_order_by.clone();
            let il = *inner_limit;
            let ck = correlation_keys.clone();
            let la = lateral_alias.clone();
            let proj = projection.clone();
            let lj = *left_join;
            Ok(Box::pin(async move {
                execute_lateral_top_k(
                    engine,
                    &op_clone,
                    LateralTopKSpec {
                        outer_alias: &oa,
                        inner_collection: ic.as_str(),
                        inner_filters: &inf,
                        inner_order_by: &iob,
                        inner_limit: il,
                        correlation_keys: &ck,
                        lateral_alias: &la,
                        projection: &proj,
                        left_join: lj,
                    },
                )
                .await
            }))
        }

        QueryOp::LateralLoop {
            outer_plan,
            outer_alias,
            inner_collection,
            inner_filters,
            correlation_predicates,
            lateral_alias,
            projection,
            left_join,
            outer_row_cap,
        } => {
            let op_clone = outer_plan.as_ref().clone();
            let oa = outer_alias.clone();
            let ic = inner_collection.clone();
            let inf = inner_filters.clone();
            let cp = correlation_predicates.clone();
            let la = lateral_alias.clone();
            let proj = projection.clone();
            let lj = *left_join;
            let orc = *outer_row_cap;
            Ok(Box::pin(async move {
                execute_lateral_loop(
                    engine,
                    &op_clone,
                    LateralLoopSpec {
                        outer_alias: &oa,
                        inner_collection: ic.as_str(),
                        inner_filters: &inf,
                        correlation_predicates: &cp,
                        lateral_alias: &la,
                        projection: &proj,
                        left_join: lj,
                        outer_row_cap: orc,
                    },
                )
                .await
            }))
        }
    }
}
