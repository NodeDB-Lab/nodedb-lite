// SPDX-License-Identifier: Apache-2.0
//! QueryOp dispatch for the Lite physical visitor.
//!
//! Routes all 13 QueryOp variants. 8 are fully implemented; 5 call writer-B
//! placeholder helpers that return `LiteError::Storage` until writer B lands.

use nodedb_physical::physical_plan::QueryOp;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::query_ops::joins::common::scan_collection;
use crate::query::query_ops::{
    aggregate::{execute_aggregate, execute_partial_aggregate},
    facets::execute_facet_counts,
    joins::{
        broadcast::execute_broadcast_join, hash::execute_hash_join,
        inline_hash::execute_inline_hash_join, nested_loop::execute_nested_loop_join,
        shuffle::execute_shuffle_join, sort_merge::execute_sort_merge_join,
    },
    lateral_loop::execute_lateral_loop,
    lateral_top_k::execute_lateral_top_k,
    recursive_scan::execute_recursive_scan,
    recursive_value::execute_recursive_value,
};
use crate::storage::engine::{StorageEngine, StorageEngineSync};

use super::LitePhysicalFut;

pub(super) fn dispatch<'a, S: StorageEngine + StorageEngineSync + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &QueryOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        QueryOp::Aggregate {
            collection,
            group_by,
            aggregates,
            filters,
            having,
            sort_keys,
            grouping_sets,
            limit: _,
            sub_group_by: _,
            sub_aggregates: _,
        } => {
            let collection = collection.clone();
            let group_by = group_by.clone();
            let aggregates = aggregates.clone();
            let filters = filters.clone();
            let having = having.clone();
            let sort_keys = sort_keys.clone();
            let grouping_sets = grouping_sets.clone();
            Ok(Box::pin(async move {
                let rows = scan_collection(engine, &collection).await?;
                execute_aggregate(
                    rows,
                    &group_by,
                    &aggregates,
                    &filters,
                    &having,
                    &sort_keys,
                    &grouping_sets,
                )
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
                let rows = scan_collection(engine, &collection).await?;
                execute_partial_aggregate(rows, &group_by, &aggregates, &filters)
            }))
        }

        QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            on,
            join_type,
            limit,
            post_group_by,
            post_aggregates,
            projection,
            post_filters,
            inline_left: _,
            inline_right: _,
            inline_left_bitmap: _,
            inline_right_bitmap: _,
        } => {
            let lc = left_collection.clone();
            let rc = right_collection.clone();
            let la = left_alias.clone();
            let ra = right_alias.clone();
            let on = on.clone();
            let jt = join_type.clone();
            let lim = *limit;
            let pg = post_group_by.clone();
            let pa = post_aggregates.clone();
            let proj = projection.clone();
            let pf = post_filters.clone();
            Ok(Box::pin(async move {
                execute_hash_join(
                    engine,
                    &lc,
                    &rc,
                    la.as_deref(),
                    ra.as_deref(),
                    &on,
                    &jt,
                    lim,
                    &pg,
                    &pa,
                    &proj,
                    &pf,
                )
                .await
            }))
        }

        QueryOp::InlineHashJoin {
            left_data,
            right_data,
            right_alias,
            on,
            join_type,
            limit,
            projection,
            post_filters,
        } => {
            let ld = left_data.clone();
            let rd = right_data.clone();
            let ra = right_alias.clone();
            let on = on.clone();
            let jt = join_type.clone();
            let lim = *limit;
            let proj = projection.clone();
            let pf = post_filters.clone();
            Ok(Box::pin(async move {
                execute_inline_hash_join(&ld, &rd, ra.as_deref(), &on, &jt, lim, &proj, &pf)
            }))
        }

        QueryOp::BroadcastJoin {
            large_collection,
            small_collection,
            large_alias,
            small_alias,
            broadcast_data,
            on,
            join_type,
            limit,
            post_group_by,
            post_aggregates,
            projection,
            post_filters,
        } => {
            let lc = large_collection.clone();
            let sc = small_collection.clone();
            let la = large_alias.clone();
            let sa = small_alias.clone();
            let bd = broadcast_data.clone();
            let on = on.clone();
            let jt = join_type.clone();
            let lim = *limit;
            let pg = post_group_by.clone();
            let pa = post_aggregates.clone();
            let proj = projection.clone();
            let pf = post_filters.clone();
            Ok(Box::pin(async move {
                execute_broadcast_join(
                    engine,
                    &lc,
                    &sc,
                    la.as_deref(),
                    sa.as_deref(),
                    &bd,
                    &on,
                    &jt,
                    lim,
                    &pg,
                    &pa,
                    &proj,
                    &pf,
                )
                .await
            }))
        }

        QueryOp::ShuffleJoin {
            left_collection,
            right_collection,
            on,
            join_type,
            limit,
            target_core,
        } => {
            let lc = left_collection.clone();
            let rc = right_collection.clone();
            let on = on.clone();
            let jt = join_type.clone();
            let lim = *limit;
            let tc = *target_core;
            Ok(Box::pin(async move {
                execute_shuffle_join(engine, &lc, &rc, &on, &jt, lim, tc).await
            }))
        }

        QueryOp::NestedLoopJoin {
            left_collection,
            right_collection,
            condition,
            join_type,
            limit,
        } => {
            let lc = left_collection.clone();
            let rc = right_collection.clone();
            let cond = condition.clone();
            let jt = join_type.clone();
            let lim = *limit;
            Ok(Box::pin(async move {
                execute_nested_loop_join(engine, &lc, &rc, &cond, &jt, lim).await
            }))
        }

        QueryOp::SortMergeJoin {
            left_collection,
            right_collection,
            on,
            join_type,
            limit,
            pre_sorted,
        } => {
            let lc = left_collection.clone();
            let rc = right_collection.clone();
            let on = on.clone();
            let jt = join_type.clone();
            let lim = *limit;
            let ps = *pre_sorted;
            Ok(Box::pin(async move {
                execute_sort_merge_join(engine, &lc, &rc, &on, &jt, lim, ps).await
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
                execute_facet_counts(engine, &col, &filt, &fields, lpf).await
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
                execute_recursive_scan(engine, &col, &bf, &rf, jl.as_ref(), mi, dist, lim).await
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
                    engine, &op_clone, &oa, &ic, &inf, &iob, il, &ck, &la, &proj, lj,
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
                execute_lateral_loop(engine, &op_clone, &oa, &ic, &inf, &cp, &la, &proj, lj, orc)
                    .await
            }))
        }
    }
}
