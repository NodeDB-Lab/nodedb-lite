// SPDX-License-Identifier: Apache-2.0

//! Plan combinators: join/aggregate/union/intersect/except/cte/subquery.

use nodedb_sql::{AggregateVisitArgs, JoinVisitArgs, SubqueryVisitArgs};

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::visitor::queries::{lower_aggregate, lower_cte, lower_join, lower_subquery};
use crate::query::visitor::set_ops::{lower_except, lower_intersect, lower_union};
use crate::storage::engine::StorageEngine;

use super::LiteFut;

pub(super) fn join<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: JoinVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    let JoinVisitArgs {
        left,
        right,
        on,
        join_type,
        condition,
        limit,
        projection,
        filters,
    } = args;
    lower_join(
        engine, left, right, on, join_type, condition, limit, projection, filters,
    )
}

pub(super) fn aggregate<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
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
    lower_aggregate(
        engine,
        input,
        group_by,
        aggregates,
        having,
        limit,
        grouping_sets,
        sort_keys,
    )
}

pub(super) fn union<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    inputs: &[nodedb_sql::types::SqlPlan],
    distinct: bool,
) -> Result<LiteFut<'a>, LiteError> {
    lower_union(engine, inputs, distinct)
}

pub(super) fn intersect<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    left: &nodedb_sql::types::SqlPlan,
    right: &nodedb_sql::types::SqlPlan,
    all: bool,
) -> Result<LiteFut<'a>, LiteError> {
    lower_intersect(engine, left, right, all)
}

pub(super) fn except<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    left: &nodedb_sql::types::SqlPlan,
    right: &nodedb_sql::types::SqlPlan,
    all: bool,
) -> Result<LiteFut<'a>, LiteError> {
    lower_except(engine, left, right, all)
}

pub(super) fn cte<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    definitions: &[(String, nodedb_sql::types::SqlPlan)],
    outer: &nodedb_sql::types::SqlPlan,
) -> Result<LiteFut<'a>, LiteError> {
    lower_cte(engine, definitions, outer)
}

pub(super) fn subquery<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    args: SubqueryVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_subquery(engine, args)
}
