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
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    args: JoinVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_join(engine, permit, args)
}

pub(super) fn aggregate<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    args: AggregateVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_aggregate(engine, permit, args)
}

pub(super) fn union<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    inputs: &[nodedb_sql::types::SqlPlan],
    distinct: bool,
) -> Result<LiteFut<'a>, LiteError> {
    lower_union(engine, permit, inputs, distinct)
}

pub(super) fn intersect<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    left: &nodedb_sql::types::SqlPlan,
    right: &nodedb_sql::types::SqlPlan,
    all: bool,
) -> Result<LiteFut<'a>, LiteError> {
    lower_intersect(engine, permit, left, right, all)
}

pub(super) fn except<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    left: &nodedb_sql::types::SqlPlan,
    right: &nodedb_sql::types::SqlPlan,
    all: bool,
) -> Result<LiteFut<'a>, LiteError> {
    lower_except(engine, permit, left, right, all)
}

pub(super) fn cte<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    definitions: &[(String, nodedb_sql::types::SqlPlan)],
    outer: &nodedb_sql::types::SqlPlan,
) -> Result<LiteFut<'a>, LiteError> {
    lower_cte(engine, permit, definitions, outer)
}

pub(super) fn subquery<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    permit: Option<&'a crate::engine::fts::coordinator::TextMutationPermit>,
    args: SubqueryVisitArgs<'_>,
) -> Result<LiteFut<'a>, LiteError> {
    lower_subquery(engine, permit, args)
}
