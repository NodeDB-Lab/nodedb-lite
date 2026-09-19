// SPDX-License-Identifier: Apache-2.0

//! KV DML: kv_insert.

use nodedb_sql::types::SqlValue;
use nodedb_sql::types::plan::KvInsertIntent;
use nodedb_sql::types_expr::SqlExpr;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::visitor::kv::lower_kv_insert;
use crate::storage::engine::StorageEngine;

use super::LiteFut;

pub(super) fn kv_insert<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    collection: &str,
    entries: &[(SqlValue, Vec<(String, SqlValue)>)],
    ttl_secs: u64,
    intent: KvInsertIntent,
    on_conflict_updates: &[(String, SqlExpr)],
) -> Result<LiteFut<'a>, LiteError> {
    lower_kv_insert(
        engine,
        collection,
        entries,
        ttl_secs,
        intent,
        on_conflict_updates,
    )
}
