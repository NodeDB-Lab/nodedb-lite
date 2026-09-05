// SPDX-License-Identifier: Apache-2.0
//! `KvOp` dispatch entry point for the Lite physical visitor.

use nodedb_physical::physical_plan::KvOp;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::super::LitePhysicalFut;
use super::{indexes, reads, unsupported, writes};

pub(crate) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &KvOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        KvOp::Get {
            collection,
            key,
            rls_filters,
            surrogate_ceiling,
        } => reads::get(engine, collection, key, rls_filters, *surrogate_ceiling),

        KvOp::Scan {
            collection,
            cursor,
            count,
            match_pattern,
            surrogate_ceiling,
            ..
        } => reads::scan(
            engine,
            collection,
            cursor,
            *count,
            match_pattern.as_deref(),
            *surrogate_ceiling,
        ),

        KvOp::GetTtl { collection, key } => reads::get_ttl(engine, collection, key),

        KvOp::BatchGet {
            collection,
            keys,
            rls_filters,
        } => reads::batch_get(engine, collection, keys, rls_filters),

        KvOp::FieldGet {
            collection,
            key,
            fields,
            rls_filters,
        } => reads::field_get(engine, collection, key, fields, rls_filters),

        KvOp::MaterializeScan {
            collection,
            cursor,
            count,
        } => reads::materialize_scan(engine, collection, cursor, *count),

        KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
            surrogate: _,
            returning,
            rls_filters,
        } => writes::put(
            engine,
            collection,
            key,
            value,
            *ttl_ms,
            returning,
            rls_filters,
        ),

        KvOp::Insert {
            collection,
            key,
            value,
            ttl_ms,
            surrogate: _,
            returning,
            rls_filters,
        } => writes::insert(
            engine,
            collection,
            key,
            value,
            *ttl_ms,
            returning,
            rls_filters,
        ),

        KvOp::InsertIfAbsent {
            collection,
            key,
            value,
            ttl_ms,
            surrogate: _,
            returning,
            rls_filters,
        } => writes::insert_if_absent(
            engine,
            collection,
            key,
            value,
            *ttl_ms,
            returning,
            rls_filters,
        ),

        KvOp::InsertOnConflictUpdate {
            collection,
            key,
            value,
            ttl_ms,
            updates,
            surrogate: _,
            rls_write_check,
            returning,
            rls_filters,
        } => writes::insert_on_conflict_update(
            engine,
            collection,
            key,
            writes::ConflictUpdateArgs {
                value,
                ttl_ms: *ttl_ms,
                updates,
                rls_write_check,
                returning,
                rls_filters,
            },
        ),

        KvOp::Delete {
            collection,
            keys,
            rls_write_check,
        } => writes::delete(engine, collection, keys, rls_write_check),

        KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
            surrogates: _,
            returning,
            rls_filters,
        } => writes::batch_put(engine, collection, entries, *ttl_ms, returning, rls_filters),

        KvOp::Expire {
            collection,
            key,
            ttl_ms,
            rls_write_check,
        } => writes::expire(engine, collection, key, *ttl_ms, rls_write_check),

        KvOp::Persist {
            collection,
            key,
            rls_write_check,
        } => writes::persist(engine, collection, key, rls_write_check),

        KvOp::Truncate { collection } => writes::truncate(engine, collection),

        KvOp::Incr {
            collection,
            key,
            delta,
            ttl_ms,
            surrogate: _,
            rls_write_check,
        } => writes::incr(engine, collection, key, *delta, *ttl_ms, rls_write_check),

        KvOp::IncrFloat {
            collection,
            key,
            delta,
            surrogate: _,
            rls_write_check,
        } => writes::incr_float(engine, collection, key, *delta, rls_write_check),

        KvOp::Cas {
            collection,
            key,
            expected,
            new_value,
            surrogate: _,
            rls_write_check,
        } => writes::cas(
            engine,
            collection,
            key,
            expected,
            new_value,
            rls_write_check,
        ),

        KvOp::GetSet {
            collection,
            key,
            new_value,
            surrogate: _,
            rls_filters,
            rls_write_check,
        } => writes::get_set(
            engine,
            collection,
            key,
            new_value,
            rls_filters,
            rls_write_check,
        ),

        KvOp::FieldSet {
            collection,
            key,
            updates,
            surrogate: _,
            rls_write_check,
        } => writes::field_set(engine, collection, key, updates, rls_write_check),

        KvOp::Transfer {
            collection,
            source_key,
            dest_key,
            field,
            amount,
            debit_surrogate: _,
            credit_surrogate: _,
            rls_write_check,
        } => writes::transfer(
            engine,
            collection,
            source_key,
            dest_key,
            field,
            *amount,
            rls_write_check,
        ),

        KvOp::TransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            surrogate: _,
            source_rls_write_check,
            dest_rls_write_check,
        } => writes::transfer_item(
            engine,
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            source_rls_write_check,
            dest_rls_write_check,
        ),

        KvOp::RegisterIndex {
            collection,
            field,
            backfill,
            ..
        } => indexes::register_index(engine, collection, field, *backfill),

        KvOp::DropIndex { collection, field } => indexes::drop_index(engine, collection, field),

        KvOp::RegisterSortedIndex {
            index_name,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
            ..
        } => indexes::register_sorted_index(
            engine,
            index_name,
            window_type,
            window_timestamp_column,
            *window_start_ms,
            *window_end_ms,
        ),

        KvOp::DropSortedIndex { index_name } => indexes::drop_sorted_index(engine, index_name),

        KvOp::SortedIndexRank {
            index_name,
            primary_key,
        } => indexes::sorted_index_rank(engine, index_name, primary_key),

        KvOp::SortedIndexTopK { index_name, k } => {
            indexes::sorted_index_top_k(engine, index_name, *k)
        }

        KvOp::SortedIndexRange {
            index_name,
            score_min,
            score_max,
        } => indexes::sorted_index_range(
            engine,
            index_name,
            score_min.as_deref(),
            score_max.as_deref(),
        ),

        KvOp::SortedIndexCount { index_name } => indexes::sorted_index_count(engine, index_name),

        KvOp::SortedIndexScore {
            index_name,
            primary_key,
        } => indexes::sorted_index_score(engine, index_name, primary_key),

        // ResolveWrite/ResolvedWrite are the resolve-before-propose wire
        // shape Origin's cross-vshard write path uses to decide a policy
        // once and replay it identically on every replica. Lite is
        // single-node with no Raft replay, so its SQL visitor and CRDT sync
        // resolve every write directly and never emit these variants.
        KvOp::ResolveWrite(_) => Err(unsupported::resolve_write()),

        KvOp::ResolvedWrite { .. } => Err(unsupported::resolved_write()),

        // PredicateUpdate/PredicateDelete carry a WHERE predicate for the Data
        // Plane to resolve against current state. Lite's SQL visitor always
        // resolves WHERE to an explicit key list before building a KvOp, so it
        // never constructs these variants.
        KvOp::PredicateUpdate { collection, .. } => {
            Err(unsupported::predicate_update(collection.as_str()))
        }

        KvOp::PredicateDelete { collection, .. } => {
            Err(unsupported::predicate_delete(collection.as_str()))
        }
    }
}
