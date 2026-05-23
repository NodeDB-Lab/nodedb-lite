// SPDX-License-Identifier: Apache-2.0
//! KvOp dispatch for the Lite physical visitor.

use nodedb_physical::physical_plan::KvOp;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::query::kv_ops;
use crate::storage::engine::StorageEngine;

use super::LitePhysicalFut;

pub(super) fn dispatch<'a, S: StorageEngine + 'a>(
    engine: &'a LiteQueryEngine<S>,
    op: &KvOp,
) -> Result<LitePhysicalFut<'a>, LiteError> {
    match op {
        KvOp::Get {
            collection,
            key,
            surrogate_ceiling,
            ..
        } => {
            let col = collection.clone();
            let k = key.clone();
            let ceiling = *surrogate_ceiling;
            Ok(Box::pin(async move {
                kv_ops::reads::kv_get(engine, &col, &k, ceiling).await
            }))
        }

        KvOp::Scan {
            collection,
            cursor,
            count,
            match_pattern,
            surrogate_ceiling,
            ..
        } => {
            let col = collection.clone();
            let cur = cursor.clone();
            let cnt = *count;
            let pattern = match_pattern.clone();
            let ceiling = *surrogate_ceiling;
            Ok(Box::pin(async move {
                kv_ops::reads::kv_scan(engine, &col, &cur, cnt, pattern.as_deref(), ceiling).await
            }))
        }

        KvOp::GetTtl { collection, key } => {
            let col = collection.clone();
            let k = key.clone();
            Ok(Box::pin(async move {
                kv_ops::reads::kv_get_ttl(engine, &col, &k).await
            }))
        }

        KvOp::BatchGet { collection, keys } => {
            let col = collection.clone();
            let ks = keys.clone();
            Ok(Box::pin(async move {
                kv_ops::reads::kv_batch_get(engine, &col, &ks).await
            }))
        }

        KvOp::FieldGet {
            collection,
            key,
            fields,
        } => {
            let col = collection.clone();
            let k = key.clone();
            let flds = fields.clone();
            Ok(Box::pin(async move {
                kv_ops::reads::kv_field_get(engine, &col, &k, &flds).await
            }))
        }

        KvOp::MaterializeScan {
            collection,
            cursor,
            count,
        } => {
            let col = collection.clone();
            let cur = cursor.clone();
            let cnt = *count;
            Ok(Box::pin(async move {
                kv_ops::reads::kv_materialize_scan(engine, &col, &cur, cnt, None).await
            }))
        }

        KvOp::Put {
            collection,
            key,
            value,
            ttl_ms,
            ..
        } => {
            let col = collection.clone();
            let k = key.clone();
            let v = value.clone();
            let ttl = *ttl_ms;
            Ok(Box::pin(async move {
                kv_ops::writes::kv_put(engine, &col, &k, &v, ttl).await
            }))
        }

        KvOp::Insert {
            collection,
            key,
            value,
            ttl_ms,
            ..
        } => {
            let col = collection.clone();
            let k = key.clone();
            let v = value.clone();
            let ttl = *ttl_ms;
            Ok(Box::pin(async move {
                kv_ops::writes::kv_insert(engine, &col, &k, &v, ttl).await
            }))
        }

        KvOp::InsertIfAbsent {
            collection,
            key,
            value,
            ttl_ms,
            ..
        } => {
            let col = collection.clone();
            let k = key.clone();
            let v = value.clone();
            let ttl = *ttl_ms;
            Ok(Box::pin(async move {
                kv_ops::writes::kv_insert_if_absent(engine, &col, &k, &v, ttl).await
            }))
        }

        KvOp::InsertOnConflictUpdate {
            collection,
            key,
            value,
            ttl_ms,
            updates,
            ..
        } => {
            let col = collection.clone();
            let k = key.clone();
            let v = value.clone();
            let ttl = *ttl_ms;
            let upd = updates.clone();
            Ok(Box::pin(async move {
                kv_ops::writes::kv_insert_on_conflict_update(engine, &col, &k, &v, ttl, &upd).await
            }))
        }

        KvOp::Delete { collection, keys } => {
            let col = collection.clone();
            let ks = keys.clone();
            Ok(Box::pin(async move {
                kv_ops::writes::kv_delete(engine, &col, &ks).await
            }))
        }

        KvOp::BatchPut {
            collection,
            entries,
            ttl_ms,
        } => {
            let col = collection.clone();
            let ents = entries.clone();
            let ttl = *ttl_ms;
            Ok(Box::pin(async move {
                kv_ops::writes::kv_batch_put(engine, &col, &ents, ttl).await
            }))
        }

        KvOp::Expire {
            collection,
            key,
            ttl_ms,
        } => {
            let col = collection.clone();
            let k = key.clone();
            let ttl = *ttl_ms;
            Ok(Box::pin(async move {
                kv_ops::writes::kv_expire(engine, &col, &k, ttl).await
            }))
        }

        KvOp::Persist { collection, key } => {
            let col = collection.clone();
            let k = key.clone();
            Ok(Box::pin(async move {
                kv_ops::writes::kv_persist(engine, &col, &k).await
            }))
        }

        KvOp::Truncate { collection } => {
            let col = collection.clone();
            Ok(Box::pin(async move {
                kv_ops::writes::kv_truncate(engine, &col).await
            }))
        }

        KvOp::Incr {
            collection,
            key,
            delta,
            ttl_ms,
        } => {
            let col = collection.clone();
            let k = key.clone();
            let d = *delta;
            let ttl = *ttl_ms;
            Ok(Box::pin(async move {
                kv_ops::writes::kv_incr(engine, &col, &k, d, ttl).await
            }))
        }

        KvOp::IncrFloat {
            collection,
            key,
            delta,
        } => {
            let col = collection.clone();
            let k = key.clone();
            let d = *delta;
            Ok(Box::pin(async move {
                kv_ops::writes::kv_incr_float(engine, &col, &k, d).await
            }))
        }

        KvOp::Cas {
            collection,
            key,
            expected,
            new_value,
        } => {
            let col = collection.clone();
            let k = key.clone();
            let exp = expected.clone();
            let nv = new_value.clone();
            Ok(Box::pin(async move {
                kv_ops::writes::kv_cas(engine, &col, &k, &exp, &nv).await
            }))
        }

        KvOp::GetSet {
            collection,
            key,
            new_value,
        } => {
            let col = collection.clone();
            let k = key.clone();
            let nv = new_value.clone();
            Ok(Box::pin(async move {
                kv_ops::writes::kv_get_set(engine, &col, &k, &nv).await
            }))
        }

        KvOp::FieldSet {
            collection,
            key,
            updates,
        } => {
            let col = collection.clone();
            let k = key.clone();
            let upd = updates.clone();
            Ok(Box::pin(async move {
                kv_ops::writes::kv_field_set(engine, &col, &k, &upd).await
            }))
        }

        KvOp::Transfer {
            collection,
            source_key,
            dest_key,
            field,
            amount,
        } => {
            let col = collection.clone();
            let src = source_key.clone();
            let dst = dest_key.clone();
            let fld = field.clone();
            let amt = *amount;
            Ok(Box::pin(async move {
                kv_ops::writes::kv_transfer(engine, &col, &src, &dst, &fld, amt).await
            }))
        }

        KvOp::TransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
        } => {
            let src_col = source_collection.clone();
            let dst_col = dest_collection.clone();
            let ik = item_key.clone();
            let dk = dest_key.clone();
            Ok(Box::pin(async move {
                kv_ops::writes::kv_transfer_item(engine, &src_col, &dst_col, &ik, &dk).await
            }))
        }

        KvOp::RegisterIndex {
            collection,
            field,
            backfill,
            ..
        } => {
            let col = collection.clone();
            let fld = field.clone();
            let bf = *backfill;
            Ok(Box::pin(async move {
                kv_ops::indexes::kv_register_index(engine, &col, &fld, bf).await
            }))
        }

        KvOp::DropIndex { collection, field } => {
            let col = collection.clone();
            let fld = field.clone();
            Ok(Box::pin(async move {
                kv_ops::indexes::kv_drop_index(engine, &col, &fld).await
            }))
        }

        KvOp::RegisterSortedIndex {
            index_name,
            window_type,
            window_timestamp_column,
            window_start_ms,
            window_end_ms,
            ..
        } => {
            let name = index_name.clone();
            let wt = window_type.clone();
            let ts_col = window_timestamp_column.clone();
            let ws = *window_start_ms;
            let we = *window_end_ms;
            Ok(Box::pin(async move {
                kv_ops::sorted::kv_register_sorted_index(engine, &name, &wt, &ts_col, ws, we).await
            }))
        }

        KvOp::DropSortedIndex { index_name } => {
            let name = index_name.clone();
            Ok(Box::pin(async move {
                kv_ops::sorted::kv_drop_sorted_index(engine, &name).await
            }))
        }

        KvOp::SortedIndexRank {
            index_name,
            primary_key,
        } => {
            let name = index_name.clone();
            let pk = primary_key.clone();
            Ok(Box::pin(async move {
                kv_ops::sorted::kv_sorted_index_rank(engine, &name, &pk).await
            }))
        }

        KvOp::SortedIndexTopK { index_name, k } => {
            let name = index_name.clone();
            let k = *k;
            Ok(Box::pin(async move {
                kv_ops::sorted::kv_sorted_index_top_k(engine, &name, k).await
            }))
        }

        KvOp::SortedIndexRange {
            index_name,
            score_min,
            score_max,
        } => {
            let name = index_name.clone();
            let smin = score_min.clone();
            let smax = score_max.clone();
            Ok(Box::pin(async move {
                kv_ops::sorted::kv_sorted_index_range(
                    engine,
                    &name,
                    smin.as_deref(),
                    smax.as_deref(),
                )
                .await
            }))
        }

        KvOp::SortedIndexCount { index_name } => {
            let name = index_name.clone();
            Ok(Box::pin(async move {
                kv_ops::sorted::kv_sorted_index_count(engine, &name).await
            }))
        }

        KvOp::SortedIndexScore {
            index_name,
            primary_key,
        } => {
            let name = index_name.clone();
            let pk = primary_key.clone();
            Ok(Box::pin(async move {
                kv_ops::sorted::kv_sorted_index_score(engine, &name, &pk).await
            }))
        }
    }
}
