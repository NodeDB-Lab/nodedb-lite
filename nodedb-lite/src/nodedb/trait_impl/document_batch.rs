// SPDX-License-Identifier: Apache-2.0

//! Batch document + vector ingest for `NodeDbLite`.
//!
//! `document_put_with_vector_batch_impl` takes a slice of
//! `(doc_collection, doc, vector_collection, id, embedding)` items and
//! acquires the CRDT lock exactly once, performs all CRDT mutations under
//! that single lock, then calls `export(updates_since(version_before))`
//! once to produce a single pending delta covering the whole batch.
//!
//! FTS indexing, bitemporal history writes, and HNSW inserts run after
//! the CRDT lock is released — matching the ordering of the single-item path.

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};

use crate::engine::crdt::engine::CrdtBatchOp;
use crate::engine::document::history::ops::{is_bitemporal, versioned_put};
use crate::engine::vector::sidecar;
use crate::engine::vector::state::ensure_hnsw;
use crate::nodedb::LockExt;
use crate::nodedb::NodeDbLite;
use crate::nodedb::convert::{document_to_msgpack, value_to_loro};
use crate::runtime::now_millis_i64;
use crate::storage::engine::StorageEngine;
use nodedb_types::vector_dtype::VectorStorageDtype;

/// One item in a batch ingest call.
pub struct BatchItem<'a> {
    pub doc_collection: &'a str,
    pub doc: Document,
    pub vector_collection: &'a str,
    pub id: &'a str,
    pub embedding: Option<&'a [f32]>,
}

/// A list of CRDT fields for one upsert: borrowed field name → Loro value.
type LoroFields<'a> = Vec<(&'a str, loro::LoroValue)>;

/// A batch item resolved before the CRDT lock is taken:
/// `(document id, document fields, vector-metadata fields)`.
type ResolvedBatchItem<'a> = (String, LoroFields<'a>, LoroFields<'a>);

impl<S: StorageEngine> NodeDbLite<S> {
    /// Batch upsert of documents with optional embeddings.
    ///
    /// Acquires the CRDT lock once, runs all per-item Loro mutations under
    /// that lock, then calls `export(updates_since(version_before))` once.
    /// FTS indexing, bitemporal history, and HNSW inserts happen after the
    /// lock is released, in the same relative order as the single-item path.
    ///
    /// Returns the list of document IDs written, in input order.
    pub async fn document_put_with_vector_batch_impl(
        &self,
        items: &[BatchItem<'_>],
    ) -> NodeDbResult<Vec<String>> {
        if items.is_empty() {
            return Ok(Vec::new());
        }

        // Reject the whole batch up front under critical memory pressure, matching
        // the single-item `document_put_impl` / `vector_insert_impl` guard. A batch
        // can ingest many documents plus embeddings at once, so the early gate is
        // even more important here than on the single-item path.
        if self.governor.pressure() == crate::memory::PressureLevel::Critical {
            return Err(NodeDbError::storage(
                crate::error::LiteError::Backpressure {
                    detail: "batch ingest rejected: memory governor is at Critical pressure".into(),
                },
            ));
        }

        // Pre-compute doc IDs and field vecs before taking the lock.
        let mut resolved: Vec<ResolvedBatchItem<'_>> = Vec::with_capacity(items.len());

        for item in items {
            let doc_id = if item.doc.id.is_empty() {
                nodedb_types::id_gen::uuid_v7()
            } else {
                item.doc.id.clone()
            };

            let doc_fields: Vec<(&str, loro::LoroValue)> = item
                .doc
                .fields
                .iter()
                .map(|(k, v)| (k.as_str(), value_to_loro(v)))
                .collect();

            let vec_fields: Vec<(&str, loro::LoroValue)> = match item.embedding {
                Some(emb) if !emb.is_empty() => {
                    vec![("embedding_dim", loro::LoroValue::I64(emb.len() as i64))]
                }
                _ => vec![],
            };

            resolved.push((doc_id, doc_fields, vec_fields));
        }

        // Build the ops slice for batch_upsert — one CRDT lock, one export.
        {
            let mut crdt = self.crdt.lock_or_recover();

            let mut ops: Vec<CrdtBatchOp<'_>> = Vec::with_capacity(items.len() * 2);
            for (i, item) in items.iter().enumerate() {
                let (ref doc_id, ref doc_fields, ref vec_fields) = resolved[i];
                ops.push((item.doc_collection, doc_id.as_str(), doc_fields.as_slice()));
                if !vec_fields.is_empty() {
                    ops.push((item.vector_collection, item.id, vec_fields.as_slice()));
                }
            }

            crdt.batch_upsert(&ops).map_err(NodeDbError::storage)?;
        }

        // Post-lock work: bitemporal history + FTS + HNSW (matches single-item ordering).
        let now_ms = now_millis_i64();

        for (i, item) in items.iter().enumerate() {
            let (ref doc_id, _, _) = resolved[i];

            if is_bitemporal(&*self.storage, item.doc_collection)
                .await
                .map_err(NodeDbError::storage)?
            {
                let body = document_to_msgpack(&item.doc);
                versioned_put(
                    &*self.storage,
                    item.doc_collection,
                    doc_id,
                    &body,
                    now_ms,
                    None,
                    None,
                )
                .await
                .map_err(NodeDbError::storage)?;
            }

            self.index_document_text(item.doc_collection, doc_id, &item.doc.fields);
            self.index_document_sparse(item.doc_collection, doc_id, &item.doc.fields);

            if let Some(embedding) = item.embedding
                && !embedding.is_empty()
            {
                let internal_id = {
                    let dtype = {
                        let configs = self.vector_state.per_index_config.lock_or_recover();
                        configs
                            .get(item.vector_collection)
                            .map(|cfg| cfg.storage_dtype)
                            .unwrap_or(VectorStorageDtype::F32)
                    };
                    let mut indices = self.vector_state.hnsw_indices.lock_or_recover();
                    let index =
                        ensure_hnsw(&mut indices, item.vector_collection, embedding.len(), dtype);
                    let id_before = index.len() as u32;
                    index
                        .insert(embedding.to_vec())
                        .map_err(NodeDbError::bad_request)?;
                    id_before
                };

                {
                    let mut id_map = self.vector_state.vector_id_map.lock_or_recover();
                    id_map.insert(
                        format!("{}:{internal_id}", item.vector_collection),
                        (item.id.to_string(), internal_id),
                    );
                }

                match sidecar::ensure_sidecar(&self.vector_state, item.vector_collection) {
                    Ok(true) => {
                        let mut sidecars = self.vector_state.codec_sidecars.lock_or_recover();
                        if let Some(s) = sidecars.get_mut(item.vector_collection)
                            && let Err(e) = s.encode_and_insert(internal_id, embedding)
                        {
                            tracing::warn!(
                                index_key = item.vector_collection,
                                id = internal_id,
                                error = %e,
                                "sidecar encode_and_insert failed; row falls back to FP32 rerank"
                            );
                        }
                    }
                    Ok(false) => {}
                    Err(e) => return Err(NodeDbError::bad_request(e.to_string())),
                }

                #[cfg(not(target_arch = "wasm32"))]
                if let Some(q) = &self.vector_outbound {
                    crate::sync::reconcile_outbound_enqueue(
                        q.enqueue_insert(
                            item.vector_collection,
                            item.id,
                            embedding.to_vec(),
                            embedding.len(),
                            "",
                        )
                        .await,
                        "vector insert (batch)",
                        item.vector_collection,
                        item.id,
                    )
                    .map_err(nodedb_types::error::NodeDbError::storage)?;
                }
            }
        }

        if items
            .iter()
            .any(|it| it.embedding.is_some_and(|e| !e.is_empty()))
        {
            self.update_memory_stats();
        }

        Ok(resolved.into_iter().map(|(id, _, _)| id).collect())
    }
}
