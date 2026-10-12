// SPDX-License-Identifier: Apache-2.0

//! The rows a text read admits: its residual filters and RLS filters,
//! evaluated against each stored row of the collection.
//!
//! Both filter slots of a `TextOp` carry zerompk `Vec<ScanFilter>` bytes.
//! Empty bytes admit every row. A row is the CRDT document of the
//! collection. A document with no stored row is not admitted when a filter
//! applies.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use nodedb_query::scan_filter::ScanFilter;
use nodedb_types::value::Value;

use crate::engine::crdt::CrdtEngine;
use crate::error::LiteError;
use crate::nodedb::convert::loro_value_to_document;
use crate::nodedb::lock_ext::LockExt;
use crate::query::scan_filter_convert::decode_scan_filters;

/// One stored row: its document id and fields.
pub(super) type TextRow = (String, HashMap<String, Value>);

/// The residual filters and RLS filters of one text read, as one AND-group.
pub(super) struct TextRowGate {
    filters: Vec<ScanFilter>,
}

impl TextRowGate {
    /// Decode both filter slots of a text op.
    pub(super) fn new(filters: &[u8], rls_filters: &[u8]) -> Result<Self, LiteError> {
        let mut all = decode_scan_filters(filters)?;
        all.extend(decode_scan_filters(rls_filters)?);
        Ok(Self { filters: all })
    }

    /// Whether the gate admits every row.
    pub(super) fn is_open(&self) -> bool {
        self.filters.is_empty()
    }

    /// Whether `row` passes every filter. An evaluation error, such as a
    /// division by zero, fails the read.
    pub(super) fn admits(&self, row: &Value) -> Result<bool, LiteError> {
        Ok(ScanFilter::all_match_value(&self.filters, row)?)
    }
}

/// The document id of every stored row of `collection`.
pub(super) fn collection_ids(crdt: &Mutex<CrdtEngine>, collection: &str) -> Vec<String> {
    crdt.lock_or_recover().list_ids(collection)
}

/// Every stored row of `collection`.
pub(super) fn collection_rows(crdt: &Mutex<CrdtEngine>, collection: &str) -> Vec<TextRow> {
    let guard = crdt.lock_or_recover();
    guard
        .list_ids(collection)
        .into_iter()
        .filter_map(|id| {
            let value = guard.read(collection, &id)?;
            let fields = loro_value_to_document(&id, &value).fields;
            Some((id, fields))
        })
        .collect()
}

/// The ids of the stored rows of `collection` the gate admits. `None` when
/// the gate is open: every document is eligible.
pub(super) fn admitted_ids(
    crdt: &Mutex<CrdtEngine>,
    collection: &str,
    gate: &TextRowGate,
) -> Result<Option<HashSet<String>>, LiteError> {
    if gate.is_open() {
        return Ok(None);
    }
    let mut admitted = HashSet::new();
    for (id, fields) in collection_rows(crdt, collection) {
        if gate.admits(&Value::Object(fields))? {
            admitted.insert(id);
        }
    }
    Ok(Some(admitted))
}

/// Whether `id` is eligible under the result of [`admitted_ids`].
pub(super) fn is_eligible(admitted: Option<&HashSet<String>>, id: &str) -> bool {
    admitted.is_none_or(|ids| ids.contains(id))
}
