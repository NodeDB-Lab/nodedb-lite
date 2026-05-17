// SPDX-License-Identifier: Apache-2.0

//! Test-only helpers shared by the per-submodule test modules. Builds an
//! in-memory `LiteQueryEngine` with all engine states wired up and a 1-dim,
//! 1-attr AST pair (`dim1_ast` / `attr1_ast`) used by most array tests.

use std::sync::{Arc, Mutex};

use nodedb_sql::types_array::{
    ArrayAttrAst, ArrayAttrType, ArrayDimAst, ArrayDimType, ArrayDomainBound,
};

use crate::engine::array::engine::ArrayEngineState;
use crate::engine::fts::FtsState;
use crate::engine::spatial::SpatialIndexManager;
use crate::engine::vector::VectorState;
use crate::query::engine::LiteQueryEngine;
use crate::storage::redb_storage::RedbStorage;

pub(super) fn make_engine() -> LiteQueryEngine<RedbStorage> {
    let storage = Arc::new(RedbStorage::open_in_memory().expect("in-memory redb"));
    let crdt = Arc::new(Mutex::new(
        crate::engine::crdt::CrdtEngine::new(1).expect("crdt"),
    ));
    let strict = Arc::new(crate::engine::strict::StrictEngine::new(Arc::clone(
        &storage,
    )));
    let columnar = Arc::new(crate::engine::columnar::ColumnarEngine::new(Arc::clone(
        &storage,
    )));
    let htap = Arc::new(crate::engine::htap::HtapBridge::new());
    let timeseries = Arc::new(Mutex::new(
        crate::engine::timeseries::engine::TimeseriesEngine::new(),
    ));
    let vector_state = Arc::new(VectorState::new(Arc::clone(&storage), 100));
    let array_state = Arc::new(Mutex::new(ArrayEngineState::open(&storage).expect("array")));
    let fts_state = Arc::new(FtsState::new());
    let spatial = Arc::new(Mutex::new(SpatialIndexManager::new()));
    LiteQueryEngine::new(
        crdt,
        strict,
        columnar,
        htap,
        storage,
        timeseries,
        vector_state,
        array_state,
        fts_state,
        spatial,
        Arc::new(Mutex::new(std::collections::HashMap::new())),
    )
}

pub(super) fn dim1_ast() -> Vec<ArrayDimAst> {
    vec![ArrayDimAst {
        name: "x".to_string(),
        dtype: ArrayDimType::Int64,
        lo: ArrayDomainBound::Int64(0),
        hi: ArrayDomainBound::Int64(15),
    }]
}

pub(super) fn attr1_ast() -> Vec<ArrayAttrAst> {
    vec![ArrayAttrAst {
        name: "v".to_string(),
        dtype: ArrayAttrType::Int64,
        nullable: false,
    }]
}
