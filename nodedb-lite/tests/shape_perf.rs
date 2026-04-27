//! Correctness tests for `ShapeDefinition::could_match` routing.
//!
//! The original wall-clock `perf_*` tests were migrated to fluxbench
//! benchmarks — see `nodedb-bench/benches/micro/shape_eval.rs`. Wall-clock
//! asserts inside `#[test]` are flaky and don't belong in the test suite.

use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};

fn doc_shape(collection: &str) -> ShapeDefinition {
    ShapeDefinition {
        shape_id: "s1".into(),
        tenant_id: 1,
        shape_type: ShapeType::Document {
            collection: collection.into(),
            predicate: Vec::new(),
        },
        description: format!("all {collection}"),
        field_filter: vec![],
    }
}

#[test]
fn could_match_document_shape_routes_by_collection() {
    let s = doc_shape("orders");
    assert!(s.could_match("orders", "o1"));
    assert!(!s.could_match("users", "u1"));
}
