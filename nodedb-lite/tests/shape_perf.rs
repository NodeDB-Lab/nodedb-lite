//! Performance test: 1000 active shapes, verify evaluation < 1ms per mutation.
//!
//! Uses the Origin's ShapeRegistry directly (it's in the nodedb crate, but
//! the ShapeDefinition type is shared via nodedb-types). This test verifies
//! the shape evaluation logic performance using the shared types.

use std::time::Instant;

use nodedb_types::sync::shape::{ShapeDefinition, ShapeType};

/// Simulate shape evaluation: check if a mutation matches any of N shapes.
///
/// This mirrors what the Origin's `ShapeRegistry::evaluate_mutation` does.
fn evaluate_mutation_against_shapes(
    shapes: &[ShapeDefinition],
    collection: &str,
    doc_id: &str,
) -> Vec<String> {
    shapes
        .iter()
        .filter(|s| s.could_match(collection, doc_id))
        .map(|s| s.shape_id.clone())
        .collect()
}

#[test]
fn perf_1000_shapes_evaluation_under_1ms() {
    // Create 1000 shapes: mix of document, vector, and graph shapes.
    let shapes: Vec<ShapeDefinition> = (0..1000)
        .map(|i| {
            let shape_type = match i % 3 {
                0 => ShapeType::Document {
                    collection: format!("collection_{}", i % 50),
                    predicate: Vec::new(),
                },
                1 => ShapeType::Vector {
                    collection: format!("collection_{}", i % 50),
                    field_name: None,
                },
                _ => ShapeType::Graph {
                    root_nodes: vec![format!("node_{i}")],
                    max_depth: 2,
                    edge_label: Some("RELATES_TO".into()),
                },
            };
            ShapeDefinition {
                shape_id: format!("shape-{i}"),
                tenant_id: 1,
                shape_type,
                description: format!("shape {i}"),
                field_filter: vec![],
            }
        })
        .collect();

    // Warm up.
    let _ = evaluate_mutation_against_shapes(&shapes, "collection_25", "doc-1");

    // Measure: evaluate one mutation against all 1000 shapes.
    let iterations = 1000;
    let start = Instant::now();
    for i in 0..iterations {
        let coll = format!("collection_{}", i % 50);
        let matches = evaluate_mutation_against_shapes(&shapes, &coll, "doc-1");
        // Each collection has ~20 document shapes + ~20 vector shapes = ~40 matches.
        assert!(!matches.is_empty());
    }
    let elapsed = start.elapsed();
    let per_eval_us = elapsed.as_micros() / iterations as u128;

    // Target: < 1ms (1000us) per evaluation.
    assert!(
        per_eval_us < 1000,
        "shape evaluation took {per_eval_us}us per mutation, target < 1000us"
    );
}

#[test]
fn perf_could_match_is_fast_for_document_shapes() {
    let shape = ShapeDefinition {
        shape_id: "s1".into(),
        tenant_id: 1,
        shape_type: ShapeType::Document {
            collection: "orders".into(),
            predicate: Vec::new(),
        },
        description: "all orders".into(),
        field_filter: vec![],
    };

    let start = Instant::now();
    for _ in 0..100_000 {
        assert!(shape.could_match("orders", "o1"));
        assert!(!shape.could_match("users", "u1"));
    }
    let elapsed = start.elapsed();
    // 100K evaluations should take < 10ms.
    assert!(
        elapsed.as_millis() < 10,
        "100K could_match took {}ms",
        elapsed.as_millis()
    );
}
