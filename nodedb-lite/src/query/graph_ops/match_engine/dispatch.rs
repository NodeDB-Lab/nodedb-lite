// SPDX-License-Identifier: Apache-2.0

//! Entry point for executing a `GraphOp::Match` against the in-memory CSR map.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use nodedb_graph::CsrIndex;
use nodedb_types::SurrogateBitmap;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::engine::crdt::CrdtEngine;
use crate::error::LiteError;

use super::ast::MatchQuery;
use super::executor::{HydrationCtx, execute_query};

/// Execute a `GraphOp::Match` against the Lite CSR map.
///
/// `query_bytes` contains a zerompk-encoded `MatchQuery`. When the collection
/// hint inside the query is absent, the first collection in the map is used.
///
/// `crdt` is optional: when provided, WHERE sub-field predicates (`a.field`)
/// are evaluated by hydrating the bound node's CRDT document. When absent and
/// a predicate references a sub-field, a typed storage error is returned.
pub async fn graph_match(
    csr_map: &Arc<Mutex<HashMap<String, CsrIndex>>>,
    query_bytes: &[u8],
    frontier_bitmap: Option<&SurrogateBitmap>,
    crdt: Option<&Arc<Mutex<CrdtEngine>>>,
) -> Result<QueryResult, LiteError> {
    let query: MatchQuery = zerompk::from_msgpack(query_bytes).map_err(|e| LiteError::Storage {
        detail: format!("deserialize MatchQuery: {e}"),
    })?;

    let map = csr_map.lock().map_err(|_| LiteError::LockPoisoned)?;

    let collection_name = query
        .collection
        .clone()
        .or_else(|| map.keys().next().cloned());

    let Some(ref col) = collection_name else {
        return Ok(QueryResult::empty());
    };

    let Some(csr) = map.get(col) else {
        return Ok(QueryResult::empty());
    };

    let hydration = crdt.map(|c| HydrationCtx {
        crdt: c.as_ref(),
        collection: col.as_str(),
    });
    let rows = execute_query(&query, csr, frontier_bitmap, hydration.as_ref())?;

    let columns: Vec<String> = if query.return_columns.is_empty() {
        query.bound_node_names()
    } else {
        query
            .return_columns
            .iter()
            .map(|rc| rc.alias.clone().unwrap_or_else(|| rc.expr.clone()))
            .collect()
    };

    let result_rows: Vec<Vec<Value>> = rows
        .into_iter()
        .map(|row| {
            columns
                .iter()
                .map(|col_name| {
                    row.get(col_name)
                        .map(|v| Value::String(v.clone()))
                        .unwrap_or(Value::Null)
                })
                .collect()
        })
        .collect();

    Ok(QueryResult {
        columns,
        rows: result_rows,
        rows_affected: 0,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use nodedb_graph::CsrIndex;
    use nodedb_types::value::Value;

    use super::super::ast::*;
    use super::graph_match;

    fn make_csr() -> CsrIndex {
        let mut csr = CsrIndex::new();
        csr.add_edge("alice", "KNOWS", "bob").unwrap();
        csr.add_edge("bob", "KNOWS", "carol").unwrap();
        csr.add_edge("alice", "LIKES", "carol").unwrap();
        csr.add_node_label("alice", "Person").unwrap();
        csr.add_node_label("bob", "Person").unwrap();
        csr.add_node_label("carol", "Person").unwrap();
        csr
    }

    fn make_query_bytes(query: &MatchQuery) -> Vec<u8> {
        zerompk::to_msgpack_vec(query).expect("serialize MatchQuery")
    }

    fn simple_query(
        src_label: Option<&str>,
        edge_type: Option<&str>,
        dst_label: Option<&str>,
        where_eq: Option<(&str, &str)>,
    ) -> MatchQuery {
        let mut predicates = Vec::new();
        if let Some((var, val)) = where_eq {
            predicates.push(WherePredicate::Equals {
                binding: var.to_string(),
                field: var.to_string(),
                value: val.to_string(),
            });
        }
        MatchQuery {
            clauses: vec![MatchClause {
                patterns: vec![PatternChain {
                    triples: vec![PatternTriple {
                        src: NodeBinding {
                            name: Some("a".to_string()),
                            label: src_label.map(str::to_string),
                        },
                        edge: EdgeBinding {
                            name: None,
                            edge_type: edge_type.map(str::to_string),
                            direction: EdgeDirection::Right,
                            min_hops: 1,
                            max_hops: 1,
                        },
                        dst: NodeBinding {
                            name: Some("b".to_string()),
                            label: dst_label.map(str::to_string),
                        },
                    }],
                }],
                optional: false,
            }],
            where_predicates: predicates,
            return_columns: vec![
                ReturnColumn {
                    expr: "a".to_string(),
                    alias: None,
                },
                ReturnColumn {
                    expr: "b".to_string(),
                    alias: None,
                },
            ],
            distinct: false,
            limit: None,
            order_by: Vec::new(),
            collection: Some("col".to_string()),
        }
    }

    fn make_csr_map(csr: CsrIndex) -> Arc<Mutex<HashMap<String, CsrIndex>>> {
        let mut map = HashMap::new();
        map.insert("col".to_string(), csr);
        Arc::new(Mutex::new(map))
    }

    /// Node-label + edge-label filter.
    #[tokio::test]
    async fn match_node_label_and_edge_label() {
        let csr_map = make_csr_map(make_csr());
        let query = simple_query(Some("Person"), Some("KNOWS"), Some("Person"), None);
        let bytes = make_query_bytes(&query);

        let result = graph_match(&csr_map, &bytes, None, None)
            .await
            .expect("graph_match must not error");

        assert_eq!(
            result.rows.len(),
            2,
            "expected 2 KNOWS rows between Persons"
        );
        assert_eq!(result.columns, vec!["a", "b"]);
    }

    /// WHERE a = 'alice' constrains anchor to a single node.
    #[tokio::test]
    async fn match_where_equals_filters_anchor() {
        let csr_map = make_csr_map(make_csr());
        let query = simple_query(Some("Person"), Some("KNOWS"), None, Some(("a", "alice")));
        let bytes = make_query_bytes(&query);

        let result = graph_match(&csr_map, &bytes, None, None)
            .await
            .expect("graph_match must not error");

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("alice".to_string()));
        assert_eq!(result.rows[0][1], Value::String("bob".to_string()));
    }

    /// Pattern with no edge-label filter returns all edge types from anchor.
    #[tokio::test]
    async fn match_no_edge_label_returns_all_edges() {
        let csr_map = make_csr_map(make_csr());
        let query = simple_query(None, None, None, Some(("a", "alice")));
        let bytes = make_query_bytes(&query);

        let result = graph_match(&csr_map, &bytes, None, None)
            .await
            .expect("graph_match must not error");

        // alice KNOWS bob, alice LIKES carol.
        assert_eq!(result.rows.len(), 2);
    }

    /// Frontier bitmap restricts free-variable anchor enumeration.
    #[tokio::test]
    async fn match_frontier_bitmap_restricts_anchors() {
        use nodedb_types::{Surrogate, SurrogateBitmap};

        let mut csr = make_csr();
        csr.set_node_surrogate("alice", Surrogate::new(1));
        csr.set_node_surrogate("bob", Surrogate::new(2));
        csr.set_node_surrogate("carol", Surrogate::new(3));
        let csr_map = make_csr_map(csr);

        let bm = SurrogateBitmap::from_iter([Surrogate::new(1)]);
        let query = simple_query(None, Some("KNOWS"), None, None);
        let bytes = make_query_bytes(&query);

        let result = graph_match(&csr_map, &bytes, Some(&bm), None)
            .await
            .expect("graph_match must not error");

        assert_eq!(result.rows.len(), 1);
        assert_eq!(result.rows[0][0], Value::String("alice".to_string()));
    }

    /// Empty CSR returns empty result, not an error.
    #[tokio::test]
    async fn match_empty_csr_returns_empty() {
        let csr_map = make_csr_map(CsrIndex::new());
        let query = simple_query(None, Some("KNOWS"), None, None);
        let bytes = make_query_bytes(&query);

        let result = graph_match(&csr_map, &bytes, None, None)
            .await
            .expect("empty CSR must not error");

        assert!(result.rows.is_empty());
    }

    /// NOT EXISTS sub-pattern anti-join.
    #[tokio::test]
    async fn match_not_exists_anti_join() {
        let mut csr = CsrIndex::new();
        csr.add_edge("alice", "KNOWS", "bob").unwrap();
        csr.add_edge("bob", "KNOWS", "carol").unwrap();
        csr.add_edge("alice", "BLOCKED", "carol").unwrap();
        let csr_map = make_csr_map(csr);

        let anti = MatchClause {
            patterns: vec![PatternChain {
                triples: vec![PatternTriple {
                    src: NodeBinding {
                        name: Some("a".to_string()),
                        label: None,
                    },
                    edge: EdgeBinding {
                        name: None,
                        edge_type: Some("BLOCKED".to_string()),
                        direction: EdgeDirection::Right,
                        min_hops: 1,
                        max_hops: 1,
                    },
                    dst: NodeBinding {
                        name: Some("b".to_string()),
                        label: None,
                    },
                }],
            }],
            optional: false,
        };

        let query = MatchQuery {
            clauses: vec![MatchClause {
                patterns: vec![PatternChain {
                    triples: vec![PatternTriple {
                        src: NodeBinding {
                            name: Some("a".to_string()),
                            label: None,
                        },
                        edge: EdgeBinding {
                            name: None,
                            edge_type: Some("KNOWS".to_string()),
                            direction: EdgeDirection::Right,
                            min_hops: 1,
                            max_hops: 1,
                        },
                        dst: NodeBinding {
                            name: Some("b".to_string()),
                            label: None,
                        },
                    }],
                }],
                optional: false,
            }],
            where_predicates: vec![WherePredicate::NotExists { sub_pattern: anti }],
            return_columns: vec![
                ReturnColumn {
                    expr: "a".to_string(),
                    alias: None,
                },
                ReturnColumn {
                    expr: "b".to_string(),
                    alias: None,
                },
            ],
            distinct: false,
            limit: None,
            order_by: Vec::new(),
            collection: Some("col".to_string()),
        };

        let bytes = make_query_bytes(&query);
        let result = graph_match(&csr_map, &bytes, None, None)
            .await
            .expect("graph_match must not error");

        // alice->bob passes; bob->carol passes; alice->carol does not exist via KNOWS.
        assert_eq!(
            result.rows.len(),
            2,
            "expected 2 rows without blocked pairs"
        );
    }

    /// WHERE a.name = 'Alice' filters via CRDT document hydration.
    ///
    /// Pre-populate two Person nodes (Alice + Bob) in the CRDT engine. Run a
    /// node-only MATCH with a sub-field WHERE. Verify only Alice is returned.
    #[tokio::test]
    async fn match_where_subfield_crdt_hydration() {
        use crate::engine::crdt::CrdtEngine;
        use std::sync::Mutex;

        // Build a CSR with two Person nodes connected by KNOWS edges so the
        // MATCH clause produces rows for both before the WHERE filters.
        let mut csr = CsrIndex::new();
        csr.add_edge("alice", "KNOWS", "charlie").unwrap();
        csr.add_edge("bob", "KNOWS", "charlie").unwrap();
        csr.add_node_label("alice", "Person").unwrap();
        csr.add_node_label("bob", "Person").unwrap();
        csr.add_node_label("charlie", "Person").unwrap();

        // Populate the CRDT engine with matching documents.
        let mut crdt_engine = CrdtEngine::new(1).expect("CrdtEngine::new");
        crdt_engine
            .upsert(
                "people",
                "alice",
                &[("name", loro::LoroValue::String("Alice".into()))],
            )
            .expect("upsert alice");
        crdt_engine
            .upsert(
                "people",
                "bob",
                &[("name", loro::LoroValue::String("Bob".into()))],
            )
            .expect("upsert bob");
        let crdt = Arc::new(Mutex::new(crdt_engine));

        // CSR map — collection "people".
        let mut csr_map_inner = HashMap::new();
        csr_map_inner.insert("people".to_string(), csr);
        let csr_map = Arc::new(Mutex::new(csr_map_inner));

        // MATCH (a:Person)-[:KNOWS]->(b) WHERE a.name = 'Alice' RETURN a
        // Both alice and bob satisfy the KNOWS pattern (each has one edge to charlie).
        // The WHERE a.name = 'Alice' must hydrate the CRDT doc and keep only alice.
        let query = MatchQuery {
            collection: Some("people".to_string()),
            clauses: vec![MatchClause {
                optional: false,
                patterns: vec![PatternChain {
                    triples: vec![PatternTriple {
                        src: NodeBinding {
                            name: Some("a".to_string()),
                            label: Some("Person".to_string()),
                        },
                        edge: EdgeBinding {
                            name: None,
                            edge_type: Some("KNOWS".to_string()),
                            direction: EdgeDirection::Right,
                            min_hops: 1,
                            max_hops: 1,
                        },
                        dst: NodeBinding {
                            name: Some("b".to_string()),
                            label: None,
                        },
                    }],
                }],
            }],
            where_predicates: vec![WherePredicate::Equals {
                binding: "a".to_string(),
                field: "name".to_string(),
                value: "Alice".to_string(),
            }],
            return_columns: vec![ReturnColumn {
                expr: "a".to_string(),
                alias: None,
            }],
            distinct: true,
            limit: None,
            order_by: Vec::new(),
        };

        let bytes = make_query_bytes(&query);
        let result = graph_match(&csr_map, &bytes, None, Some(&crdt))
            .await
            .expect("graph_match must not error");

        assert_eq!(
            result.rows.len(),
            1,
            "only Alice should pass the WHERE filter"
        );
        assert_eq!(result.rows[0][0], Value::String("alice".to_string()));
    }
}
