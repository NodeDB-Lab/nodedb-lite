// SPDX-License-Identifier: Apache-2.0

//! Pattern match executor for Lite.
//!
//! Executes Cypher-subset MATCH queries against the in-memory CSR index.
//!
//! ## Supported patterns
//!
//! - Node bindings with optional labels: `(a:Person)`
//! - Edge bindings with label and direction: `-[:KNOWS]->`
//! - Fixed single-hop patterns and multi-hop chains
//! - Variable-length paths `[:KNOWS*1..N]` via BFS expansion (capped at 10 000 rows per triple)
//! - OPTIONAL MATCH (LEFT JOIN semantics)
//! - WHERE predicates: node equality, numeric/lexicographic comparison, NOT EXISTS sub-pattern,
//!   and sub-field CRDT hydration (`WHERE a.field = 'value'`, `a.age > 30`, `NOT EXISTS(a.email)`)
//! - RETURN column projection, DISTINCT, LIMIT
//! - Frontier bitmap to restrict anchor node enumeration

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use nodedb_graph::CsrIndex;
use nodedb_types::SurrogateBitmap;

use crate::engine::crdt::CrdtEngine;
use crate::error::LiteError;

use super::ast::{MatchClause, MatchQuery, NodeBinding, PatternChain, PatternTriple};
use super::predicates::apply_predicate;

/// A single result row: variable name → node-id string.
pub(super) type BindingRow = HashMap<String, String>;

/// Context for CRDT sub-field hydration.
pub(super) struct HydrationCtx<'a> {
    pub crdt: &'a Mutex<CrdtEngine>,
    pub collection: &'a str,
}

/// Execute a deserialized `MatchQuery` against a `CsrIndex`.
pub(super) fn execute_query(
    query: &MatchQuery,
    csr: &CsrIndex,
    frontier_bitmap: Option<&SurrogateBitmap>,
    hydration: Option<&HydrationCtx<'_>>,
) -> Result<Vec<BindingRow>, LiteError> {
    let mut rows: Vec<BindingRow> = vec![HashMap::new()];

    for clause in &query.clauses {
        let clause_rows = execute_clause(clause, csr, &rows, frontier_bitmap);
        if clause.optional {
            rows = left_join_rows(&rows, &clause_rows, clause);
        } else {
            rows = clause_rows;
        }
    }

    for predicate in &query.where_predicates {
        rows = apply_predicate(rows, predicate, csr, frontier_bitmap, hydration)?;
    }

    if !query.return_columns.is_empty() {
        let col_exprs: Vec<&str> = query
            .return_columns
            .iter()
            .map(|rc| rc.expr.as_str())
            .collect();
        rows = project_columns(rows, &col_exprs);
    }

    if query.distinct {
        let mut seen: HashSet<String> = HashSet::new();
        rows.retain(|row| {
            let key = format!("{row:?}");
            seen.insert(key)
        });
    }

    if let Some(limit) = query.limit {
        rows.truncate(limit);
    }

    Ok(rows)
}

pub(super) fn execute_clause(
    clause: &MatchClause,
    csr: &CsrIndex,
    input_rows: &[BindingRow],
    frontier_bitmap: Option<&SurrogateBitmap>,
) -> Vec<BindingRow> {
    let mut result_rows = input_rows.to_vec();
    for chain in &clause.patterns {
        let mut next_rows = Vec::new();
        for row in &result_rows {
            next_rows.extend(execute_chain(chain, csr, row, frontier_bitmap));
        }
        result_rows = next_rows;
    }
    result_rows
}

fn execute_chain(
    chain: &PatternChain,
    csr: &CsrIndex,
    input_row: &BindingRow,
    frontier_bitmap: Option<&SurrogateBitmap>,
) -> Vec<BindingRow> {
    let mut rows = vec![input_row.clone()];
    for triple in &chain.triples {
        let mut next_rows = Vec::new();
        for row in &rows {
            next_rows.extend(execute_triple(triple, csr, row, frontier_bitmap));
        }
        rows = next_rows;
        if rows.is_empty() {
            break;
        }
    }
    rows
}

fn execute_triple(
    triple: &PatternTriple,
    csr: &CsrIndex,
    input_row: &BindingRow,
    frontier_bitmap: Option<&SurrogateBitmap>,
) -> Vec<BindingRow> {
    let direction = triple.edge.to_direction();
    let label_filter = triple.edge.edge_type.as_deref();
    let src_nodes = resolve_binding(&triple.src, csr, input_row, frontier_bitmap);

    if src_nodes.is_empty() {
        return Vec::new();
    }

    let mut results = Vec::new();

    if triple.edge.is_variable_length() {
        for src_name in &src_nodes {
            let expanded = csr.traverse_bfs(
                &[src_name.as_str()],
                label_filter,
                direction,
                triple.edge.max_hops,
                50_000,
                frontier_bitmap,
            );
            for dst_name in expanded {
                if dst_name == *src_name && triple.edge.min_hops > 0 {
                    continue;
                }
                let Some(dst_raw) = csr.node_id_raw(&dst_name) else {
                    continue;
                };
                if !binding_compatible(&triple.dst, csr, input_row, dst_raw) {
                    continue;
                }
                let mut row = input_row.clone();
                bind_node(&mut row, &triple.src, src_name);
                bind_node(&mut row, &triple.dst, &dst_name);
                if let Some(ref ename) = triple.edge.name {
                    row.insert(
                        ename.clone(),
                        format!("{src_name}|{}|{dst_name}", label_filter.unwrap_or("*")),
                    );
                }
                results.push(row);
                if results.len() >= 10_000 {
                    return results;
                }
            }
        }
    } else {
        for src_name in &src_nodes {
            let neighbors = csr.neighbors(src_name, label_filter, direction);
            for (label_name, dst_name) in neighbors {
                let Some(dst_raw) = csr.node_id_raw(&dst_name) else {
                    continue;
                };
                if !binding_compatible(&triple.dst, csr, input_row, dst_raw) {
                    continue;
                }
                let mut row = input_row.clone();
                bind_node(&mut row, &triple.src, src_name);
                bind_node(&mut row, &triple.dst, &dst_name);
                if let Some(ref ename) = triple.edge.name {
                    row.insert(ename.clone(), format!("{src_name}|{label_name}|{dst_name}"));
                }
                results.push(row);
            }
        }
    }

    results
}

fn resolve_binding(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    frontier_bitmap: Option<&SurrogateBitmap>,
) -> Vec<String> {
    if let Some(ref name) = binding.name
        && let Some(bound_val) = row.get(name)
    {
        let Some(raw) = csr.node_id_raw(bound_val) else {
            return Vec::new();
        };
        if let Some(ref lbl) = binding.label
            && !csr.node_has_label(raw, lbl)
        {
            return Vec::new();
        }
        return vec![bound_val.clone()];
    }
    (0..csr.node_count() as u32)
        .filter(|&id| {
            let label_ok = binding
                .label
                .as_ref()
                .is_none_or(|l| csr.node_has_label(id, l));
            let bitmap_ok = frontier_bitmap.is_none_or(|bm| {
                bm.contains(nodedb_types::Surrogate::new(csr.node_surrogate_raw(id)))
            });
            label_ok && bitmap_ok
        })
        .map(|id| csr.node_name_raw(id).to_string())
        .collect()
}

fn binding_compatible(
    binding: &NodeBinding,
    csr: &CsrIndex,
    row: &BindingRow,
    node_id: u32,
) -> bool {
    if let Some(ref lbl) = binding.label
        && !csr.node_has_label(node_id, lbl)
    {
        return false;
    }
    if let Some(ref name) = binding.name
        && let Some(existing) = row.get(name)
    {
        return existing == csr.node_name_raw(node_id);
    }
    true
}

fn bind_node(row: &mut BindingRow, binding: &NodeBinding, node_name: &str) {
    if let Some(ref name) = binding.name {
        row.entry(name.clone())
            .or_insert_with(|| node_name.to_string());
    }
}

pub(super) fn project_columns(rows: Vec<BindingRow>, columns: &[&str]) -> Vec<BindingRow> {
    rows.into_iter()
        .map(|row| {
            columns
                .iter()
                .filter_map(|col| {
                    let key = col.split('.').next().unwrap_or(col);
                    row.get(key).map(|v| (key.to_string(), v.clone()))
                })
                .collect()
        })
        .collect()
}

pub(super) fn left_join_rows(
    input: &[BindingRow],
    clause_rows: &[BindingRow],
    clause: &MatchClause,
) -> Vec<BindingRow> {
    let new_vars: Vec<String> = clause
        .patterns
        .iter()
        .flat_map(|chain| {
            chain.triples.iter().flat_map(|t| {
                let mut vars = Vec::new();
                if let Some(ref n) = t.src.name {
                    vars.push(n.clone());
                }
                if let Some(ref n) = t.dst.name {
                    vars.push(n.clone());
                }
                if let Some(ref n) = t.edge.name {
                    vars.push(n.clone());
                }
                vars
            })
        })
        .collect();

    let mut result = Vec::new();
    for input_row in input {
        let matches: Vec<&BindingRow> = clause_rows
            .iter()
            .filter(|cr| {
                input_row
                    .iter()
                    .all(|(k, v)| cr.get(k).is_none_or(|cv| cv == v))
            })
            .collect();

        if matches.is_empty() {
            let mut row = input_row.clone();
            for var in &new_vars {
                row.entry(var.clone()).or_insert_with(|| "NULL".to_string());
            }
            result.push(row);
        } else {
            result.extend(matches.into_iter().cloned());
        }
    }
    result
}
