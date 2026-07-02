// SPDX-License-Identifier: Apache-2.0

//! Mirror AST types for `MatchQuery`.
//!
//! These must have the same MessagePack wire format as the types in
//! `nodedb/src/engine/graph/pattern/ast.rs`. The `zerompk` codec uses
//! positional struct field encoding and the `#[msgpack(c_enum)]` tag for
//! C-like enums — binary compatible as long as field order and discriminant
//! values match.

#[derive(Debug, Clone, zerompk::FromMessagePack, zerompk::ToMessagePack)]
pub(crate) struct MatchQuery {
    pub clauses: Vec<MatchClause>,
    pub where_predicates: Vec<WherePredicate>,
    pub return_columns: Vec<ReturnColumn>,
    pub distinct: bool,
    pub limit: Option<usize>,
    pub order_by: Vec<OrderByColumn>,
    pub collection: Option<String>,
}

#[derive(Debug, Clone, zerompk::FromMessagePack, zerompk::ToMessagePack)]
pub(crate) struct MatchClause {
    pub patterns: Vec<PatternChain>,
    pub optional: bool,
}

#[derive(Debug, Clone, zerompk::FromMessagePack, zerompk::ToMessagePack)]
pub(crate) struct PatternChain {
    pub triples: Vec<PatternTriple>,
}

#[derive(Debug, Clone, zerompk::FromMessagePack, zerompk::ToMessagePack)]
pub(crate) struct PatternTriple {
    pub src: NodeBinding,
    pub edge: EdgeBinding,
    pub dst: NodeBinding,
}

#[derive(Debug, Clone, zerompk::FromMessagePack, zerompk::ToMessagePack)]
pub(crate) struct NodeBinding {
    pub name: Option<String>,
    pub label: Option<String>,
}

#[derive(Debug, Clone, zerompk::FromMessagePack, zerompk::ToMessagePack)]
pub(crate) struct EdgeBinding {
    pub name: Option<String>,
    pub edge_type: Option<String>,
    pub direction: EdgeDirection,
    pub min_hops: usize,
    pub max_hops: usize,
}

impl EdgeBinding {
    pub(crate) fn is_variable_length(&self) -> bool {
        self.min_hops != self.max_hops || self.min_hops > 1
    }

    pub(crate) fn to_direction(&self) -> nodedb_graph::Direction {
        match self.direction {
            EdgeDirection::Right => nodedb_graph::Direction::Out,
            EdgeDirection::Left => nodedb_graph::Direction::In,
            EdgeDirection::Both => nodedb_graph::Direction::Both,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::FromMessagePack, zerompk::ToMessagePack)]
#[repr(u8)]
#[msgpack(c_enum)]
pub(crate) enum EdgeDirection {
    Right = 0,
    Left = 1,
    Both = 2,
}

#[derive(Debug, Clone, zerompk::FromMessagePack, zerompk::ToMessagePack)]
pub(crate) enum WherePredicate {
    /// `WHERE a = 'value'` — match the node-id of variable `a`.
    Equals {
        binding: String,
        field: String,
        value: String,
    },
    /// `WHERE a.age > 25`.
    Comparison {
        binding: String,
        field: String,
        op: ComparisonOp,
        value: String,
    },
    /// `WHERE NOT EXISTS { MATCH ... }`.
    NotExists { sub_pattern: MatchClause },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::FromMessagePack, zerompk::ToMessagePack)]
#[repr(u8)]
#[msgpack(c_enum)]
pub(crate) enum ComparisonOp {
    Eq = 0,
    Neq = 1,
    Lt = 2,
    Lte = 3,
    Gt = 4,
    Gte = 5,
}

#[derive(Debug, Clone, zerompk::FromMessagePack, zerompk::ToMessagePack)]
pub(crate) struct ReturnColumn {
    pub expr: String,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, zerompk::FromMessagePack, zerompk::ToMessagePack)]
pub(crate) struct OrderByColumn {
    pub expr: String,
    pub ascending: bool,
}

impl MatchQuery {
    /// All unique node variable names bound across all clauses, in discovery order.
    pub(crate) fn bound_node_names(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for clause in &self.clauses {
            for chain in &clause.patterns {
                for triple in &chain.triples {
                    if let Some(ref n) = triple.src.name
                        && !names.contains(n)
                    {
                        names.push(n.clone());
                    }
                    if let Some(ref n) = triple.dst.name
                        && !names.contains(n)
                    {
                        names.push(n.clone());
                    }
                }
            }
        }
        names
    }
}
