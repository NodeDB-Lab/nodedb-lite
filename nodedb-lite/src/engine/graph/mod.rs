// Re-export shared graph engine from nodedb-graph crate.
// The core CSR implementation lives in the shared crate.
// Lite-specific persistence (checkpoint via redb) is handled in nodedb/core.rs.
pub use nodedb_graph::csr as index;
pub use nodedb_graph::traversal;

pub use nodedb_graph::{CsrIndex, Direction};
