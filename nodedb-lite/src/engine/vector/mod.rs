// Re-export shared vector engine from nodedb-vector crate.
// Lite uses the base crate without simd/ivf/collection features.
pub use nodedb_vector::distance;
pub use nodedb_vector::hnsw;
pub use nodedb_vector::hnsw as graph;
pub use nodedb_vector::hnsw::build;
pub use nodedb_vector::hnsw::search;

pub use nodedb_vector::{DistanceMetric, HnswIndex, HnswParams, SearchResult};
