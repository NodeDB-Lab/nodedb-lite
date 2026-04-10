pub mod manager;

pub use manager::FtsCollectionManager;

// Re-export types callers need.
pub use nodedb_fts::FtsIndex;
pub use nodedb_fts::backend::FtsBackend;
pub use nodedb_fts::backend::memory::MemoryBackend;
pub use nodedb_fts::posting::{MatchOffset, Posting, QueryMode, TextSearchResult};

/// Type alias for Lite's in-memory FTS index (no persistence, rebuilt on restart).
pub type LiteFtsIndex = FtsIndex<MemoryBackend>;
