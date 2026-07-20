pub mod analyzer;
pub mod checkpoint;
pub mod manager;
pub mod search;
pub mod state;

pub use manager::FtsCollectionManager;
pub(crate) use search::run_text_search;
pub use state::FtsState;

// Re-export types callers need.
pub use nodedb_fts::FtsIndex;
pub use nodedb_fts::backend::FtsBackend;
pub use nodedb_fts::backend::memory::MemoryBackend;
pub use nodedb_fts::posting::{MatchOffset, Posting, QueryMode, TextSearchResult};

/// Type alias for Lite's persistent FTS index (serialized to KV store on flush).
pub type LiteFtsIndex = FtsIndex<MemoryBackend>;
