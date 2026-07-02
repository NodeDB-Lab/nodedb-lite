//! Sync-layer constants shared across push and drain paths.

/// Maximum number of entries drained from a durable outbound queue per sync
/// push cycle. Keeps each push loop iteration bounded in time and memory.
pub const PUSH_DRAIN_LIMIT: usize = 256;
