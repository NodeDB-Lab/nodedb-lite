// SPDX-License-Identifier: Apache-2.0

//! Runtime configuration for NodeDB-Lite.
//!
//! [`LiteConfig`] controls memory budget allocation across the embedded engines
//! and the background maintenance intervals. It is designed for future TOML
//! support via `serde`, but can be constructed programmatically or loaded from
//! environment variables via [`LiteConfig::from_env`].
//!
//! Every field is honored by the `NodeDbLite::open*` constructors, including
//! the two that take effect as background tasks (`auto_flush_ms` and
//! `auto_compact_ms`) — the constructors return `Arc<NodeDbLite>` so those
//! tasks can be spawned from the configuration the caller supplied.
//!
//! ## Environment variables
//!
//! | Variable                      | Description                                        | Default |
//! |-------------------------------|----------------------------------------------------|---------|
//! | `NODEDB_LITE_MEMORY_MB`          | Total memory budget in mebibytes                   | 100     |
//! | `NODEDB_LITE_AUTO_FLUSH_MS`      | Auto-flush interval in milliseconds (0 = disabled) | 1000    |
//! | `NODEDB_LITE_AUTO_COMPACT_MS`    | Auto-compact interval in milliseconds (0 = disabled) | 0     |
//! | `NODEDB_LITE_OUTBOUND_QUEUE_CAP` | Max pending entries per durable outbound queue     | 100000  |

pub mod defaults;
pub mod env;
pub mod types;
pub mod validate;

pub use types::LiteConfig;
