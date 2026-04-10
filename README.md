# NodeDB-Lite

**All seven [NodeDB](https://github.com/NodeDB-Lab/nodedb) engines as an embedded library.** Vector search, graph traversal, documents, full-text search, timeseries, spatial, and key-value -- all in-process with sub-millisecond reads. No server required. CRDT-based offline-first sync to Origin when connectivity returns.

## When to Use

- Mobile apps that need to work offline
- AI agents that need local memory (vectors + graph + documents)
- Browser-based apps (WASM, ~4.5 MB)
- Desktop applications with local-first data
- IoT gateways with intermittent connectivity

## Platforms

| Platform | Crate              | Backend                 | Size      |
| -------- | ------------------ | ----------------------- | --------- |
| Linux    | `nodedb-lite`      | redb (file-backed)      | Native    |
| macOS    | `nodedb-lite`      | redb (file-backed)      | Native    |
| Windows  | `nodedb-lite`      | redb (file-backed)      | Native    |
| Android  | `nodedb-lite-ffi`  | redb + C FFI + Kotlin/JNI | Native |
| iOS      | `nodedb-lite-ffi`  | redb + C FFI (cbindgen) | Native    |
| Browser  | `nodedb-lite-wasm` | redb (in-memory + OPFS) | ~4.5 MB   |

## Install

```bash
# Rust
cargo add nodedb-lite

# JavaScript / TypeScript (WASM)
npm install @nodedb/lite
```

## Quick Start

```rust
use nodedb_lite::NodeDbLite;
use nodedb_client::NodeDb;

let db = NodeDbLite::open("./my-app-data").await?;

// Insert a document
db.execute("CREATE COLLECTION notes").await?;
db.execute("INSERT INTO notes { title: 'Hello', body: 'World' }").await?;

// Vector search
db.execute("CREATE COLLECTION articles ENGINE vector DIMENSION 384").await?;
db.vector_search("articles", &embedding, 10).await?;

// Graph traversal
db.execute("MATCH (a)-[:KNOWS*1..3]->(b) WHERE a.name = 'Alice' RETURN b").await?;
```

## Same API, Any Runtime

The `NodeDb` trait is identical across Lite and Origin. Application code doesn't change:

```rust
// Works with both NodeDbLite (in-process) and NodeDbRemote (over network)
async fn search(db: &dyn NodeDb, query: &[f32]) -> Result<Vec<Article>> {
    db.vector_search("articles", query, 10).await
}
```

Moving from embedded to server is a connection string change, not a rewrite.

## CRDT Sync

Every write produces a delta. Deltas sync to Origin over WebSocket when online. Multiple devices converge regardless of operation order.

```
Offline:    App writes locally -> Loro generates delta -> delta persisted to redb
Reconnect:  Device opens WebSocket -> sends vector clock + accumulated deltas
Cloud:      Origin validates (RLS, UNIQUE, FK) -> merges -> pushes back missed changes
Conflict:   Rejected deltas -> dead-letter queue + CompensationHint -> device handles
Converged:  Device and cloud share identical Loro state hash
```

- **Shape subscriptions** -- Control what data each device holds: `WHERE user_id = $me`, not the entire database
- **Conflict resolution** -- Declarative per-collection policies. SQL constraints (UNIQUE, FK) enforced on Origin at sync time with typed compensation hints back to the device.
- ACK-based flow control (AIMD), CRC32C delta integrity, JWT token refresh, replay dedup

## Key Features

- **All engines locally** -- Vector, graph, document, FTS, timeseries, spatial, KV -- all in-process, no network
- **Sub-millisecond reads** -- Hot data lives in memory indexes (HNSW, CSR, Loro)
- **Full SQL** -- Same SQL as Origin. Window functions, CTEs, subqueries, JOINs.
- **Encryption at rest** -- AES-256-GCM + Argon2id key derivation
- **Memory governance** -- Per-engine budgets, pressure levels, LRU eviction

## Performance

| Metric                                | Target        |
| ------------------------------------- | ------------- |
| Vector search (1K vectors, 384d, k=5) | < 1ms p99     |
| Graph BFS (10K edges, 2 hops)         | < 1ms p99     |
| Document get                          | < 0.1ms       |
| Cold start (10K vectors + 100K edges) | < 500ms       |
| Sync round-trip (single delta)        | < 200ms       |
| WASM bundle                           | ~4.5 MB       |
| Mobile memory                         | < 100 MB      |

## Workspace

This repository contains three crates:

| Crate              | Description                                              |
| ------------------ | -------------------------------------------------------- |
| `nodedb-lite`      | Core embedded database library                           |
| `nodedb-lite-ffi`  | C FFI bindings for iOS/Android (cbindgen, Kotlin/JNI)    |
| `nodedb-lite-wasm` | JavaScript/TypeScript bindings via wasm-bindgen           |

## Building from Source

```bash
git clone https://github.com/NodeDB-Lab/nodedb-lite.git
cd nodedb-lite

# Build all crates
cargo build --release

# Build WASM
cargo build -p nodedb-lite-wasm --target wasm32-unknown-unknown --release

# Run tests
cargo test
```

For local development against the NodeDB workspace, create `.cargo/config.toml`:

```toml
[patch.crates-io]
nodedb-types = { path = "../nodedb/nodedb-types" }
nodedb-client = { path = "../nodedb/nodedb-client" }
nodedb-codec = { path = "../nodedb/nodedb-codec" }
nodedb-crdt = { path = "../nodedb/nodedb-crdt" }
nodedb-query = { path = "../nodedb/nodedb-query" }
nodedb-spatial = { path = "../nodedb/nodedb-spatial" }
nodedb-graph = { path = "../nodedb/nodedb-graph" }
nodedb-vector = { path = "../nodedb/nodedb-vector" }
nodedb-fts = { path = "../nodedb/nodedb-fts" }
nodedb-strict = { path = "../nodedb/nodedb-strict" }
nodedb-columnar = { path = "../nodedb/nodedb-columnar" }
nodedb-sql = { path = "../nodedb/nodedb-sql" }
```

## License

Apache-2.0. See [LICENSE](LICENSE) for details.
