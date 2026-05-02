<div align="center">

<img src="assets/wordmark.svg" alt="NodeDB" width="420">

# NodeDB Lite

<h3>The embedded multi-model database for local-first apps, agents, and edge runtimes.</h3>

<p>
  <a href="https://github.com/NodeDB-Lab/nodedb">NodeDB</a> engines in-process. One API.
  Zero server requirement. Run vector search, graph traversal, document queries, full-text search,
  timeseries, and other multi-model workloads on device, then sync to Origin when connectivity returns.
</p>

<p>
  <a href="#status"><strong>Status</strong></a>
  ·
  <a href="#platforms"><strong>Platforms</strong></a>
  ·
  <a href="#crdt-sync"><strong>CRDT Sync</strong></a>
  ·
  <a href="#performance"><strong>Performance</strong></a>
  ·
  <a href="https://github.com/NodeDB-Lab/nodedb"><strong>NodeDB Origin</strong></a>
</p>

<p align="center">
  <a href="https://discord.gg/s54gDMVc7B">
    <img src="assets/discord-cta.svg" alt="Join the NodeDB Discord" width="340">
  </a>
</p>

<p>
  <a href="https://github.com/NodeDB-Lab/nodedb-lite/actions/workflows/ci.yml">
    <img src="https://img.shields.io/github/actions/workflow/status/NodeDB-Lab/nodedb-lite/ci.yml?branch=main&label=ci" alt="CI status">
  </a>
  <img src="https://img.shields.io/badge/status-in%20development-orange" alt="Status: in development">
  <a href="https://github.com/NodeDB-Lab/nodedb-lite/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License">
  </a>
  <a href="https://github.com/NodeDB-Lab/nodedb-lite/stargazers">
    <img src="https://img.shields.io/github/stars/NodeDB-Lab/nodedb-lite?style=social" alt="GitHub stars">
  </a>
</p>

</div>

NodeDB Lite replaces the usual SQLite + vector sidecar + ad hoc cache + custom sync layer stack with one embedded engine. Local reads stay in-process, writes remain available offline, and the same application code can later sync to NodeDB Origin without a rewrite.

## Status

NodeDB Lite is currently in development and is not released yet.

The immediate focus is NodeDB Origin through `v0.1.0`. Once Origin reaches `v0.1.0`, development focus shifts back to NodeDB Lite for packaging, platform hardening, and release work.

Until then, this repository should be treated as active development, not a published/stable product.

## Why NodeDB Lite

- **One embedded engine, not a stitched-together client stack.** Vectors, graph, documents, full-text, timeseries, key-value, and other NodeDB data models run in one runtime with shared storage and one query surface.
- **Built for offline-first.** Every write is captured as a CRDT delta locally, then merged to Origin when the network comes back.
- **Same API as Origin.** The `NodeDb` trait is identical across Lite and server deployments, so moving from on-device to remote is a connection decision, not an architecture rewrite.
- **Edge-ready.** Linux, macOS, Windows, Android, iOS, and browser/WASM support from the same product line.

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

## Planned Packages

NodeDB Lite is not published yet. The package names below reflect the intended release targets:

```bash
# Rust (planned)
cargo add nodedb-lite

# JavaScript / TypeScript (WASM, planned)
npm install @nodedb/lite
```

## Quick Start

API shape preview while the project is still in development:

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

- **Multi-model locally** -- Vector, graph, document, full-text, timeseries, key-value, and more, all in-process with no network
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
