# NodeDB-Lite

NodeDB-Lite is a fully capable embedded database for edge devices — phones, tablets, browsers, and desktops. It runs all seven engines in-process with sub-millisecond reads, no server required. Offline-first with CRDT sync to Origin when connectivity returns.

## When to Use

- Mobile apps that need to work offline
- AI agents that need local memory (vectors + graph + documents)
- Browser-based apps (WASM)
- Desktop applications with local-first data
- IoT gateways with intermittent connectivity

## Platforms

| Platform              | Backend                   | Binary Size                                                                          |
| --------------------- | ------------------------- | ------------------------------------------------------------------------------------ |
| Linux, macOS, Windows | redb (file-backed)        | Native                                                                               |
| iOS _(in progress)_   | redb + C FFI (cbindgen)   | Native _(requires macOS build environment — Rust compiles but not yet built/tested)_ |
| Android               | redb + C FFI + Kotlin/JNI | Native                                                                               |
| Browser (WASM)        | redb (in-memory + OPFS)   | ~4.5 MB                                                                              |

## Key Features

- **All engines locally** — Vector search, graph traversal, document CRUD, full-text search, timeseries, spatial, KV — all in-process, no network
- **Sub-millisecond reads** — Hot data lives in memory indexes (HNSW, CSR, Loro)
- **CRDT sync** — Every write produces a delta. Deltas sync to Origin over WebSocket when online. Multiple devices converge regardless of operation order.
- **Shape subscriptions** — Control what data each device holds: `WHERE user_id = $me`, not the entire database
- **Conflict resolution** — Declarative per-collection policies. SQL constraints (UNIQUE, FK) enforced on Origin at sync time with typed compensation hints back to the device.
- **Encryption at rest** — AES-256-GCM + Argon2id key derivation
- **Memory governance** — Per-engine budgets, pressure levels, LRU eviction
- **Full SQL** — Same SQL via DataFusion as Origin. Window functions, CTEs, subqueries, JOINs.

## Same API, Any Runtime

The `NodeDb` trait is identical across Lite and Origin. Application code doesn't change:

```rust
// Works with both NodeDbLite (in-process) and NodeDbRemote (over network)
async fn search(db: &dyn NodeDb, query: &[f32]) -> Result<Vec<Article>> {
    db.vector_search("articles", query, 10).await
}
```

Moving from embedded to server is a connection string change, not a rewrite.

## Sync Architecture

```
Offline:    App writes locally -> Loro generates delta -> delta persisted to redb
Reconnect:  Device opens WebSocket -> sends vector clock + accumulated deltas
Cloud:      Origin validates (RLS, UNIQUE, FK) -> merges -> pushes back missed changes
Conflict:   Rejected deltas -> dead-letter queue + CompensationHint -> device handles
Converged:  Device and cloud share identical Loro state hash
```

Sync features:

- ACK-based flow control (AIMD)
- CRC32C delta integrity
- JWT token refresh during sync
- Replay dedup and sequence gap detection
- Rate limiting and downstream throttle

## Performance Targets

| Metric                                | Target                  |
| ------------------------------------- | ----------------------- |
| Vector search (1K vectors, 384d, k=5) | < 1ms p99               |
| Graph BFS (10K edges, 2 hops)         | < 1ms p99               |
| Document get                          | < 0.1ms                 |
| Cold start (10K vectors + 100K edges) | < 500ms                 |
| Sync round-trip (single delta)        | < 200ms                 |
| WASM bundle                           | ~4.5 MB                 |
| Mobile memory                         | < 100 MB (configurable) |

## FFI and WASM

**C FFI** (`nodedb-lite-ffi`) — 12 extern functions with cbindgen-generated header. Kotlin/JNI bridge for Android.

**WASM** (`nodedb-lite-wasm`) — JavaScript/TypeScript API via wasm-bindgen. redb runs in-memory with optional OPFS persistence in browsers.

## Related

- [Documents](documents.md) — Schemaless documents with CRDT sync
- [Architecture](architecture.md) — How Origin's execution model differs from Lite
- [Security](security/README.md) — Encryption at rest on Lite devices

[Back to docs](README.md)
