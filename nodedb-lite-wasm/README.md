# nodedb-lite-wasm

WebAssembly bindings for **NodeDB-Lite**, the embedded variant of NodeDB. Runs in browsers and Node.js. Exposes all eight Lite engines (Vector, Graph, Document schemaless, Document strict, Columnar/Timeseries/Spatial, KV, FTS, Array) through a single `NodeDb` API.

> **Lite only.** This crate is *not* a WASM build of the Origin server. The distributed Origin engine (Tokio Control Plane, io_uring Data Plane, QUIC cluster transport) does not target WebAssembly. To talk to an Origin cluster from the browser, run Lite-WASM locally and sync via WebSocket. See the [WASM deployment guide](../../nodedb/docs/wasm.md) for the full picture.

## Status

**Experimental / preview.** Build and basic engine usage work end-to-end. A WASM CI lane runs `cargo check --workspace --target wasm32-unknown-unknown`, a release build via `wasm-pack build`, and the `wasm-pack test --node` suite on every PR.

Outstanding items before this is considered stable:

- Published `npm` package

Until that lands, treat the WASM target as preview. File issues for anything that breaks.

## Install

Local development build:

```bash
cd nodedb-lite/nodedb-lite-wasm
wasm-pack build --target web --release
```

Outputs go to `pkg/`. To consume from a sibling app:

```bash
cd ../my-app
npm link ../nodedb-lite-wasm/pkg
```

A published npm package will be available once the project reaches stable status.

## Quick Start

```javascript
import init, { NodeDbLite } from "nodedb-lite-wasm";

await init();
const db = new NodeDbLite();

await db.sql("CREATE COLLECTION users");
await db.sql("INSERT INTO users { name: 'Alice', age: 30 }");

const rows = await db.sql("SELECT * FROM users WHERE age > 25");
console.log(rows);
```

## Engines

All eight engines work in WASM with the same SQL surface as native Lite:

| Engine             | DDL example                                                          |
| ------------------ | -------------------------------------------------------------------- |
| Document           | `CREATE COLLECTION docs`                                             |
| Key-Value          | `CREATE KV cache`                                                    |
| Vector             | `CREATE VECTOR INDEX idx ON docs METRIC cosine DIM 384`              |
| Full-text          | `CREATE FTS INDEX idx ON docs FIELD body`                            |
| Graph              | `CREATE COLLECTION edges` + `GRAPH INSERT EDGE ...`                  |
| Columnar           | `CREATE COLLECTION events WITH (storage = 'columnar')`               |
| Timeseries         | `CREATE COLLECTION metrics WITH (profile = 'timeseries', ...)`       |
| Spatial            | `CREATE COLLECTION places WITH (profile = 'spatial', ...)`           |
| Array (NDArray)    | `CREATE ARRAY grid DIMS (...) ATTRS (...) TILE_EXTENTS (...)`        |

See the [query language reference](../../nodedb/docs/query-language.md).

## CRDT Sync to Origin

Writes are local-first. Configure sync to push deltas to an Origin cluster:

```javascript
await db.sync_config({
  server_url: "wss://origin.example.com",
  auth_token: "...",
  auto_sync: true,
  sync_interval_ms: 5000,
});
```

Origin validates constraints and returns compensation hints on conflict. See [offline sync patterns](../../nodedb/docs/offline-sync-patterns.md).

## Build Targets

```bash
# Browsers (ESM)
wasm-pack build --target web --release

# Node.js
wasm-pack build --target nodejs --release

# Bundlers (webpack, rollup, vite)
wasm-pack build --target bundler --release
```

## Testing

Tests run under Node.js via wasm-pack. From the `nodedb-lite/` workspace root:

```bash
wasm-pack test --node nodedb-lite-wasm
```

The `--node` flag is required (not `--web` or `--headless`) because the tests use
`wasm_bindgen_test_configure!(run_in_node_experimental)` and rely on Node.js
module resolution rather than a browser environment.

To verify the workspace compiles for the wasm32 target before running tests:

```bash
cargo check --workspace --target wasm32-unknown-unknown
```

This command must be run from the `nodedb-lite/` workspace directory.

## Limitations

- **In-memory only** — no native filesystem. Use IndexedDB/`localStorage` via a wrapper if persistence is needed across reloads.
- **Single-threaded** — runs on the JS/WASM main thread; no thread-per-core, no parallelism.
- **No io_uring, no native sockets** — storage and networking go through host APIs.
- **No cluster role** — Lite-WASM cannot serve as a Raft member or vShard host. It is a client/edge node only.
- **Bundle size** — measure your own build; gzip before serving.

## Size Optimization

```bash
cargo install wasm-opt
wasm-opt -O4 pkg/nodedb_lite_wasm_bg.wasm -o pkg/nodedb_lite_wasm_bg.wasm
```

Measure before and after on your own build.

## License

Apache-2.0. See the workspace root `LICENSE` file.

## See Also

- [WASM deployment guide](../../nodedb/docs/wasm.md)
- [NodeDB-Lite](../nodedb-lite/) — native embedded crate
- [NodeDB-Lite FFI](../nodedb-lite-ffi/) — C/iOS/Android bindings
