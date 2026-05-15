# Contributing to NodeDB Lite

Thank you for your interest in contributing.

## Repository layout

| Crate | Description |
|---|---|
| `nodedb-lite` | Core embedded Rust library |
| `nodedb-lite-ffi` | C FFI bindings (cbindgen, Kotlin/JNI for Android) |
| `nodedb-lite-wasm` | JavaScript/TypeScript bindings via wasm-bindgen |

## Building

```bash
# Check all crates compile
cargo check --workspace

# Rust core
cargo build -p nodedb-lite

# C FFI (requires cbindgen)
cargo build -p nodedb-lite-ffi

# WASM (requires wasm-pack)
wasm-pack build --target web nodedb-lite-wasm
```

## Running tests

```bash
# Rust unit and integration tests
cargo nextest run -p nodedb-lite

# FFI tests
cargo nextest run -p nodedb-lite-ffi

# WASM tests (headless browser required)
wasm-pack test --headless --firefox nodedb-lite-wasm
```

Always use `cargo nextest run`, not `cargo test`. The test suite relies on nextest's
per-test isolation and retry configuration in `.config/nextest.toml`.

## Before opening a pull request

- [ ] `cargo fmt --all` — no formatting diffs
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` — no warnings
- [ ] `cargo nextest run -p nodedb-lite` — all tests green
- [ ] New public API has at least one integration test in `tests/`
- [ ] No `.unwrap()` calls in library code — propagate errors with `?`
- [ ] Files stay under 500 lines; split by concern if needed

## Commit messages

Conventional commits are encouraged: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `chore:`.
Use the imperative mood in the subject line ("Add X", "Fix Y", not "Added X").
Keep the subject under 72 characters. Reference the relevant issue number if one exists.

## Code of conduct

This project follows the [Contributor Covenant 2.1](./CODE_OF_CONDUCT.md).
