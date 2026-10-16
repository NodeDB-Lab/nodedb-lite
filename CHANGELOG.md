# Changelog

All notable changes to NodeDB Lite are documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
NodeDB Lite uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.1.0] - 2026-10-16

First release. Embedded NodeDB for Linux, macOS, Windows, Android and the browser, built on NodeDB 0.6.2 crates and pagedb 0.1.

### Fixed

- **Vector index:** search found nothing after the last vector was deleted.
- **Full-text search:** each reopen rebuilt the index on top of the old memory reservation, so the memory budget ran out.

---

[Unreleased]: https://github.com/NodeDB-Lab/nodedb-lite/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/NodeDB-Lab/nodedb-lite/releases/tag/v0.1.0
