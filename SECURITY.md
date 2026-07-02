# Security Policy

## Supported versions

| Version | Supported |
| ------- | --------- |
| 0.1.x   | Yes       |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Report security issues by email to **security@nodedb.io**. If you receive no acknowledgement within 72 hours, follow up by opening a GitHub issue marked `[security]` with no vulnerability details included.

## Disclosure process

NodeDB follows a 90-day coordinated disclosure default:

1. Reporter sends details to `security@nodedb.io`.
2. Maintainers acknowledge within 72 hours and begin investigation.
3. A fix is prepared in a private branch.
4. Reporter is notified when the fix is ready and a release date is set.
5. Fix is released and a CVE (if applicable) is published simultaneously.
6. Reporter is credited in the release notes unless they prefer to remain anonymous.

If 90 days pass without a fix, reporters are free to publish their findings.

## Scope

NodeDB Lite is an embedded library. The primary security boundaries are:

- Encryption at rest (AES-256-GCM + Argon2id key derivation)
- CRDT sync transport (TLS required on production endpoints)
- C FFI memory safety (cbindgen-generated bindings)
- WASM sandbox isolation

Out of scope: issues in `redb`, `loro`, or other upstream dependencies that are tracked by those projects directly.
