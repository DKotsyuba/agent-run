# A12 — Python-compatible canonical JSON

Status: accepted and implemented.

## Decision

`agent-run-domain::canonical` is the single compatibility serializer for values
whose bytes must match the frozen Python baseline. It implements the inventoried
CPython form:

```text
json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=...)
```

It provides `dumps` and `sha256_hex`, including CPython-compatible string
escaping and float rendering. The `ensure_ascii` choice remains explicit because
both historical forms exist.

## Current callers

- `agent-run-config/src/snapshot.rs` — configuration and runtime snapshot bytes
- `agent-run-config/src/role_plan.rs` — role/config/content revisions
- `agent-run-adapters/src/materialize.rs` — generated-home revision
- `agent-run-platform/src/snapshot_tree.rs` — snapshot manifests
- `agent-run-core/src/service.rs` — replay request fingerprint

Other JSON used only as a wire frame or a new Rust-local manifest may use its
own documented serializer. It must not silently replace the compatibility
serializer at a persisted cross-version boundary.

## Evidence

`crates/agent-run-domain/tests/canonical.rs` checks raw bytes and SHA-256 against
the frozen vectors in `crates/agent-run-domain/tests/fixtures/canonical/`.
Caller-specific tests cover snapshots, role plans, replay identity, and
materialized homes.

Historical Python call sites are cited from
`archive/python-legacy:src/agent_run/...`; those files are intentionally absent
from `main`.
