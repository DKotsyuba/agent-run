# A18 — Python-created runs are readable but not Rust-resumable

Status: accepted compatibility boundary.

## Decision

The Rust service may read historical Python-created v16 rows for inspection and
reconciliation, but refuses to resume a parent whose identity lacks the native
Rust continuation proof. It returns a typed unsupported result instead of
guessing a provider session, re-resolving broader grants, or creating a new
conversation under the old lineage.

The historical formats are documented by
`archive/python-legacy:src/agent_run/resume.py` and
`archive/python-legacy:src/agent_run/preparation.py`. Those citations refer to
the frozen branch; the files are intentionally absent from `main`.

## Consequence

Existing Python runs remain auditable, and their terminal answers remain
readable. New starts and continuations use the Rust-native identity and have
live start/resume evidence for Codex, Claude, and GLM in the
[canary checkpoint](../live-canary-checkpoint-2026-09-20.md).

Supporting cross-runtime resume later would require a versioned compatibility
proof and explicit policy decision; silent fallback is prohibited.
