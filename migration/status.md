# Migration status — Rust primary at 0.12.0

Updated: 20 September 2026.

The migration is complete at the implementation boundary: `main` contains the
native Rust service, and the former Python implementation is frozen on
`archive/python-legacy` at `c58d5a0`. The Rust release does not invoke or bundle
Python. Qwen is removed by [ADR A22](adr/A22-qwen-removal.md).

## Verified

- The workspace suite, formatting, clippy, release qualification, source
  archive verification, and evidence-index verification pass on macOS arm64.
- A sealed 0.12.0 candidate passed CLI, API, MCP, broker, answer-proof, doctor,
  and cleanup smoke; see the [release smoke](evidence/release-smoke-2026-09-20.md).
- Real Codex, Claude, and GLM start and resume canaries succeeded through an
  isolated Rust broker; see the [indexed live-canary evidence](evidence/live-canary-2026-09-20.md).
- Configuration reload compares the exact `config.toml` SHA-256 every 60
  seconds and at request boundaries. Invalid revisions leave the last valid
  configuration active.

## Remaining qualification gaps

| Scope | State | Acceptance evidence still required |
|---|---|---|
| macOS arm64 | supported | release gates and sealed-release smoke are recorded |
| Linux x86_64 | non-blocking validation only; release and qualification owner-deferred | a future release decision plus hosted Linux full suite and sealed-release smoke (T82) |
| Desktop delivery | not yet live-qualified | smoke against the real Desktop host (T71) |
| Production cutover | operator action, not implied by build success | completed [review checklist](review-checklist.md), deployment journal, service smoke, and rollback evidence |

Version 0.12.0 publishes no Linux artifact. No document may promote Linux or
Desktop delivery to supported/qualified until that evidence is committed.
Current scenario status is in the
[qualification scope map](evidence/qualification-scope.md).

## Historical material

The task board, migration plans, baseline inventories, and dated evidence
retain the facts and counts observed while the port was underway. They do not
override this status. See [README.md](README.md) for archive and citation rules.
