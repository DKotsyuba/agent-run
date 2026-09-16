# Migration status — corpus ported, release gate NOT satisfied

Baseline: `DKotsyuba/agent-run` v0.11.15, commit `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`.

## What this artifact is

A native Rust implementation of agent-run: workspace source, SQL, assets, an
optional signed-Node bridge for one external host, tests, and migration
documentation. It is not a Python wrapper.

The Python implementation is present in this repository and was used as the
behavioral reference throughout: every ported Rust test cites the Python test it
mirrors, and the citations are machine-checked by
`migration/tools/coverage_report.py`.

**What "ported" means here.** A behavior is counted only when a Rust test that
carries a machine-parsed `Mirrors` citation is discovered by
`cargo test --list`. It does not mean the behavior was reviewed by a human, and
it does not mean the corresponding subsystem has been qualified under load or on
any platform other than the one it was tested on. See *Blocking acceptance
gates*.

## Corpus coverage

Measured by `migration/tools/coverage_report.py`; the per-behavior ledger is
`migration/baseline/test-map.csv`.

| Status | Behaviors |
| --- | ---: |
| ported | 1107 |
| declared divergence | 39 |
| remaining | 1 |
| **total** | **1147** |

The single remaining behavior is
`tests/test_adapter_versions.py::test_grandchild_holding_stdout_cannot_outlive_the_deadline`.
It is blocked on an owner decision, not on implementation work: Python kills the
process group unconditionally, while Rust signals only a leader it has verified
is alive — a deliberate protection against PID reuse established by ADR A10.
Three separate agents independently declined to change that rule on their own
authority.

Declared divergences are recorded per behavior in
`migration/baseline/divergences.csv`, each pointing at the ADR that decided it:
A09 (Desktop relay), A10 (payload pipe, closed adapter set), A19 (release and
deploy), A20 (supervisor architecture), A21 (runtime platform).

## Current check results

Measured at commit `a27e597` on macOS (Darwin 27.0.0, arm64):

- `cargo test --workspace --all-features`: 979 passed. The 56 failures observed
  were all Unix-domain-socket bind denials in a sandbox that forbids them;
  classified individually by failure text, none is a product defect. A
  socket-permitted run is recorded in `migration/evidence/`.
- `cargo fmt --all --check`: clean.
- `cargo clippy --workspace --all-targets --all-features`: zero warnings.

## Corrections to earlier versions of this document

Earlier revisions of this file described the first, unverified port. The
following statements in those revisions are **no longer true** and have been
removed rather than softened:

- "Numbered migrations 1–15 not ported" — `sql/migrations/002`–`016` are present
  and 19 migration behaviors are ported.
- "OmniRoute and labelled Codexbar collectors missing" — OmniRoute is implemented
  (`crates/agent-run-core/src/capacity/omniroute.rs`) with 13 behaviors ported;
  Codexbar account mapping is implemented in `capacity/sources.rs`.
- "Inline answers are capped at 128 KiB, lower than upstream" — wrong twice. The
  cap is 1 MiB (`crates/agent-run-platform/src/verify/mod.rs:18`) and it *matches*
  Python's `_DEFAULT_INLINE_ANSWER_BYTES` (`service.py:68`). The separate
  verification bound is 16 MiB on both sides.
- "Process creation does not reproduce `posix_spawn(setsid=True)`-first" —
  session-creating `posix_spawn` with `POSIX_SPAWN_SETSID` is the normal path
  (`crates/agent-run-platform/src/launch.rs`), with a fork fallback only where the
  C library rejects the flag.
- "macOS Keychain credential fallback is not implemented" — implemented in
  `crates/agent-run-platform/src/keychain.rs`.
- "`implemented` means code exists, not that it compiles or passes a test" — the
  workspace compiles, the suite runs, and the gates above are green.

## Known differences and unsupported paths

1. Python-created runs cannot be resumed by this implementation (ADR A18). A
   matching schema version is not evidence that identity snapshots, launch homes
   and request replay are compatible. Cutover therefore requires a quiescent
   installation with no unfinished Python-created runs.
2. Completion delivery into the ChatGPT desktop app requires a minimal signed
   Node shim: the host authenticates its peer, parent and grandparent against an
   Apple team identifier that a normally signed Rust binary cannot satisfy
   (ADR A09). The package is therefore not literally all-Rust at that one
   boundary. A09 remains *Proposed* and needs an owner decision.
3. Read-only Claude and GLM runs omit Bash. Tool preferences are not promoted to
   unverified OS isolation guarantees; `required_constraints` is the union of role
   and request, never an override (ADR A11, decided).
4. Normalized messages are persisted; the upstream raw-stream spool and
   external-payload reference contract are not reproduced, and detailed
   usage/TTFT/API timing views are not all equivalent.
5. Delivery evidence uses a smaller Rust shape; historical Python-shaped evidence
   is not fully projected, and notice wording is not byte-for-byte equivalent.
6. The legacy `codex queue` subprocess sender is out of scope: Python retains it
   but its production transport never calls it (ADR A09, option C2).

## Blocking acceptance gates

The corpus is ported. **Formal acceptance is not performed, and this artifact
must not be installed over a working release.** Outstanding:

1. **Qualification (board M55) has not been run.** `cargo xtask qualify` does not
   exist. No security-fault, load, or multi-platform qualification has been
   performed, and `migration/evidence/qualification.md` is deliberately absent —
   writing it without a qualification run would fabricate evidence.
2. **Handoff tooling (board M56) does not exist.** `cargo xtask archive --verify`
   and `cargo xtask evidence verify` are unimplemented.
   `migration/evidence/index.json` exists as a hashed inventory but is
   hand-assembled and unverified by tooling.

   For precision: `xtask` itself is not absent. `xtask/src/main.rs` dispatches
   `cargo xtask check` (fmt, clippy and the workspace test run) and
   `cargo xtask release build|build-native|verify|install|update|rollback`, backed
   by `xtask/src/release.rs` and `xtask/src/deploy.rs`. The missing pieces are the
   `qualify`, `archive` and `evidence` subcommands and their modules, not the tool.
3. **Platform support is undecided.** All testing to date ran on macOS/arm64
   only. Linux code paths exist — including the process-identity and liveness
   paths the supervision model rests on — but have never been executed. ADR A15
   records the evidence and recommends declaring macOS/arm64 only; the decision
   is the owner's.
4. **Four board tasks lack their named acceptance evidence** (M28a, M31b, M33,
   M14a). The implementing code was located, but each task's acceptance is a
   specific `cargo test` target that does not yet exist. They are deliberately
   held open rather than closed on inspection.
5. **Live engine and notification-host smoke tests** against real installed
   engines have not been authorized or run.

A reviewed `Cargo.lock` is committed and the toolchain is pinned to Rust 1.98.1.

## Structured output clarification

The pinned Python Claude adapter implements `output_schema` as a prompt
instruction, not a native JSON-schema validator (`adapter.py:252-259`). The Rust
stream adapter follows that limited behavior. Neither a schema-shaped prompt nor
a successful answer proof establishes JSON-schema conformance; this port claims
no stronger guarantee.
