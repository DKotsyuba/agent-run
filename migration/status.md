# Migration status — Rust primary at 0.12.0

Baseline: `DKotsyuba/agent-run` v0.11.15, commit `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`.

## Update — 20 September 2026

The Rust-primary release includes:

- the broker compares the exact `config.toml` SHA-256 every 60 seconds and
  loads changed valid configuration without restart; starts and continuations
  also check immediately;
- the command-flood failure was a real event-loop fairness defect, not a flaky
  assertion: both stream runners now poll ready engine output before opening a
  later command page;
- the stale-socket test now waits for the kernel's required `ECONNREFUSED`
  precondition instead of racing Darwin's pending-connect queue;
- a repeated rollback is rejected as `deployment is already rolled back`
  instead of reporting a successful no-op.
- Qwen is removed from both implementations, active fixtures, and public
  runtime documentation; legacy adapter identifiers fail with deprecation
  guidance instead of attempting dynamic loading.

The full workspace suite passed with zero failures and one ignored Keychain
smoke. Formatting, clippy with warnings denied, release qualification, source
archive verification, and the 33-entry evidence index all pass. Real start and
resume canaries reached Codex, Claude, and GLM through the isolated Rust broker.
Desktop-host delivery and a non-macOS qualification run remain unclaimed.

### Qwen removal checkpoint

Qwen is intentionally deprecated, not awaiting a replacement implementation.
The Rust adapter variant and execution branches, Python adapter package,
Qwen-only tests and generated-home fixtures, Keychain fallback, public runtime
documentation, and live T57 requirement are removed. Legacy Rust and Python
adapter identifiers remain only as fail-fast deprecation guards. The explicit
OmniRoute capacity source remains runtime-neutral and is not a Qwen adapter.

Verification completed on this candidate: the Rust workspace builds every test
target with `cargo test --offline --workspace --all-features --no-run`; the
Python 3.14 native-settings selector passes 26 tests and 44 subtests, including
the deprecated-config rejection. A first full Python run reached 1117 passed / 1
failed / 1 skipped: the only failure was the operator-guide test still expecting
the removed `opencode/` Qwen alias. The guide and assertion now describe only
Codex/Claude/GLM, and the exact regression passes. Affected Python selectors
pass 93 tests / 113 subtests; Rust clippy and affected Rust selectors pass. The
coverage map is reconciled at 1078 ported / 69 declared divergences / 0 planned.
The first full Rust run likewise had only three stale operator-guide assertions
expecting `opencode/`; both affected Rust targets now pass (22 + 9 tests).
The clean full Python rerun passes 1118 tests / 408 subtests with one skipped
Keychain smoke. The clean full Rust rerun passes 1076 tests with zero failures
and one ignored Keychain smoke. Qualification accepts the explicit T57
divergence and reports 84/84 scenarios. Independent code review found no defects; evidence review findings are
resolved. The implementation is committed as
`93c7a8c1e106bbb9f1eeab09ec1f51b069254125`; its source archive verifies, and
the refreshed evidence index now verifies 33 entries.

The sealed candidate at `e8d5b7d6011680f4ff79913d0c25c8d6f92a81c4`
also passed an isolated release smoke: manifest verification, CLI, API ping and
tool discovery, MCP initialization/listing, broker-backed fixture execution,
verified answer proof, clean doctor, and socket cleanup. Evidence is recorded
in `migration/evidence/release-smoke-2026-09-20.md`.

The owner-terminal live run then reached all supported providers through that
same isolated broker. Codex, Claude, and GLM each returned the exact requested
canary sentinel, and each native session resumed to a second exact sentinel;
answer proofs, transcripts, lineage, and process cleanup were verified. The
record is `migration/live-canary-checkpoint-2026-09-20.md`. This closes the
supported-engine live portions of T58–T59 without claiming Desktop T71 or a
second operating system for T82.

## What this artifact is

A native Rust implementation of agent-run: workspace source, SQL, assets, an
optional signed-Node bridge for one external host, tests, and migration
documentation. It is not a Python wrapper.

The frozen Python implementation on `archive/python-legacy` was the behavioral
reference throughout. Rust tests retain their baseline citations, and the final
coverage ledger generated before archival remains in
`migration/baseline/test-map.csv`.

**What "ported" means here.** A behavior was counted only when a Rust test
carrying a machine-parsed `Mirrors` citation is discovered by `cargo test
--list`. It does not mean a human reviewed the behavior, and it does not mean
the subsystem was qualified under load or on a different platform. Live engine
evidence is recorded separately from this historical coverage count.

## Corpus coverage

The final pre-archive measurement is retained in
`migration/baseline/test-map.csv`.

| Status | Behaviors |
| --- | ---: |
| ported | 1078 |
| declared divergence | 69 |
| remaining | 0 |
| **total** | **1147** |

The last previously planned behavior,
`tests/test_adapter_versions.py::test_grandchild_holding_stdout_cannot_outlive_the_deadline`,
is an explicit A10 divergence. Rust returns promptly at the deadline but never
unconditionally signals a process group after its leader identity can no longer
be verified.

That divergence was measured, not assumed; the measurement and false-pass trap
are recorded in `migration/adr/A10-spawn-backend.md` under "Measured cost of
decision 5". The Rust-primary decision explicitly records it as a divergence,
not unfinished work.

Declared divergences are recorded per behavior in
`migration/baseline/divergences.csv`, each naming the ADR that decided it: A09
(Desktop relay, legacy queue sender), A10 (payload pipe, closed adapter set),
A19 (release and deploy), A20 (supervisor architecture), A21 (runtime platform),
and A22 (Qwen removal).

## Qualification scope

`migration/evidence/qualification-scope.md` maps the plan's 84 qualification
scenarios onto the Rust tests that assert them.

| | Scenarios |
| --- | ---: |
| covered, no live portion | 79 |
| covered, live portion remains | 2 |
| partial, live portion open | 2 |
| divergent, no live portion | 1 |
| **not covered** | **0** |

Every cited test name was checked against `cargo test --list`, so a citation
cannot refer to a test that does not exist.

The supported-engine live start/resume portions of T58–T59 are complete. The
remaining unclaimed resource classes are not coding tasks:

1. the running ChatGPT Desktop host (T71);
2. one non-macOS machine (T82).

`cargo xtask qualify --release` reports this state and **refuses** to certify a
platform with no recorded evidence rather than warning about it.

## Current check results

Measured personally on macOS (Darwin 27.0.0, arm64) at the commits noted:

- `cargo fmt --all --check`: clean.
- `cargo clippy --workspace --all-targets --all-features`: **zero** warnings.
- `cargo xtask qualify --release`: exit 0, 84/84 scenarios; its immutable scope
  map still labels the live boundaries while the supported-engine run is
  recorded separately in the live-canary checkpoint.
- `cargo xtask archive --verify`: exit 0.
- `cargo xtask evidence verify`: exit 0, 33 entries.
- The four acceptance commands that earlier revisions of this file called
  unproven all pass: M14a (3 tests), M28a (2), M31b (3), M33 (2).

**Full suite, measured where Unix sockets are permitted**, at commit `69258c7`
(the `xtask` recovery drills merged after it, adding 5 tests):

| Run | Result |
| --- | --- |
| first | **1084 passed, 0 failed, 1 ignored** across 109 targets |
| `--locked` | **1084 passed, 0 failed, 1 ignored** — the committed lockfile resolves |
| second | 1083 passed, **1 failed**, 1 ignored |
| 20 September working tree | **all targets passed, 0 failed, 1 ignored** |

The ignored test is a Keychain probe that needs a real keychain. The two former
intermittent failures are resolved in the 20 September update above.

A shell that forbids binding Unix domain sockets reports ~56 failures across 7
targets. Every one is a bind denial — verified by a direct bind probe and by
reading `PermissionDenied: Operation not permitted` in the panic text, not by
counting. Do not accept that classification without reading a panic message
yourself.

## Defects this migration found in itself

Eight correctness defects were found in the Rust implementation while closing
the qualification gaps, and all are fixed and merged. Five are the same failure
in different places: **Python names a deadline or refuses to answer, and this
port waited forever or reported "clean" from an observation it could not make.**
Two of those sites carried a comment asserting a bound the code did not
implement, which is how they survived review.

The ledger — site, class, how it surfaced, and the test that guards it now — is
`migration/defects-found.md`. Read it before extending this code; the same shape
is worth hunting elsewhere.

## Cutover runbook: rehearsed once, outside fixtures

The §21 runbook was exercised end to end on a disposable home and prefix, using
the real commands rather than the fixture functions the recovery tests call.
Every `xtask` deploy subcommand requires `--prefix` and `--home` explicitly and
none reads `$HOME`, so a rehearsal cannot reach an installed home by accident.

Sealed a release, verified it, installed it, updated onto a second release,
rolled back, rolled forward, and ran `recover`. After all of it the `current`
pointer still named a sealed release, both releases and both backups survived —
retention deleted nothing in use — and the journal stayed readable at every
step. A rollback with no previous release **refused** with `journal has no
previous release` and exit 2 rather than inventing an empty restore.

The journal also names the external boundary itself: *"outside xtask, restore
services/jobs, verify API readiness and capability discovery, then run the
isolated release smoke."* That is ADR A19's divergence stated in the artifact an
operator reads at the moment of cutover.

The repeated-rollback weakness found by this rehearsal is fixed in the 20
September working tree: a second rollback recognizes `rolled_back`, returns an
explicit error, and leaves the pointer and journal unchanged.

This rehearsal is **not** a substitute for the §21.6 drill set or for a
production cutover. It shows the switch, rollback and roll-forward mechanics
work on real files; it says nothing about services, readiness or smoke, which
are the operator's half of the runbook.

## Known differences and unsupported paths

1. Python-created runs cannot be resumed by this implementation (ADR A18). A
   matching schema version is not evidence that identity snapshots, launch homes
   and request replay are compatible. Cutover therefore requires a quiescent
   installation with no unfinished Python-created runs.
2. Completion delivery into the ChatGPT desktop app requires a minimal signed
   Node shim: the host authenticates its peer, parent and grandparent against an
   Apple team identifier that a normally signed Rust binary cannot satisfy
   (ADR A09). A certificate of our own does not help — the question is whose
   signature, not whether one exists. A09 remains *Proposed*.
3. **Three of the nine cutover steps are deliberately outside this tool.** ADR
   A19 places C5 (restore services), C6 (wait for API readiness) and C7
   (release-time smoke) out of scope: `xtask` owns the deployment journal and the
   pointer swap, not service control. A production cutover therefore cannot be
   performed by `cargo xtask` alone; that half of the runbook is the operator's.
4. Read-only Claude and GLM runs omit Bash. Tool preferences are not promoted to
   unverified OS isolation guarantees; `required_constraints` is the union of role
   and request, never an override (ADR A11, decided).
5. Normalized messages are persisted; the upstream raw-stream spool and
   external-payload reference contract are not reproduced, and detailed
   usage/TTFT/API timing views are not all equivalent.
6. Delivery evidence uses a smaller Rust shape; historical Python-shaped evidence
   is not fully projected, and notice wording is not byte-for-byte equivalent.
7. The legacy `codex queue` subprocess sender is out of scope: the frozen
   `archive/python-legacy` branch retains it, but its production transport never
   calls it (ADR A09, option C2).

## Remaining unclaimed acceptance surfaces

The scoped macOS/arm64 Rust-primary release has full fixture coverage, a sealed
release smoke, and successful live Codex/Claude/GLM start and resume evidence.
The following claims remain intentionally outside that acceptance:

1. **Desktop host delivery.** T71 still needs a smoke against the real running
   ChatGPT Desktop host. ADR A09 and board row M09 remain the controlling record.
2. **Non-macOS support.** T82 still needs a second operating system. Version
   0.12.0 therefore supports macOS/arm64 only; Linux CI is validation-only.
3. **Production cutover.** A green release candidate does not authorize changing
   a working installation. The operator must complete the signed checklist,
   backup, pointer switch, service restart, API/MCP smoke, and rollback evidence.

The A10 process-group policy is accepted as an explicit divergence: Rust never
signals a possibly reused group after the leader identity is no longer proven.
It is not unfinished migration work.

### What the board's numbers do not mean

`migration/tasks.csv` holds 82 rows: 80 `implemented`, 2 `planned` (M09, M55),
and **0 `verified`**. The plan separates those two statuses deliberately and
requires an explicit flag when live verification is absent. "80 of 82" is
therefore not a readiness figure — it says the code exists and its own
acceptance command passes. `migration/tasks.md` states this in full.

## Toolchain and lockfile

A reviewed `Cargo.lock` is committed (224 packages, lock version 4) and the
toolchain is pinned to Rust 1.98.1.

**Verifying `--locked` offline: filter by platform, or it will look broken.**
`cargo metadata --locked --offline` without a platform filter fails with

```
error: failed to download `android_system_properties v0.1.6`
Caused by: attempting to make an HTTP request, but --offline was specified
```

That is **not** a stale lockfile. `cargo metadata` resolves dependencies for
every platform at once, and `android_system_properties` is an Android-only
transitive dependency of `iana-time-zone`; it is absent from the offline
registry because a macOS build never needs it. Resolution restricted to the host
target succeeds:

```
cargo metadata --locked --offline --filter-platform aarch64-apple-darwin
```

## Structured output clarification

The pinned Python Claude adapter implements `output_schema` as a prompt
instruction, not a native JSON-schema validator (`adapter.py:252-259`). The Rust
stream adapter follows that limited behavior. Neither a schema-shaped prompt nor
a successful answer proof establishes JSON-schema conformance; this port claims
no stronger guarantee.
