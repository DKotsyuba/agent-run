# Migration status — corpus ported and qualified on fixtures; not accepted

Baseline: `DKotsyuba/agent-run` v0.11.15, commit `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`.

## What this artifact is

A native Rust implementation of agent-run: workspace source, SQL, assets, an
optional signed-Node bridge for one external host, tests, and migration
documentation. It is not a Python wrapper.

The Python implementation is present in this repository and was used as the
behavioral reference throughout: every ported Rust test cites the Python test it
mirrors, and the citations are machine-checked by
`migration/tools/coverage_report.py` against `cargo test --list`.

**What "ported" means here.** A behavior is counted only when a Rust test
carrying a machine-parsed `Mirrors` citation is discovered by `cargo test
--list`. It does not mean a human reviewed the behavior, and it does not mean
the subsystem was qualified under load, against a real engine, or on any
platform other than the one it was tested on. See *Blocking acceptance gates*.

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
It is **composite**, and only half of it is open. The deadline half — the call
returns promptly even while a descendant holds stdout — is satisfied. The other
half, that the descendant is dead once cleanup returns, is not: Python kills the
recorded process group unconditionally, while this port signals only a leader it
has verified alive (ADR A10, decision 5).

That divergence was measured, not assumed; the measurement and the false-pass
trap that hides it are recorded in `migration/adr/A10-spawn-backend.md` under
"Measured cost of decision 5". It is an owner decision, which is why the
behavior is carried as unported rather than quietly reclassified.

Declared divergences are recorded per behavior in
`migration/baseline/divergences.csv`, each naming the ADR that decided it: A09
(Desktop relay, legacy queue sender), A10 (payload pipe, closed adapter set),
A19 (release and deploy), A20 (supervisor architecture), A21 (runtime platform).

## Qualification scope

`migration/evidence/qualification-scope.md` maps the plan's 84 qualification
scenarios onto the Rust tests that assert them.

| | Scenarios |
| --- | ---: |
| covered, no live portion | 80 |
| covered, live portion remains | 1 |
| partial, live portion open | 3 |
| **not covered** | **0** |

Every cited test name was checked against `cargo test --list`, so a citation
cannot refer to a test that does not exist.

The live portions that no fixture can supply are exactly three, and none is a
coding task:

1. one installed `qwen` binary, to prove the flags are accepted (T57);
2. the running ChatGPT Desktop host (T71);
3. one non-macOS machine (T82).

`cargo xtask qualify --release` reports this state and **refuses** to certify a
platform with no recorded evidence rather than warning about it.

## Current check results

Measured personally on macOS (Darwin 27.0.0, arm64) at the commits noted:

- `cargo fmt --all --check`: clean.
- `cargo clippy --workspace --all-targets --all-features`: **zero** warnings.
- `cargo xtask qualify --release`: exit 0, 84/84 scenarios, three live portions
  named and explicitly not claimed as run.
- `cargo xtask archive --verify`: exit 0.
- `cargo xtask evidence verify`: exit 0, 29 entries.
- The four acceptance commands that earlier revisions of this file called
  unproven all pass: M14a (3 tests), M28a (2), M31b (3), M33 (2).

**Full-suite figure: being re-measured.** The last socket-permitted full run
recorded 1077 passed / 0 failed / 1 ignored at commit `176c89e`. Seven merges
have landed since, including a 217-line rewrite of the socket transport, so that
figure no longer describes this revision and is not restated here as if it did.
A re-measurement at the current commit is in progress and will replace this
paragraph.

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
7. The legacy `codex queue` subprocess sender is out of scope: Python retains it
   but its production transport never calls it (ADR A09, option C2).

## Blocking acceptance gates

The corpus is ported and the scenario map is closed on fixtures. **Formal
acceptance has not been performed, and this artifact must not be installed over
a working release.** What remains is not implementation:

1. **Live qualification (board M55) has not been run and needs authorization.**
   Three real-world resources are required, listed under *Qualification scope*.
   `migration/evidence/qualification.md` is deliberately absent: writing it
   without a qualification run would fabricate evidence.
2. **Platform support is undecided.** All testing ran on macOS/arm64. Linux code
   paths exist — including the process-identity and liveness paths the
   supervision model rests on — and have never executed. ADR A15 records the
   evidence and recommends declaring macOS/arm64 only; the decision is the
   owner's. Plan risk K02 names "Mac/Linux" explicitly in its closure condition.
3. **One blocking risk is open.** Of the seven risks the plan marks blocking,
   six are closed by evidence. K01 — Desktop admission without a signed shim —
   is open, and the plan states the consequence directly: without that evidence,
   full parity is not claimed. This is the same question as board row M09 and
   ADR A09.
4. **The process-group kill strategy is undecided**, which is what leaves the one
   unported behavior open. See *Corpus coverage*.
5. **P12 is not closed by a green test run.** The plan's exit condition is a
   signed review checklist, an evidence bundle, a release archive and a
   post-cutover report — and before any of that, the §21 runbook exercised on a
   **copy** of the environment, with the §21.6 recovery drills on a disposable
   home. The unsigned checklist is `migration/review-checklist.md`.

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
