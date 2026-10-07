# Development and release performance design — 2026-10-04

## Scope and evidence

This design examines the complete feedback and delivery cycle: local editing,
Cargo compilation/linking, test selection, CI dependencies and scheduling,
packaging, artifact transfer, and release acceptance. It is not limited to
GitHub caching. The published baseline is agent-run v0.20.3 at
`523517ccf91e22de11b595a71b47bb2627510d79`; observed execution evidence and
cache limits are in [CI_CACHE_AUDIT.md](CI_CACHE_AUDIT.md).

## Required constraints

- Preserve locked inputs, provenance and signature identities, platform
  qualification, meaningful runtime/transport/provider assertions, exact
  shipped-payload acceptance and immutable releases.
- Distinguish source gates from executable acceptance. Cache restoration or
  agreement about a plan is not proof of runtime or release readiness.
- Prefer existing Cargo/xtask/test facilities and a small number of direct
  changes. Any new tool or abstraction must address a measured limitation.
- Separate reusable template policy from agent-run-specific code. Existing
  consumers are independent and are upgraded through their reviewed process.

## Decision and verification structure

Each proposed change must identify exact implementation boundaries, work it
removes, one-time versus steady-state cost, evidence-backed benefit or unknown
benefit, retained guards, and a minimal verification selector. Evaluate local
feedback latency, release critical-path latency and total compute separately.
Use comparable cold/warm inputs rather than attributing differences between
different commits or machines to a cache.

## Recommended first phase: preserve all current checks

1. Remove the native artifact job's dependency on the release source gate in
   `.github/workflows/release.yml:63`. Run both jobs in parallel; preserve
   publication's `needs: [gate, native]` and both existing full suites. A failed
   gate still prevents publication. This reduces serialization, not compute;
   it requires two native runners available concurrently.
2. Give the existing combined check/native dependency-cache workload one full
   main-side producer: the macOS CI check job. Give the shorter release-contract
   job a separate key or make it restore-only. Use a fresh namespace to stop
   restoring the current partial immutable entry, and update the workflow key
   assertions in `xtask/tests/ci_workflow.rs:88-100`. Preserve PR restore-only
   policy, native platform boundaries and disabled binary caching. Measure extra
   cache bytes and transfer cost; workspace compilation/linking remain.
3. Document a short local edit loop in `CONTRIBUTING.md`: direct formatting
   first, then Cargo check/test scoped to the changed crate and test target.
   Keep `--all-features`, because `test-fixtures` supplies real acceptance
   coverage. Run the full integration gate and explicit native/live selectors
   at coherent checkpoints and before a PR. No optimized release build per edit.

These changes need no new dependency, runtime module or cache framework. They
can be assessed independently before changing test coverage.

## Second phase: coordinated gate and coverage changes

Remove the native job's repeated full source gate only through a combined
workflow/contract-test/documentation change. The source gate remains authoritative
for annotated tag, workspace version, native architecture and full tests. The
native job retains architecture, explicit target and a cheap tag/version check.
Both use the same workflow source identity with no checkout ref override;
publication still requires both jobs. Do not assume its shallow checkout has
all historical Git metadata. Update the existing assertion requiring the second
gate in `xtask/tests/ci_workflow.rs:199-202` and `docs/releasing.md:72-74` together.
This removes the observed 397-second gate step per release, including repeated
debug tests. It also removes second-runner repetition, which may expose flakes;
it does not remove or replace an executed-release-binary acceptance check.

Consolidating the two macOS CI native builds is a separate coverage decision.
Keep one explicit-target release-contract build and skip the matrix job's native
build only after documenting the lost host-default compile/link/seal smoke.
A small output-directory unit test preserves that directory contract, not
equivalence of host and explicit-target builds. Their build-script/proc-macro
and compiler-flag behaviour differs. If host-mode smoke must remain, retain
both builds and optimize only their cache keys.

If consolidation is adopted, split cache producer roles coherently: the CI
check job produces the check key for CI checks and the release source gate;
release-contract CI produces the explicit-target native key for native consumers.
The check job no longer produces native outputs after its native build is removed.

Reuse of a CI-built sealed executable in release is a later provenance design:
version alone does not bind source, lockfile, toolchain, flags, target and payload
digest. Integrity/provenance verification is not execution of the shipped
binary. Linker/profile changes and additional test-runner/compiler-cache tools
remain measurement-dependent proposals, not assumed quick wins.

For the observed v0.20.3 jobs, parallel execution while retaining both gates has
a work-only critical path of `max(424, 670) + 8 = 678` seconds (11m18s) before
queueing. Also removing the repeated 397-second native source gate would give
`max(424, 670 - 397) + 8 = 432` seconds (7m12s) before queueing. Adding the old
189-second overhead yields illustrative 14m27s and 10m21s scenarios, not a
forecast: changing the job graph changes resource contention and queues.

## Verification and retained limits

- Hold commit, lockfile, toolchain, native runner selection, target, features,
  profile, compiler flags and fetch state equal. Use at least three comparable
  cold, dependency-warm, same-directory no-op and edit samples. Scope dependency
  `Checking`/`Compiling` counts by phase and profile; distinguish compilation,
  linking, test execution, transfer and queueing.
- Use a fresh namespace or isolated target to obtain cold measurements; confirm
  a miss in the log. Do not delete caches. Use nonpublishing runs for experiments;
  compare future legitimate releases observationally rather than rerun an
  already published immutable tag.
- Check `cargo test --offline --locked -p xtask --test ci_workflow` and an
  end-to-end nonpublishing run after implementation. Preserve test counts,
  explicit live guards, failure propagation, publication fan-in and existing
  integrity checks. Judge performance against comparable observations, not
  inferred time scenarios as hard thresholds.
- Embedded documentation, SQL, role plans and templates are executable inputs:
  documentation-only file extensions are not a reason to bypass source checks.
- Linux is currently nonblocking and fails Clippy at platform `fs.rs:232`;
  repair is a separate prerequisite for a successful Linux cache producer, not
  permission to suppress the warning or claim Linux qualification.
- Inspected workflows do not themselves verify cryptographic commit/tag
  signatures, run a dependency advisory audit or execute the sealed release
  payload. Annotated-tag checks, `gh release create --verify-tag`, checksums and
  attestations are not those checks. This is a scoped workflow observation;
  external qualification boundaries remain unchanged and are not claimed closed.

## Applicability and evidence

Reusable template policy: independent artifact jobs with publication fan-in,
one complete producer per actual cache workload, phase-specific validation,
and explicit source-versus-payload acceptance. Do not copy agent-run's job graph
into a template that already has one build/gate merely to create more jobs.
Crate selectors, host/explicit-target coverage and embedded/native guard lists
remain agent-run-specific.

The read-only design is complete. Source, workflow contracts and primary log
excerpts were checked; cache contents were not unpacked and controlled speedup
measurements have not been run. Source review also triggered the IDE's automatic
background check (0 errors, 4 warnings), which is not a full project gate. No
implementation, manual full build/test run, CI rerun, cache deletion, commit,
push, release or installed-service change was performed for this design.

Sources: [CI timings](https://github.com/DKotsyuba/agent-run/actions/runs/37226623655),
[release timings](https://github.com/DKotsyuba/agent-run/actions/runs/37226665361),
[pinned workflows and contracts](https://github.com/DKotsyuba/agent-run/tree/523517ccf91e22de11b595a71b47bb2627510d79),
[Cargo target/cache semantics](https://doc.rust-lang.org/cargo/reference/build-cache.html),
[GitHub CLI tag-existence option](https://cli.github.com/manual/gh_release_create).
