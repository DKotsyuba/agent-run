# GitHub Actions cache audit — 2026-10-04

This read-only audit examines published workflow executions, not local source
changes. Runtime code and workflow configuration are outside the audit's write
scope. Conclusions apply to the inspected commits and job logs.

## Inspected revisions and executions

- Latest inspected release: `v0.20.3`, source
  `523517ccf91e22de11b595a71b47bb2627510d79`.
- Comparisons: release/CI executions for `v0.20.1` and `v0.20.2`, plus the
  initial `v0.20.0` executions and associated pool-development PR executions.
- Source workflows: `.github/workflows/ci.yml` and `release.yml` at the exact
  inspected release revision.

## Verified timing before cache-log classification

| v0.20.3 job | Cache restore | Gates | Native build | Total job |
|---|---:|---:|---:|---:|
| CI macOS checks | 12 s | 415 s | 217 s | 693 s |
| CI release contract | 9 s | archive 8 s | 132 s | 168 s |
| Release source gate | 12 s | 384 s | — | 424 s |
| Release native archive | 5 s | 397 s | 246 s | 670 s |
| Release publication | — | — | — | 8 s |

The CI Linux validation job failed; its cache post step was skipped. It is
configured as non-blocking, so the workflow's overall success does not imply
that this job passed or saved a cache. The primary failure was Clippy rejecting
an unnecessary `u32 -> u32` cast in `agent-run-platform/src/fs.rs:232`.

## Configuration facts

Compatible macOS jobs use a shared dependency cache namespace and the pinned
Rust cache action. CI saves only from successful pushes to main; release jobs
save in their tag scope. Cargo binary caching is disabled. Both release source
gate and native archive job run the workspace gate. Publication downloads built
artifacts and does not compile the product.

## Verified cache effectiveness

The sampled warm macOS jobs restored the same exact cache key, ending
`4d367070-3176d0e0`, with `full match: true`. The archive contained
279,240,628 bytes, reported by the action as approximately 266 MB. Restoration
typically took 5–15 seconds. This is an actual cache hit, not merely a green
restore step.

The classified sample contains 39 completed cache-bearing jobs: 28 macOS and
11 Linux. There were 19 exact macOS hits, four fallback macOS hits and five
initial macOS misses; all 11 Linux restores missed. Interrupted restore attempts
are excluded from these hit-rate denominators. All 19 warm unchanged-dependency
macOS jobs hit exactly; both latest dependency-update PRs correctly fell back to
the prior compatible entry (`full match: false`) in their four macOS jobs.

The first successful main-side producer was the v0.20.0 release-contract job
111317424425. It uploaded precisely 279,240,628 bytes. It built the native
release WITHOUT an explicit target argument. The source-gate job for tag
v0.20.0 separately saved 304,240,022 bytes, scoped to that tag. Subsequent
v0.20.1–0.20.3 jobs kept restoring the smaller main-side entry and ended with
`Cache up-to-date.`; they did not enrich that immutable cache with the outputs
they had just compiled.

This explains the principal coverage mismatch. At the inspected revision,
`xtask/src/main.rs:104-134` invokes Cargo with `--target` only when explicitly
passed. The release-contract CI job omits it, producing `target/release`.
The regular native CI and release archive jobs pass
`--target aarch64-apple-darwin`, producing
`target/aarch64-apple-darwin/release`. Both are eligible cache paths, but the
first main-side producer did not compile the explicit-target variant or the
full workspace test workload. A shared key therefore denotes a partial set of
compiled outputs, not a fully warm identical workload.

The logs confirm expensive recompilation despite exact hits. In v0.20.3 the
explicit-target build recompiled Tokio, Serde, serde_json, tracing, rusqlite,
reqwest, rmcp and Clap, then compiled workspace binaries. It took 217 seconds
in native CI and 246 seconds in the release archive job. The host-target
release-contract build took 132 seconds. These jobs differ in workload and
runner timing; their differences are not a controlled estimate of cache savings.

The release's source gate and native archive gate also repeated the same
`cargo xtask check` on the same source. In v0.20.3 those gate steps consumed
384 + 397 = 781 seconds before the native archive was published. The full
release workflow took 21 minutes 31 seconds; summed job durations were
18 minutes 22 seconds, leaving about 3 minutes of scheduling/inter-job overhead.
That overhead and actual test execution cannot be removed by dependency caching.

Linux repeatedly reported `No cache found.` and failed before its save phase.
Its independent namespace therefore remained cold in the inspected executions.

## Recommendations and limits

1. Normalize host versus explicit-target Cargo invocation for equivalent native
   jobs, or use workload-specific keys. Populate the reusable main cache from a
   successful full producer covering the intended test/native variants. A key
   epoch or controlled rebuild is needed to replace the existing partial entry;
   another successful restore does not modify it.
2. Review repeated source gates and native build jobs separately from cache
   tuning. Preserve source/qualification and exact-payload acceptance rather than
   deleting required checks to make a duration chart smaller.
3. Fix the Linux compilation failure before expecting a successful Linux cache
   producer; its non-blocking status currently hides the failure in workflow totals.
4. Benchmark cold and warm runs on the same commit/toolchain/runner selection,
   separating cache transfer, dependency compilation, workspace compilation,
   linking and test execution. No causal speedup percentage is claimed here.

Runtime/workflow fixes were not applied by this audit. No new CI runs, cache
deletions, settings changes, credential changes, source builds, commits or pushes
were performed. The audit report is local documentation, based on completed
GitHub executions and pinned published source.

Primary execution sources:
[CI 0.20.3](https://github.com/DKotsyuba/agent-run/actions/runs/37226623655),
[release 0.20.3](https://github.com/DKotsyuba/agent-run/actions/runs/37226665361),
[CI 0.20.2](https://github.com/DKotsyuba/agent-run/actions/runs/37198779120),
[release 0.20.2](https://github.com/DKotsyuba/agent-run/actions/runs/37198805091),
[CI 0.20.1](https://github.com/DKotsyuba/agent-run/actions/runs/37163360829),
[release 0.20.1](https://github.com/DKotsyuba/agent-run/actions/runs/37163370199),
[latest rmcp PR](https://github.com/DKotsyuba/agent-run/actions/runs/37226778646),
[latest thiserror PR](https://github.com/DKotsyuba/agent-run/actions/runs/37226766438).

Pinned source evidence:
[workflow](https://github.com/DKotsyuba/agent-run/blob/523517ccf91e22de11b595a71b47bb2627510d79/.github/workflows/ci.yml),
[release workflow](https://github.com/DKotsyuba/agent-run/blob/523517ccf91e22de11b595a71b47bb2627510d79/.github/workflows/release.yml),
[native Cargo output selection](https://github.com/DKotsyuba/agent-run/blob/523517ccf91e22de11b595a71b47bb2627510d79/xtask/src/main.rs#L104-L134).
