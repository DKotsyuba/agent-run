# Load stability plan

## Evidence and qualification boundary

An October 7, 2026 Europe/Belgrade audit preserved 228 execution rows: 45 failed/timed-out executions across 30 stable agents, plus one startup failure found only in a supervisor log. Retention removed detailed evidence for three cases. This is a minimum of 46 failed executions, not a complete history or an all-day failure rate. Retained interval peaks (19 admitted, 18 started-not-finished) include cleanup delay; they are not a census of running harness processes.

The deployed broker was 0.22.4. Published 0.22.6 fixes Claude result correlation; publication does not qualify production behavior. Original empty-result frame sequences were not captured. No active agent was cancelled or duplicated for the audit. Private per-run evidence stays outside the repository.

## Priority and acceptance

| Priority | Change | Evidence / acceptance |
|---|---|---|
| P0 | Reject unsupported static launch parameters synchronously through shared launch checks | Real CLI/MCP/socket return typed refusals with no admitted rows; single/pool grant checks and mutable-binary race retain ownership protections. Included in 0.22.7. |
| P0 | Preserve content-free execution error categories | Database and native-protocol faults remain failures and retain class/stage/numeric codes without source strings; diagnostic sink failure must not replace the outcome. Included in 0.22.7. |
| P1 | Bound optional cache work before terminal delivery | Three Codex cleanup-to-terminal intervals were 558–658 seconds, with cache publication immediately preceding terminal commit. Preserve immutable caches and exclusive runtime-home ownership while bounding/skipping optional work or fencing deferred maintenance. Test restart, resume and slow publication; do not move it after finish without a fence. |
| P1 | Separate process identity loss from ownership-checkpoint persistence errors | The current generic ownership label also covers observer/store errors. Preserve fail-closed identity and verified-leader-only signalling. Inject SQLite busy/constraint and I/O faults; retain a closed stage/category and retry only explicitly transient cases under a finite bound. |
| P1 | Qualify pool recovery with terminal members | Retained deployed state includes open pools with terminal current members, including all-cancelled rosters. Test cancellation, failure/replacement, resume, restart, closure and exactly-once common delivery; enqueue alone is not receipt proof. Never treat mere agreement as result correctness. |
| P2 | Retain a compact incident ledger across session retirement | Latest-100 retention can remove same-day failure evidence. Keep only bounded content-free cause/cleanup/delivery timestamps, with explicit retention; require a numbered migration and historical migration tests if stored in new tables. |
| P2 | Qualify 15 concurrent agents and mixed pools in isolated fixtures | Simultaneous terminal events, tool streams, cancellation, quota exhaustion, replacement, broker restart, SQLite contention and slow optional caches. Record API/control p95, database busy counts, cleanup-to-terminal and terminal-to-receipt, peak owned processes and survivors. No false success, duplicate admission/delivery, lost typed refusal or orphan is acceptable. Native paid qualification follows finite fixtures. |

## Unresolved causes

- Fourteen Claude empty-result executions exited zero without an answer proof. Their original result-frame ordering is unknown; do not claim that the correlated-result fix has reproduced each production incident.
- Ten old generic transport failures discarded their inner execution error. The specifically investigated Codex case continued model/tool activity after analytics-only warnings; neither a model-service network failure nor its exact terminal cause was established.
- Five Claude failures exited 143 in one approximately one-second cluster before their deadlines. Captured command tails contained no accepted cancellation, but this cannot rule out external/manual signals.
- Two Codex ownership failures have no retained inner checkpoint error. Their label does not prove the primary process died.
- No causal claim that concurrency or memory pressure produced these failures is supported by the retained evidence. Measure these in the bounded qualification instead.

## Delivery and rollout

Keep source validation, publication, installation and native receipt verification separate. Deploy only at a protected quiescent boundary; never cancel unrelated active leads for rollout. First run finite fixture qualification, then a bounded native start/resume and pool delivery check against the installed candidate. Keep all uncertain/empty outcomes failed and preserve permanent agent IDs.

## Implementation baseline — October 9, 2026

This is a technical plan, not acceptance of the unfinished changes. The main
baseline is `e8cb76f154ce776465b87381e3cf5a894c10a05b`; the published 0.22.7
payload source is `4f61fc23f019eb0bf069cbace184dd4d49c9795d`. Version 0.22.7 was
installed and qualified on macOS Apple silicon. The store schema is
26 and the pinned Rust toolchain is 1.98.1. Restricted Codex research has actual
web/page/report, command denial, valid native patch denial, traversal refusal,
artifact hash and cleanup evidence. Its final continuation had an effective
240-second deadline; earlier exploratory continuations had 288-second deadlines.
Those proofs do not qualify GLM or a complete Claude initialization inventory.

The preserved cache draft contains five changed files, based on `6f0a0ddb`:
`native_cache.rs`, `native_tree_cache.rs`, `runtime_cache.rs`,
`native_publication.rs` and platform `shared_assets.rs`. Inspection found
nonblocking publication locks and cooperative entry/chunk budgets. An import,
staging copy, verification or filesystem syscall already in progress can exceed
that budget. The prior 16 passing shared-assets tests do not qualify the complete
draft; core/native-publication verification remains outstanding.

Pool recovery already has useful behavior. `pool.activity` derives `cancelled`
and `needs_action` while the durable pool remains open. `settle_open_pools`
rechecks readiness and cleanup. An all-cancelled pool remaining open is
intentional recoverability, not sufficient evidence of a defect. The unfinished
work is to qualify the recovery matrix and fix reproduced gaps.

## Work packages and order

| Package | Outcome | Depends on |
|---|---|---|
| S0 | Preserve the draft and establish one current source baseline | None |
| S1 | Remove optional cache work from live terminal finalization | S0 |
| S2 | Preserve typed ownership-observer failures without weakening identity checks | S0; integrated after S1 |
| S3 | Qualify terminal-member pool recovery and common delivery | S1, S2 |
| S4 | Keep a bounded incident ledger after ordinary history retirement | S2, S3 |
| S5 | Qualify 15 concurrent agents and mixed pools | S1–S4 |
| R1 | Complete Claude/GLM research evidence and version the research skill source | Can proceed independently after S0, subject to live capacity |

Each package ends with its own checks and a reviewable source delta. Integration
and the full release gate follow the complete candidate. A planning document does
not authorize a new release, installation, production cancellation or credential
change.

### S0 — recoverable baseline

1. Preserve the five-file draft as a dated patch plus content hashes before any
   branch update. Include untracked source if it appears; retain the original
   working copy until the recovered candidate is verified.
2. Continue in the existing managed stability worktree. Integrate accepted main
   only after the draft is recoverable. The five edited cache paths have no
   committed changes between their base and accepted main, but compile and
   contract checks must still verify the integrated result.
3. Keep research, personal skills and stability changes in separate reviewable
   commits. A recovery checkpoint may preserve incomplete work; it must not be
   labelled accepted or release-ready.
4. Record the source commit or complete dirty-source fingerprint for every check.
   `archive --verify` verifies its selected committed revision, not unstaged work.

Acceptance: every draft byte is recoverable; no foreign edits, credentials,
immutable releases, state/history or active jobs have been changed. All later
checks identify the same integrated candidate.

### S1 — terminal delivery independent of cache publication

**Decision:** remove optional native cache consolidation from the live
`execute_provider` terminal path. Reuse the existing offline
`consolidate_retained_native` / `storage compact --apply` path for consolidation.
Do not add a background worker, queue, lease type or post-terminal mutation.

Current live order is engine completion, verified cleanup/history/answer, optional
consolidation, then `store.finish`. The consolidation call can hold delivery
behind filesystem work. The intended live order is verified cleanup and required
history/answer proof, `store.finish`, then the existing completion outbox. Required
launch-time shared-asset installation and immutable answer verification stay in
their current protected paths.

Touch points:

- `crates/agent-run-core/src/supervisor.rs`: distinguish live finalization from
  retained-home maintenance; record a closed cache-deferred disposition.
- `crates/agent-run-core/src/runtime_cache.rs`, `native_cache.rs` and
  `native_tree_cache.rs`: retain useful nonblocking draft changes for optional
  maintenance; describe their budgets as cooperative, not hard syscall deadlines.
- `crates/agent-run-platform/src/shared_assets.rs`: preserve the shared required
  and optional import implementation, topology validation and create-only seals.
- `crates/agent-run-core/tests/native_publication.rs` and existing storage/cache
  integration tests: verify the two distinct execution paths.

Offline consolidation still requires the existing broker/service locks, zero
active work, latest-terminal authority, frozen account/role/grant checks and
verified shared-store root. A busy publication leaves the private home unchanged;
incomplete staging keeps or rolls back its journal according to existing recovery
rules. No invalid tree is published, and no incomplete census authorizes deletion.

Tests: busy lock; large tree; deliberately slow import/copy/verification; crash at
publication boundaries; restart and explicit offline retry; resume before/after
compaction; failed/cancelled/successful runs; unchanged answer hash and verdict.

Acceptance: a slow optional publisher is never entered on the live terminal path;
with a healthy store the fixture's cleanup-to-terminal interval is at most one
second even when the offline publisher is deliberately blocked. Contended store
writes remain finite typed failures/deferrals. Offline retry preserves source and
passes manifest, hash, topology and cleanup checks.

Tradeoff: automatic per-run cache deduplication is deferred until explicit offline
compaction. Measure retained private bytes and launch/download cost. This trades
immediate disk savings for prompt, verifiable completion; the existing maintenance
operation retains the storage capability. Automatic consolidation would require a
separately designed ownership fence and is outside this first change.

### S2 — typed ownership checkpoint diagnostics

The concrete loss of detail is in `Process::next` / `receive`: checkpoint errors
become `Event::Failure("engine_process_ownership_failed")`. The checkpoint may
have failed because a snapshot is unavailable or because its store callback
failed; neither observation proves the process died.

1. Preserve the typed cause through the shared adapter event boundary. Use closed
   stages for initial capture, streaming checkpoint and final checkpoint, and
   categories for identity unavailable, observation failure, SQLite contention,
   SQLite constraint, I/O failure and other classified failure.
2. Reuse the safe execution-error classifier and SQLite numeric-code extraction.
   Never persist `to_string`, `Debug`, SQL, paths, task text or native session and
   orchestrator identifiers as diagnostics. Keep each diagnostic under 4096 UTF-8
   bytes.
3. Retry only positively classified transient SQLite busy/locked outcomes using a
   short connection busy timeout and one monotonic budget. Required initial
   persistence splits the existing store allowance (five seconds) across two
   attempts before releasing native task input. Recurring checkpoints retain
   two attempts within 250 ms. Both remain capped by the original run deadline.
   A one-second startup allowance reproduced SQLITE_BUSY under fifteen starts;
   the real 1.1-second held-writer regression requires the original store bound.
   Permanent constraint, identity and I/O errors are not retried. The hot
   supervisor journal uses two-millisecond busy sleeps within that same existing
   monotonic allowance; no new queue, service or configuration knob is added.
   Repeated unchanged Claude/GLM frame session IDs validate without redundant
   journal writes; first observations and genuine changes remain durable.
4. Keep unpersisted snapshots in the current owner object until verified cleanup.
   Advance the recorded revision only after successful persistence. If durable
   ownership/cleanup proof is missing, retain the unresolved ownership record and
   report failure; do not convert it into death or successful cleanup.

Touch points: adapter `io.rs` and `mcp_catalog.rs`; core `supervisor.rs`, `codex.rs`,
`stream.rs` and `lifecycle/reconcile.rs`; store `process_ownership.rs` and existing
ownership tests. Cover every shared checkpoint caller, including startup discovery
and the final checkpoint, rather than adding a guard to only one provider runner.

Tests: a real held SQLite writer; busy then recovery; exhausted retry budget;
constraint error; filesystem observation denial; reused PID; absent snapshot;
leader exit with captured descendants; final persistence failure after a valid
answer; diagnostic sink failure. Exercise ordinary and schema-2 provider routes.

Acceptance: persistence faults have their own safe class/stage; no false
`owner_lost`, unjustified signal, fabricated cleanup, infinite retry or deadline
extension occurs. The diagnostic sink does not replace the original outcome.

### S3 — pool recovery qualification

Keep the existing successful-completion invariant: current roster/proposal,
exact valid votes covering every criterion, successful current execution tips,
joined enrollment and verified lineage cleanup. Failure or unanimous cancellation
must not create a successful common notice.

Use existing `pool.activity`, `settle_pool`, `settle_open_pools`, `pool_replace`,
resume lineage and delivery outbox. Add implementation only for a reproduced gap.
Touch points are store `pool_settle.rs`, `pool_replace.rs`, `pool_enrollment.rs`
and `pool_log.rs`, core lifecycle/service sweeps and common delivery.

| Fixture | Required result |
|---|---|
| All current members cancelled and cleaned | Durable state open, activity cancelled; resume/replacement remains possible; no success notice |
| One failed or mixed-cancelled member | needs_action; existing healthy work remains untouched |
| Terminal member still owns an unresolved attempt | stopping; replacement/settlement refused until genuine cleanup proof |
| Resume after a vote | Stable public agent ID; new execution tip invalidates the old tip's vote |
| Replacement after cleanup | Roster/proposal revisions revalidated; stale votes cannot complete the new roster |
| Crash before/after terminal write and settlement | Existing sweep converges; repeated sweep does not create another completion |
| Missing binding or ambiguous delivery acknowledgement | Waiting/uncertain delivery stays explicit; no duplicate notice or fabricated receipt |
| Valid agreed success | One frozen completion and one logical common notice with actual transport receipt |

Acceptance: every row of this matrix passes through the real broker boundary;
terminal rosters are explainable and recoverable; result correctness remains
separate from formal agreement. Existing pool states and already completed records
are not rewritten to hide old incidents.

### S4 — compact incident ledger

Ordinary retention removes events, attempts and delivery evidence along with
expired sessions. Add a small independent ledger rather than extending transcript
retention or disabling the latest-100/14-day policy.

Proposed storage: schema-27 `incident_ledger`, with an immutable execution/attempt
key and no cascading foreign key to expirable agents. Use closed fields only:
event kind, stage/category, safe numeric codes, terminal outcome, nullable cleanup
verdict and phase timestamps, plus bounded delivery disposition/timestamps. Native
session IDs, prompts, answers, arguments, environment values, account credentials
and free-form error strings are excluded. Keep delivery observations append-only
under deduplicated phase keys; absence remains unknown.

Initial retention policy: 30 days and at most 10,000 records, each at most 4096
UTF-8 bytes. These are proposed bounds, not measured current coverage. Reuse the
existing maintenance cadence and small transaction batches; no new daemon or
scheduler. Start with a bounded reader in existing diagnostics, without a new
general SQL or public MCP tool.

Populate after durable lifecycle events without allowing ledger sink failures to
change the model outcome. Before retention deletes incident-bearing source rows,
ensure their compact record is committed; otherwise defer that deletion batch.
Retry on the existing maintenance schedule. Historical missing causes stay marked
unknown: no backfill invents an original network or cancellation cause.

Touch points: a numbered `027_incident_ledger.sql` migration, current-schema
definition/mirrors and version, store lifecycle/delivery/retention modules, bounded
diagnostic reader, migration/retention/diagnostic tests, and history-retention docs.
Migration installation is a separate acceptance boundary: prove the explicit paired
26-to-27 migration, backup, failure recovery and journal-aware rollback. Do not
assume the schema-1 configuration conversion alone provides that operation; extend
the explicit migration path if required. Never point a 0.22.7/schema-26 binary at a
schema-27 database.

Tests: migration from retained historical schemas and 26; reopen/idempotence;
retirement of more than 100 same-day sessions; lineage and pool protection; ledger
sink failure during prune; finite busy handling; limits/expiry; secret canaries;
restart between outcome and ledger insertion; immutable duplicate phase records;
delivery acknowledgement arriving after terminal state.

Acceptance: retirement removes bulky history while preserving the bounded incident
summary and its unknown fields; errors never leak strings or secrets. The migration
and rollback proof is required before any release containing schema 27.

### S5 — 15-agent qualification

Reuse the existing fake engine and real broker/CLI/socket/MCP tests. Use one
isolated, owned home outside `/tmp` with short socket paths, a distinct shared-store
root and no production credentials. Keep fixtures finite and preserve their
receipts. Fixture scripts must stop only processes they created after fresh birth
identity checks.

Run 15 simultaneous agents for each of these scenarios, then mixed pools:
simultaneous completion, dense tool streams, backpressure, controlled cancellation,
quota/failover fixtures, repeated idempotency keys, terminal resume/replacement,
broker restart, held SQLite writers and blocked optional publication. Include empty
answers, malformed native frames and missing cleanup evidence as expected failures.
For comparable baseline/candidate measurements use three fixed-seed rounds and at
least 200 interleaved read/control requests per round.

Record admission/start, engine-end, cleanup, terminal-write, outbox and exact receipt
timestamps; API/read/control p50/p95/p99; SQLite busy counts and wait duration;
process/descendant peaks and survivors; home/private-cache bytes; reservation and
lease counts. Do not call an admitted count the number of live harnesses.

Proposed warmed-host acceptance targets: read/control acknowledgement p95 <=250 ms
and p99 <=1 s; healthy-store cleanup-to-terminal <=1 s after S1; no more than twofold
p95 regression against the same baseline workload. Measure actual receipt delay
against the configured bounded transport retry window; an enqueue is not a receipt.
These targets remain unqualified until measured on macOS Apple silicon.

Hard correctness gates: no false success, duplicate admission/completion, lost typed
refusal, leaked secret, foreign signal or owned surviving process; every stable
agent/attempt and reservation reconciles. A refused or ambiguous cleanup remains a
failed/unqualified case, not a passing zero-survivor claim.

Only after fixtures pass, perform finite native start/resume and mixed-pool receipt
checks on approved available routes. Use confirmed completion binding and passive
delivery, inspect actual answer/report/cleanup proofs after notification, and verify
the *effective* stored deadline after the configured timeout multiplier. Unavailable
capacity leaves that native route explicitly unqualified.

### R1 — research evidence and skill source

1. Capture safe tool-name metadata from actual Claude initialization. Require only
   the reviewed native web tools and the six private worker tools, with no shell,
   native file tools, delegation, inherited user MCPs, plugins or slash commands.
   Keep raw initialization/session/config payloads out of diagnostic receipts.
2. Run a finite Claude canary: real search/page retrieval, cited report through
   `save_report`, traversal/overwrite refusal, nonblank answer/report hashes and
   verified cleanup. Preserve the existing Codex proof, including the distinction
   between an advertised patch tool and its actual valid-input refusal.
3. At execution time check GLM capabilities, live guide and fresh quota. Use the
   existing approved route with the same enforced research profile. If native web
   or capacity is unavailable, retain that precise qualification gap; do not add
   a shell/MCP proxy or restore omniroute-web.
4. In the separate skills repository, review and version only the canonical
   `role-research/SKILL.md`; verify its bytes match the accepted installed copy.
   Preserve unrelated files. Installed runtime catalogs remain physical copies;
   no broad skill synchronization or global Codex-policy change is needed.

Acceptance: Claude's complete observed inventory is captured, and each tested
provider has real source/report/denial/hash/cleanup receipts bound to its exact
candidate. GLM cannot be labelled qualified from adapter tests or Claude results.

## Verification and release gates

Run focused checks once per coherent changed package, using the pinned toolchain,
committed lockfile and the actual integrated source. Examples of existing targets:

```sh
cargo test --locked -p agent-run-platform --all-features --lib shared_assets
cargo test --locked -p agent-run-core --all-features --test native_publication
cargo test --locked -p agent-run-adapters --all-features
cargo test --locked -p agent-run-store --test process_ownership --test ownership
cargo test --locked -p agent-run-store --test pool_chat --test pool_schema
cargo test --locked -p agent-run --all-features --test provider_public_boundary --test worker_pool_wire
cargo test --locked -p agent-run-store --test retention --test state_migrations --test diagnostics
```

After the full candidate is coherent, run the repository gate, offline dependency
policy, committed-source archive check, release build and transport checks:

```sh
cargo xtask check
cargo deny --offline --locked check
cargo xtask archive --verify
cargo build --locked --release --package agent-run --bin agent-run
node --test scripts/check-desktop-transport.cjs scripts/check-codegraph-probe.cjs
```

Record exact source/artifact hashes, observed effective deadlines, native IDs and
completion/receipt facts. Known doctor false positives and historical ownership
warnings require an accurate before/after comparison; never delete or rename their
evidence to claim a clean doctor. Add no unmeasured family/host qualification.

Choose a fresh release version only after acceptance. macOS Apple silicon remains
the only published host; Linux qualification stays deferred. Publication,
installation, profile activation, schema migration and installed live qualification
remain separate proofs under [releasing.md](releasing.md). Deployment waits for
fresh zero active work and real writer/lock quiescence, with no production job
cancellation. A schema-changing release additionally requires the tested explicit
migration and recovery procedure from S4.

## Local candidate implementation evidence — October 9, 2026

The unpublished local candidate is 0.23.0/schema 27; installed production remains
0.22.7/schema 26. Live optional cache publication is deferred to offline compaction.
Typed ownership phases and bounded SQLite retries are implemented, existing pool
recovery was qualified without changing its success invariant, and the compact
ledger has migration/retention/privacy/diagnostic tests. The explicit paired
26-to-27 upgrade and rollback preserve all application-table values and original
configuration bytes; SQLite backup may change physical page/header encoding.

Ten isolated real-broker cohorts each proved fifteen live native fixture leaders
by fresh PID/token/birth observations and retained real cleanup evidence. They
cover dense tools, empty/malformed answers, quota/nonzero-exit faults, partial
and full cancellation, broker restart, duplicate admission and a held WAL writer.
The optimized production candidate passed 4,000 read/drain requests, including
200 identical-key control acknowledgements; cancellation acknowledgements were
2.2–4.2 ms. Observed quiet p95 was 6.2–10.6 ms and p99 7.2–14.8 ms. The dense
case retained all 3,000 tool results while reducing redundant session events from
6,045 to fifteen. Cleanup-event to terminal-row delay stayed below 949 ms;
closed monotonic stage diagnostics separately identify persistence waits.
Unavailable initial cleanup was injected for fifteen actual native leaders:
all remained lost with unavailable answers, fifteen durable denials and no second
attempt or success. This is fixture qualification, not a paid fifteen-model load.

A finite native Claude candidate run captured a complete eight-tool initialization
inventory (WebSearch, WebFetch and the six private worker tools), real page/report
activity, traversal/overwrite refusal, sealed hashes and confirmed group/descendant
cleanup. A mixed installed-0.22.7 Claude/Codex pool recovered its failed Claude
member through the same stable identity, kept its successful peer, completed one
frozen proposal and delivered one common notice with an actual relay receipt.
GLM remains unqualified: fresh live evidence still reports exhausted quota.
The full candidate gate, comparable baseline and final exact-binary native proof
are required before acceptance; focused checks are not a release claim.

After this stabilization checkpoint, the owner explicitly requested removal of
all agent wall-clock execution limits, timeout margins/parameters and inherited
resume allowances. That separate implementation follows the current changes;
publication or installation of a new candidate is not implied.
