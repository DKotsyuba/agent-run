# Load stability plan

## Evidence and qualification boundary

An October 7, 2026 Europe/Belgrade audit preserved 228 execution rows: 45 failed/timed-out executions across 30 stable agents, plus one startup failure found only in a supervisor log. Retention removed detailed evidence for three cases. This is a minimum of 46 failed executions, not a complete history or an all-day failure rate. Retained interval peaks (19 admitted, 18 started-not-finished) include cleanup delay; they are not a census of running harness processes.

The deployed broker was 0.22.4. Published 0.22.6 fixes Claude result correlation; publication does not qualify production behavior. Original empty-result frame sequences were not captured. No active agent was cancelled or duplicated for the audit. Private per-run evidence stays outside the repository.

## Priority and acceptance

| Priority | Change | Evidence / acceptance |
|---|---|---|
| P0 | Reject unsupported static launch parameters synchronously through shared launch checks | Real CLI/MCP/socket return typed refusals with no admitted rows; single/pool grant checks and mutable-binary race retain ownership protections. Implemented in this candidate. |
| P0 | Preserve content-free execution error categories | Database and native-protocol faults remain failures and retain class/stage/numeric codes without source strings; diagnostic sink failure must not replace the outcome. Implemented in this candidate. |
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
