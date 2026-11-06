# Explicit worker completion and event-driven idle

Status: delivered on macOS Apple silicon. Source9d004b7217b91213c3a01ea66e6e0d4b09eb55d7
is merged in PR #112, published as0.25.0 and installed with schema29.
Exact-source local acceptance and hosted release attempt2 passed. Five GitHub
provenance subjects bind the exact source/run/attempt. The configured production
API/capacity/delivery services are running; fresh API/MCP and TUI checks passed.
Installed Codex, Sonnet and GLM each completed a120-second native command with
two turns and about117–122 seconds idle, no polling calls, exact finish/answer
hashes and verified cleanup. Each then resumed in the same native session,
finished the new execution and retained its original answer. Own test services
were stopped; history, other profiles and global policy were preserved.
Doctor retains the same two compaction-key false positives and three historical
ownership warnings. Failed publication attempt1 and the installer readiness
fixture flake remain recorded below. Native code_mode and Linux stay unqualified.

## Required behavior

An agent run completes successfully only after its own authenticated internal MCP `finish` call. The callback supplies the final summary. Ending an inference turn means idle, not successful completion. An admitted event starts another turn in the same run and native session, without a replacement agent or cold model launch.

The final answer comes from the accepted finish payload. Later assistant text, native background results and duplicate callbacks cannot replace it. Existing native history remains available for explicit continuation after a completed run.

The owner's unlimited-execution decision remains in force: no run wall-clock timeout, percentage margin, time allowance inherited by resume, or age-based idle termination. Active and idle duration are observations only. Operational IPC/process-cleanup bounds remain separate from agent lifetime.

## Inputs and evidence

- [Owner brief](../experiments/agent-run-explicit-finish-brief.md).
- [Transcript audit](../experiments/agent-ide-transcript-audit-20261010/report.md), sections 2–3, reports 5,452 empty-wait turns and 45.2% of measured input in a 68-worker sample. These are the audit's measurements, not a new measurement or a monetary savings estimate.
- [Native hook probes](../experiments/agent-ide-hook-probes-20261010/report.md), especially P1/P2/P3/P5 and their limitations.

The brief's references to a 480-second default, a 20% margin and a rejected timeout argument describe older source assumptions. Installed 0.24.2 uses schema 28 and deliberately has no lifetime argument. Restoring those limits is outside this plan. Wake-versus-poll monetary estimates and Codex code-mode exec-cell survival remain hypotheses requiring evidence; native exec session survival has been observed separately.

## 1. Completion contract

Add `finish` to the existing private worker MCP surface, not a public tool that can finish an arbitrary agent. Reuse its current run/attempt token, role grant and channel binding. A callback from another, retired or revoked attempt is refused.

Implemented closed arguments: `summary`, `status` (done/blocked/failed, default
done). Checks and unfinished work belong inside the summary; there are no extra
arguments. A blocked or failed finish is terminal but never successful. Retain
the summary with its true outcome.

Validate a nonempty bounded answer using the existing answer validator. If `output_schema` is set, validate the canonical answer against it; do not bypass it by treating arbitrary text as a structured result. Reject invalid payloads in the callback without ending the run.

Persist finish intent and the canonical summary/hash atomically under existing storage ownership. Same-attempt identical retries return the original receipt; conflicting payloads cannot overwrite it. The model does not need to supply another agent ID or gain new filesystem/network rights.

Return the callback receipt before closing its tool channel. Fence new turns, preserve the native history identity, settle owned processes and publish the terminal outcome only with the required answer and cleanup evidence. A declared done plus unconfirmed cleanup is not success. Restart must reconcile an accepted callback exactly once.

## 2. Run state versus turn state

Define the logical flow as running → idle → running, repeated without a resume-count limit. Explicit finish moves to closing, followed by the verified terminal outcome. Native exit/failure and operator cancellation remain distinct unsuccessful terminal paths when no finish was accepted.

Idle remains an active run: it is not resumable as a second execution, does not expire its worker authority, and can still receive control/event messages. An explicit resume after a finished run creates the next execution using the retained history and current permissions as today. Internal wakes change turn counters, not the public admission sequence.

Freeze how idle contributes to existing process/account capacity. The initial implementation keeps resident-resource reservations and returns a typed capacity refusal rather than evicting idle agents by age. Control/wake handling must remain available when every admitted agent is idle. Separate inference capacity from resident limits only if measurement shows the need.

Persist new state/mode/finish data through a numbered migration if needed. Historical execution records keep their original meaning and bytes/proofs; mode is captured at admission so an update never changes an active legacy run's completion contract.

## 3. Native adapters

Claude CLI: keep stdin and the owned process open after a turn result unless finish, actual failure or cancellation requires closure. Native result frames describe turns and usage, not the run's final answer. Accept self-started turns only through verified native ownership/session/lifecycle evidence. Notification-looking user text or an unknown external replay is insufficient.

Codex app-server: retain the process/thread after `turn/completed`. Deliver the next event through the same native thread. Before changing its completion loop, probe whether an outstanding code-mode exec cell survives turn end. Record exact client versions and outcomes. If it does not survive, keep that command awaited within the turn through the supported bounded tool API or provide a qualified supervisor-owned job path; never pretend a discarded cell is still pending.

Both adapters preserve cancellation, quota-switch rules, replay guards, role enforcement and birth-verified cleanup. Closing an execution does not delete conversation history.

## 4. Durable wakes and pending jobs

Reuse the existing command/outbox paths for pool messages and steer. Add only the missing typed integration for native task/script completion, approved notification triggers and asynchronous MCP results whose producers actually support completion delivery.

Each accepted event has an owner, stable deduplication identity, payload bound and explicit delivery/consumption state. Enqueue alone is not proof that the model received it. Serialize turns per native session; if an event arrives during a turn, queue/coalesce it and deliver after the turn boundary. Cover the event-before-idle race and restart without duplicate model turns.

A generic synchronous MCP call cannot automatically become a background job. For supported producers, retain the admitted job handle and bounded result reference, then wake once on completion. Others keep their native awaited behavior. No model polling or sleep loop is required to hold a run open.

After accepted finish, events cannot reopen the completed run or mutate its summary. Record late events with a safe disposition or route them to an explicitly admitted continuation. Never invent another worker to consume them.

## 5. Idle without a pending producer

The implemented policy sends zero inferred nudges, following the owner's
no-extra-questions scope. If no work is pending and finish is absent, remain idle
and expose that state. Only an admitted event starts another turn. No repeated
model wake or elapsed-age termination is introduced.

If the native process actually exits without finish, publish a distinct non-success reason such as `ended_without_finish`, with normal cleanup proof. This is not a timer. Owner cancellation also remains available.

## 6. Worker guidance and rollout

Update the canonical worker prompts/role skills and physically materialized catalogs: end a turn while waiting; use finish only for the final result; no repeated pool-read/exec-status/sleep turns to preserve lifetime. Provide finish to every role using this contract, including read-only and restricted research roles, without adding unrelated native tools.

Qualify the new mode in isolated opt-in runs first. Existing frozen legacy runs retain end-of-turn completion. After evaluation, make explicit finish the default for new admissions, following the owner's clarified target. Keep a declared compatibility path and historical-mode attribution rather than silently changing old runs. Release/install are separate acceptance boundaries.

## Implementation sequence

| Stage | Deliverable | Acceptance |
|---|---|---|
| F0 | Exact-version Claude/Codex probes and closed contracts | Exec-cell survival known; native turn/exit/notification shapes retained privately |
| F1 | Private finish intent, immutable summary and receipts | Auth/refusal, schema validation, idempotency, conflict and restart tests |
| F2 | Durable idle/turn lifecycle and native adapters | End of turn cannot succeed a run; same run/session survives multiple wakes |
| F3 | Typed event/pending-job integration | No lost wake, duplicates, cross-worker messages or model polling |
| F4 | Roles, migration and measured opt-in evaluation | All supported roles work; legacy behavior preserved; idle/live metrics truthful |
| F5 | Default rollout and installed qualification | Exact published payload, fresh protocol/process checks and real provider scenarios |

Use the existing domain dispatcher, worker capability compiler, store, supervisor and adapter boundaries. No separate scheduler, remote analytics service or generic workflow language is needed.

## Required qualification

- Claude/Sonnet and GLM each start a two-minute background command, end the turn, receive its actual completion and call finish. One run, same native session, exact callback summary, verified cleanup and zero inference turns during the wait.
- A pool member idles and wakes from a peer message with no empty pool-read polling turns. Message delivery/consumption and deduplication are proven.
- A background subagent/native task completion after a parent turn is handled without ambiguous success or replacing the callback summary. Unsupported/forged origins still fail closed.
- An idle worker with no pending work remains idle without an inferred nudge or execution deadline. Native exit without finish is non-success.
- Invalid output schema, duplicate/conflicting callbacks, simultaneous finish/wake/cancel, stale attempt tokens, broker restart during closing and uncertain cleanup preserve truthful outcomes.
- Legacy mode retains its current result/EOF semantics. Resuming after finish preserves the immutable previous summary and native history, without a separate continuation cap.
- Codex exec-cell survival is documented; the implemented waiting path is tested. Do not substitute a mock or a catalog for the real host proof.
- Report active/idle/model-turn time, wake counts, empty-wait turns and provider-accurate new/cached input and output. No fixed token or money savings claim before measurement.

## Release acceptance observation

The implementation and release notes are committed and merged in PR #112.
Exact source 9d004b7217b91213c3a01ea66e6e0d4b09eb55d7 passed the local
workspace, dependency-policy and integration acceptance gates. Annotated
v0.25.0 points to that accepted source. Release run 38081945511 attempt 1
built the native payload but failed its full source gate; the wrapper retained
only hashes and omitted the failed assertion from hosted logs. The original
failure receipt is preserved. A single full rerun, attempt 2, subsequently passed the full source/build/publish
gates. Actual installation and qualification also passed, as recorded above. Follow-up: make failed acceptance output and
failed gate artifacts diagnosable without weakening lifecycle assertions.
Production was 0.24.2/schema 28 at that failure observation; final production is
0.25.0/schema 29.

The ordinary-parallel local reproduction failed the unchanged installer
watchdog fixture at its three-second PID-readiness boundary; its all-feature
pass had succeeded. The same fixture passed in isolation in 3.55 seconds,
with all startup, hard-escalation and child-cleanup assertions unchanged.
This does not establish which assertion failed in GitHub attempt 1. Preserve
both observations; follow up fixture startup determinism and failure diagnostics.

Final installation was qualified, and a paired migration backup was retained
in the operator's private Agent Run home.
