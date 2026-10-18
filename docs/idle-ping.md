# Idle health checks and bounded recovery

A finished inference turn is not a finished agent run. The health monitor detects
unexplained idle without changing explicit finish, answer sealing, task grants or
process cleanup. It does not impose a run lifetime or automatically cancel work.

## Admission and cost

New provider starts accept `idle_ping`:

| Policy | Behavior |
|---|---|
| `observe` (new-start default) | Non-model supervisor probes and actionable diagnostics. No inference is started. |
| `recover` | Also authorizes one model control turn per observed task-tool progress episode, through the same native session and selected model. |
| `off` | No health probes. Use for an explicitly held execution. |

The CLI accepts `--idle-ping observe|recover|off`; MCP and socket start requests
use the same `idle_ping` property. Pool member start requests use that same
contract. Recovery requires explicit-finish mode. Policy is frozen in admission
and inherited by continuation. The free default is omitted from serialized
requests, preserving old replay fingerprints. Existing attempts without a health
initialization marker remain unmonitored; explicit `off` is retained.
An explicitly-requested-only model must not be admitted with `recover` unless its
owner also authorizes these additional inference turns. No model substitution or
paid qualification is implicit in observation.

```sh
agent-run start --provider codex --model YOUR_MODEL --profile review \
  --workdir /absolute/project --task 'Review the patch' --idle-ping recover
```

## Two different checks

Every 60 seconds the broker queues an attempt-bound `ping` for the existing
supervisor command loop. Its receipt proves the supervisor consumed the command
and observed its captured native process identity alive. It neither writes model
input nor proves provider responsiveness or useful model progress.

After 300 seconds idle without an outstanding owned native tool, `observe`
records `idle_without_wait_source`. Under `recover`, a recent valid control
receipt and an empty conflicting-command queue instead permit one `steer` health
challenge. The existing adapter writes it to the same session. Cancellation,
finish, attempt identity, new task activity and pending native work are rechecked
before input. OpenCode additionally obtains fresh native idle/empty-inbox proof.
Unknown delivery is never repeated under a new identity.

The model receives the challenge id and calls private `health_reply` with that
id and `working`, `waiting` or `blocked`. Authentication binds the reply to the
current attempt. Identical replies are idempotent; changed, stale, foreign,
unclaimed or late first replies are refused. The callback cannot finish a run,
assert success, change permissions or create a trusted waiting producer.

The original observation allowance is 120 seconds, including delivery. A missing
reply or return to idle without actual task-tool activity becomes attention. A
pong and health-tool transcript traffic never renew the recovery budget. A new recovery episode requires a later native task turn and task-tool activity.
Inspection performed inside the recovery turn itself cannot rearm polling.
A proven pre-input refusal may also be superseded by new task activity. This is
activity evidence, not a guarantee of task quality or success.

## Real waiting and uncertain waiting

An unmatched attempt-owned native tool call protects a legitimate long job;
there is no age-based cancellation. A synchronous MCP response containing the
word `pending` is already a completed native call, not a registered background
producer. A model cannot hide indefinite idle by claiming `waiting` without
observable work. Application-level asynchronous MCP callbacks still need their
own qualified producer integration; this monitor does not invent that support
or parse prose as ownership.

A service-channel failure is distinct from a missing model response. Unavailable
process identity, clock anomalies, stale input and uncertain native delivery are
not treated as proof of death or permission to replay work.

## Observation and notifications

Agent list/status views include optional `health` with policy, monitor state,
reason, pending-native-tool count, control receipt and correlated model reply.
Bounded waiting sources include transcript cursors and an activity observation
timestamp; these cached read facts are diagnostic, not live producer ownership.
Historical/off runs omit it. MCP list text displays a compact health line.
`needs_attention` is diagnostic: the run remains active until normal finish,
native failure or explicit cancellation, so an installer cannot bypass it.

One fixed broker risk notice is durably queued per attempt/progress/cause through
the existing delivery route. Unbound notices retain `waiting_binding`; unavailable
Bridge delivery does not erase the public diagnostic. No model text or credentials
are included in the notice. An automatic cancellation policy is not enabled.

## Persistence, ownership and checks

The implementation reuses existing `events`, `commands`, worker capabilities and
`worker_notifications`/delivery records. No dependency, database schema or public
broker ping semantics changes. Closed writer operations 93 (`HealthTick`) and 94
(`AcceptHealthReply`) retain broker-only and worker-only authority respectively.
Only new lifecycle initialization enables monitoring; an upgrade cannot add it to
an already admitted historical attempt.

A bounded oldest-due page runs in a broker-owned health maintenance task,
separate from reconciliation. Transcript aggregation and challenge hashing run
through bounded readers off the executor and outside the sole writer. The writer
rechecks indexed attempt, turn and message fences before accepting those facts.
A timed-out or inconsistent read permits only a free control check; idle reports
`activity_observation_unavailable`. Public diagnostics reuse bounded cached facts. Time checks and typed state are durable; a broker restart cannot reset the
model budget or duplicate a queued challenge. Normal process ownership and
cleanup remain with the existing supervisor.

Provider-free tests cover the IDE-pending regression, free observation,
long-running native work, replay/restart, paid-probe budget, frozen policy,
operator/cancel races, clock causality, actual stream input and verified child
group cleanup. Real model calls and installed-runtime qualification are separate;
this source change neither publishes nor installs a release.

See [reliability recovery](reliability-recovery.md) for native completion races, pool waiting, legacy transcript reconstruction and writer observations.


## Native command completion is distinct from a pending return

Codex assigns a PTY `processId` to foreground commands too. That field alone
must never enqueue another model turn. Explicit runs journal terminal command
results once, then correlate the exact owned thread, origin turn and process
with a complete model-facing structured return containing an unfinished
`session_id`. Early background completion is retained until that return is
observed; synchronous results, duplicate terminal frames and delayed foreground
frames do not create a second turn.

The observer reads only the exact native rollout beneath the isolated native
home, through no-follow regular-file descriptors. Lazy file creation is retried
without skipping the first return. It skips pre-admission history,
checks session identity, retains partial JSONL rows, and consumes at most64KiB
per existing supervisor pass. Each row is bounded to4MiB and handle evidence
to4096entries. It parses native tool-return envelopes, never JSON embedded in
stdout. The same path covers fresh threads and resumed native sessions; it does
not enable experimental raw events or change permissions. Missing, malformed,
foreign or oversized observation records one bounded coverage-gap event instead
of guessing that a command was pending. MCP completion handling is unchanged.
