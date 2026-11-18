# Reliability recovery and observation

## Native completion waiting

A Codex command with a native `processId` returned a pending process handle.
Its owned terminal item queues one durable completion even if it arrives before
its originating inference turn ends. The queue delivers it only after idle.
Synchronous commands without a process handle do not manufacture a new turn;
replayed/foreign item identities never duplicate a wake or journal result.

The legacy Run pool read guard, pool wait evidence and `pool_progress_stalled`
reason were removed with the pool tools in 0.32.0. A historical pool-loop
corrective steer still queued is refused as stale; stored health snapshots that
carry old pool wait evidence still decode. Old attempts are not silently
enrolled in health checks.

## Legacy Claude transcript display

Use `agent-run transcript AGENT_ID --full --native-history /exact/history.jsonl
--format text` to reconstruct historical anonymous fragments for one execution.
The explicitly supplied file is read with the existing anchored no-follow,
64-MiB bound. Every JSONL line must be complete and session identities consistent.
A known journal tail id must match one unambiguous native full-message id, and
the concatenated already-redacted journal text must match that message exactly.
No native text is copied into the result and raw database rows never change.
Missing, ambiguous, partial or redaction-mismatched groups remain anonymous.
JSON output carries the source hash and reconstructed-message count. This view
is not a new continuity seal, model-consumption receipt or completion proof.

## Writer pressure

The existing `ping` request with `{"writer_health":true}` includes bounded
writer health when served by the resident broker. The empty request retains its
exact historical `{"ok":true}` response. Opted-in diagnostics include: queue jobs/bytes plus the current and last typed callback's
operation identity, kind, queue wait and execution milliseconds. These values
remain readable while SQL is blocked and survive cancellation of the caller
until the actual callback leaves. A finished callback does not prove commit;
resolve the original durable receipt, especially after an unknown outcome.
No SQL, operation payload, token or environment value enters the observation.
Failure diagnostics also retain the original `writer_operation_id` and closed
`writer_op_kind`, alongside the original unknown/not-enqueued outcome, writer
phase, elapsed time and attempts. Recording this identity requires no new SQL
write on the already-failing path; the normal supervisor evidence carries it.

Reconciliation still examines native process evidence on every sweep. It skips
writer admission only for an unchanged cursor or a previously recorded identical
unresolved cleanup reason. New evidence/reasons and proof-gated releases retain
the full writer checks. Historical writer deadline logs lacking operation ids
cannot retrospectively identify the slow statement; receipt churn is measured
load, not causal proof for those incidents. Do not replace investigation with a
larger timeout or blind replay of an uncertain write.
