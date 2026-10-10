# Explicit worker completion

New provider admissions use explicit completion. The private, attempt-bound
`agent_run_worker.finish` callback supplies the final answer. A native turn
ending leaves the same admitted run and native session idle; it does not
publish success. There is no execution deadline, idle-age expiration or
continuation-count allowance.

```json
{"summary":"Changed the parser; checked the regression; nothing unfinished.","status":"done"}
```

`summary` is a nonblank string of at most 64 KiB UTF-8. `status` defaults to
`done`; `blocked` and `failed` are terminal non-success declarations. Checks
and unfinished work belong in the summary. Unknown arguments are refused.
The model supplies no recipient, agent ID, attempt ID, token or path: the
private MCP proxy binds those credentials from its supervisor context.

The callback stores immutable intent and its digest atomically. Identical
same-attempt retries return the original receipt; a different payload conflicts.
Another, retired or revoked attempt cannot finish this run. An accepted finish
fences new turns and worker writes. Its receipt is not owner approval or proof
of cleanup. Success additionally requires the exact sealed answer and verified
owned-process cleanup. Later assistant text and native results cannot replace
the summary. Cancellation retains its existing precedence.

If `output_schema` is set, the summary must contain JSON text whose parsed
value matches that schema. Invalid answers stay retryable without finish intent.
External schema references cannot read files or fetch network resources.

## Idle and wakes

Idle remains active for process/account capacity and keeps its worker capability.
Pool messages and steering use the existing durable command queue. Claude-family
native notifications must pass the owned-session replay contract. Codex native
command/MCP completions must match the exact started item, thread, originating
turn, kind and tool name. Completion receipts are deduplicated per attempt;
events arriving during another turn stay queued until its boundary. A wake
starts another turn on the same native thread, without another admission.

A synchronous MCP call needs a producer that actually emits an asynchronous
native completion; this runtime does not fabricate completion events. Unknown
or foreign events cannot create a turn or certify task success.

The public view retains `status=running` while exposing `phase=idle` or
`phase=closing`, the internal `turn_count` and observed `idle_seconds`. Public
admission `sequence` advances only for an explicit resume, not internal wakes.
A real native exit without an accepted callback is non-success. No elapsed
silence alone terminates an idle worker.
Without a pending producer, the run also remains idle. The runtime sends no
inferred nudge or repeated question; only an admitted event starts another turn.

After a receipt, a short native closing grace collects the final turn/usage when
available, then normal birth-verified cleanup settles owned processes. This is
an IPC/cleanup bound after declared completion, never an agent lifetime timeout.
If final usage cannot be observed, it stays unknown instead of reporting an
earlier partial turn as complete billing. A broker restart does not replace a
healthy supervisor. A dead supervisor's acknowledged finish is reconciled only
with ownership and cleanup proof; unknown cleanup leaves closing unresolved.
If the supervisor died before sealing native history, the summary can be
recovered while continuation remains `continuation_unavailable`. Existing seals
are retained verbatim; recovery never invents a new baseline from changed files.

## Compatibility and continuation

Mode is frozen at admission. Historical requests without this field restore as
legacy; an update does not change an already admitted run's contract. Explicit
`resume` retains native history and the previous immutable summary in its own
execution record, using the parent's mode and current permitted authority.

For a legacy integration, use `--legacy-completion` (equivalently
`--explicit-finish=false`) or `explicit_finish=false` in MCP/API start input.
Legacy mode keeps native-result/EOF completion and the original five-tool
private catalog. Explicit mode adds `finish`; restricted research also retains
its confined `save_report`, without gaining shell or arbitrary writes.

Schema 29 adds attempt lifecycle/intent and native-wake deduplication tables.
The migration preserves historical request JSON and answer/ownership evidence.
Publication and installation remain separate acceptance boundaries.

## Native qualification

On macOS, isolated default-mode runs on Codex, Sonnet and GLM each launched one
120-second native command, ended the inference turn and received its actual
completion in the same native session. Each used two runtime turns with about
118–121 seconds idle, no polling, an exact callback/answer hash match and confirmed
owned-process cleanup. A peer-message scenario also demonstrated idle/wake,
ordered messages, current-proposal ready votes and formal pool completion.

Explicit continuations preserve earlier answers and native session identity.
The runtime supplies a new execution boundary so an earlier finish cannot be
mistaken for a ban on newly admitted work. Capability probing of Codex CLI 0.160
found code_mode disabled and experimental: code-cell survival is not qualified.
Native execution sessions are the qualified waiting path; asynchronous MCP
completion requires a producer that actually emits an owned completion.

These observations establish lifecycle behavior, not a fixed token or money
saving. Missing final provider usage remains unknown. Local qualification does
not establish publication or installed qualification.
