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
`done`; `partial`, `blocked` and `failed` are terminal non-success
declarations (`worker_partial`, `worker_blocked`, `worker_failed`). Unknown
arguments are refused.

Workers should prefer a structured result; a plain summary stays valid and
keeps its historical payload bytes, digest and answer:

```json
{"status":"done","summary":"Added structured finish; focused tests pass.",
 "changes":[{"path":"src/finish.rs","kind":"modified"}],"commits":["0a1b2c3d"],
 "checks":[{"command":"cargo test -p x","result":"passed","evidence":"logs/test.txt"},
           {"command":"cargo xtask check","result":"not_run","note":"host-only gate"}],
 "unverified":["macOS launch"],"next":"commit and push"}
```

Any field beyond `summary` and `status` makes the call structured. It then
requires `status`, a `summary` of at most 4 KiB (1–3 sentences), `changes`
(entries `{path, kind}` with kind `added`, `modified` or `deleted`, or the
literal `"none"` / `"read_only"`) and/or `commits` (7–64 hex characters),
`checks` (`{command, result: passed|failed|not_run, note?, evidence?}`),
`unverified` (may be empty) and `next`. Optional fields are
`baseline_failures`, `blocked_on` (required when `status` is `blocked`),
`questions`, `artifacts` and a Markdown `report`. Lists and entries are
bounded, paths and commands are single lines, check `evidence` must be a
relative path without `..` or an absolute path under the workdir (checked
lexically, never opened), and all text shares the 64 KiB budget. The same
launch-secret screen as a plain summary applies to every field and to the
rendering. Refusals echo no values.

The broker stores the canonical JSON as the finish intent; its SHA-256 is the
receipt digest. At sealing, `answer.json` receives exactly those bytes and
`answer.md` receives a deterministic Markdown rendering (status, summary,
then Changes, Checks, Baseline failures, Unverified, Blocked on, Questions,
Artifacts, Next and Report sections) ending with
`Structured result: answer.json sha256 <digest>`. The run's answer proof
covers `answer.md`; `answer.json` is trusted only when its bytes match that
digest. `answer` returns the rendering, and completion notices add a compact
worker-result line (see `agent-run doc completion`).
The model supplies no recipient, agent ID, attempt ID, token or path: the
private MCP proxy binds those credentials from its supervisor context.

The callback stores immutable intent and its digest atomically. Identical
same-attempt retries return the original receipt; a different payload conflicts.
Another, retired or revoked attempt cannot finish this run. An accepted finish
fences new turns and worker writes. Its receipt is not owner approval or proof
of cleanup. Success additionally requires the exact sealed answer and verified
owned-process cleanup. Later assistant text and native results cannot replace
the summary. Cancellation retains its existing precedence.

The model puts its complete final report only in `finish` (its summary, or
its structured fields and optional report). The finish
call is its final response: after calling it, no assistant acknowledgement,
repeated summary or closing text is sent. An uncertain receipt permits only
an identical tool-payload retry, without commentary. Newly resolved roles
freeze these shared instructions; existing admitted prompts retain their bytes.

If `output_schema` is set, the summary must contain JSON text whose parsed
value matches that schema, and structured fields are refused. Invalid answers stay retryable without finish intent.
External schema references cannot read files or fetch network resources.

## Idle and wakes

Idle remains active for process/account capacity and keeps its worker capability.
Steering uses the existing durable command queue. Claude-family
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
owned-process cleanup. A peer-message scenario (legacy Run pools, removed in
0.32.0) also demonstrated idle/wake and ordered messages.

Explicit continuations preserve earlier answers and native session identity.
The runtime supplies a new execution boundary so an earlier finish cannot be
mistaken for a ban on newly admitted work. Capability probing of Codex CLI 0.160
found code_mode disabled and experimental: code-cell survival is not qualified.
Native execution sessions are the qualified waiting path; asynchronous MCP
completion requires a producer that actually emits an owned completion.

These observations establish lifecycle behavior, not a fixed token or money
saving. Missing final provider usage remains unknown. Local qualification does
not establish publication or installed qualification.
