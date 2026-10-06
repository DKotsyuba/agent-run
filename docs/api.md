# agent-run JSON-RPC API (Unix socket)

Programmatic access to agent-run for external processes on the same machine.
This is the third transport next to the CLI and the stdio MCP server; all
three expose the same operator tool surface through one shared dispatcher
(`crates/agent-run-core/src/dispatch.rs`), so a tool that exists in MCP exists here
under the same name with the same parameters.

Workers use a [separate MCP surface](worker-mcp.md) containing only
`notify_orchestrator`. Its private `worker/notify` broker route is not in operator
discovery and requires the current run/attempt capability on every call.

Audience: an integrating agent or developer who has never seen this repo.
Everything needed to connect is on this page.

## Starting the server

Foreground:

```bash
agent-run --home ~/.agent-run api serve
```

For the recommended long-lived macOS setup, generate a keep-alive launchd
plist, then bootstrap it for the current user:

```bash
plist="$HOME/Library/LaunchAgents/com.agent-run.api.plist"
agent-run --home "$HOME/.agent-run" api launchd --binary "$(command -v agent-run)" \
  | plutil -extract plist raw -o "$plist" -
launchctl bootstrap "gui/$(id -u)" "$plist"
```

The launchd command emits a JSON result. Extract its `plist` field as shown;
redirecting the full JSON object does not create a valid launchd plist.

The generated service runs `BINARY --home HOME api serve` with `RunAtLoad` and
`KeepAlive` enabled. It sets the job's soft open-file limit to 65,536; the API
broker, supervisors, and runtime children inherit that limit instead of
launchd's default 256. A foreground process can still be run under another
supervisor when launchd is unavailable.

Future, unqualified Linux builds can run the foreground command under an
external service manager. Linux is not a qualified or published release target,
and agent-run does not generate systemd units. A user unit can use:

```ini
[Unit]
Description=agent-run broker

[Service]
ExecStart=/home/you/.local/bin/agent-run --home /home/you/.agent-run api serve
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=default.target
```

Install it as `~/.config/systemd/user/agent-run.service`, adjust both absolute
paths, then run `systemctl --user enable --now agent-run.service`.

- Socket path defaults to `<home>/api.sock` (with `--home ~/.agent-run`
  that is `~/.agent-run/api.sock`). Override with `--socket PATH`.
  macOS caps `AF_UNIX` paths at ~104 bytes — keep the path short.
- The home must be an owned private directory (mode `0700`) and the socket is
  `chmod 0600`. The broker checks each peer's UID, and the built-in client checks
  the broker's UID against its effective UID. There is no network listener or
  general operator token. The internal worker notification route additionally
  verifies an attempt-specific capability; it cannot choose its recipient.
- If a live server already owns the socket, a second `api serve` refuses
  to start (native ownership lock plus a connect probe). A stale socket file
  left by a crash is replaced automatically.
- `SIGTERM`/`SIGINT` shut the server down and remove the socket file.
- Startup takes a couple of seconds (service construction); wait for the
  socket file to appear before connecting.

## Wire protocol

JSON-RPC 2.0, one JSON object per newline-terminated line, UTF-8, both
directions, over a `SOCK_STREAM` Unix socket. Maximum line size 1 MiB.
Notifications (requests without `id`) get no reply. Batch arrays are not
supported (`-32600`).

`method` is the tool name; `params` is a single object whose fields are the
tool's arguments.

```json
{"jsonrpc": "2.0", "id": 1, "method": "list_agents", "params": {"limit": 1}}
{"jsonrpc": "2.0", "id": 1, "result": {"items": [...], "revision": 42, ...}}
```

Terminal agent responses include `delivery.last_attempt` when Codex queue made
at least one delivery attempt. The additive nullable object records a safe
classifier, executable provenance, argv shape without values, duration, exact
return code or spawn errno, error class, original output byte counts,
truncation flags, bounded redacted stdout/stderr tails, and whether a remote
message id was observed. Each tail is at most 4096 UTF-8 bytes. Messages,
session ids, argv/environment values, and credentials are never persisted.

One connection may send many requests; on a single connection they are
answered in order; no cross-connection ordering is promised. Separate
connections execute concurrently within independently bounded control and read
groups. Durable start/resume/cancel/steer admission uses the control group;
ordinary reads use the read group, so slow reads cannot consume control permits.
Long polls hold neither group's permit. The server caps
connections and queued calls, reserves one connection slot for parsed control
methods, rejects ordinary overload with JSON-RPC code `-32001`, and
returns `-32002` when its request deadline expires. Input frames remain limited
to 1 MiB; idle reads and response writes also have finite deadlines. A client
holding the reserved slot without completing its first frame is disconnected
within 0.5 seconds.

The socket path is fenced by a lifetime native file lock. A pre-existing socket
is reclaimed only after a refused or missing-endpoint connect result and an
unchanged-inode check. A slow or malformed ping is never evidence that an owner
is dead. Shutdown stops accepting connections and gives admitted handlers a
bounded grace period before closing remaining streams.

## Method surface

Discover the authoritative surface at runtime:

- `tools` (no params) — returns the full tool table **with JSON schemas
  for every tool's parameters**. This is the contract; prefer it over any
  hardcoded list.
- `ping` (no params) — `{"ok": true}`; liveness probe.

Cooperative pools use five strict methods from the same table: `start_pool`
(two to five ordinary start requests sharing one goal and acceptance criteria,
admitted atomically; returns the stable `pool_id` and each member's `agent_id`,
name, role and status), `pool_post` (an operator message stamped as the
orchestrator), `pool_replace` (replace a terminal, fully cleaned member; same
`request_id` returns the same new member) and `pool` (status plus a cursor-paged
log), and `list_pools` (read-only discovery). The first three ride the control
lane; `pool` and `list_pools` use the read lane. Refusals use the shared error codes
with the pool code leading the message (`member_busy`, `pool_completed`,
`pool_not_found`, ...). A pool completes only by formal verification (every
member voted ready on one proposal, ended successfully, cleanup verified) and
delivers exactly one common notice; that is not proof the result is correct.

Over MCP, every tool result renders as one compact plain-text page (see
`assets/mcp/*.txt.j2`) instead of the structured JSON below; `start`/`resume`
additionally keep `structuredContent` `{"agent_id": ..., "sequence": ...}` so
automatic PostToolUse binding can pin its exact admission. The numeric counter
is transport metadata, not another agent identifier; orchestrators use only
`agent_id`. This socket API and the CLI
keep the structured contracts documented here.

The current MCP contract, byte/row budgets, protocol-versus-business error
channels, and status-preserving presentation fallback are documented in
[MCP presentation](mcp-presentation.md). Current discovery is generated with
`cargo xtask contract export` and checked against both live registries;
`schemas/mcp-registration.json` describes commands and environment names
without registering anything with a host.

Shared MCP/socket discovery advertises 15 tools: `start`, `resume`, `cancel`,
`steer`, `list_agents`, `answer`, `transcript`, `doc`, `delegation_guide`,
`limits`, `start_pool`, `pool_post`, `pool_replace`, `pool`, and `list_pools`.
`models` and `capacity_order` remain call-only compatibility methods in the
same registry/dispatcher; their original structured CLI/socket responses and
schema-1 behavior remain available, including direct MCP calls by known name.

`list_pools` accepts a strict object with optional `state` (`"open"` or
`"completed"`; omitted/null means all), `limit` (integer 1..200, default 50)
and `offset` (nonnegative integer, default 0). It returns one consistent read
snapshot, an exact filtered `total`, and `items` ordered by `created_at`
descending with `pool_id` descending as the tie-breaker. `next_offset` is null
at the end; `complete` also holds for an offset beyond the total. Offset pages
may shift if pools are admitted or purged between calls.

Each item has `pool_id`, `state`, a `goal` excerpt capped at 512 UTF-8 bytes
on a character boundary, `goal_truncated`, `created_at` (UTC epoch seconds),
`last_seq` (greatest retained log sequence, zero for an empty log), nullable
`completed_at`, `roster_revision`, `members_count`, `ready`, nullable
`current_proposal_seq`, and `members` in ascending slot order. Member rows
contain only `slot`, `name`, `role`, stable `agent_id` and `tip_status`.
`ready` counts only members whose derived `pool` status has `counts: true`;
raw ready decisions invalidated by a changed roster, tip or failed execution
do not count. Completed summaries reuse the frozen completion evidence even
if a member resumes later. Full criteria, proposal snapshots and log bodies
are available through `pool`, and are absent from discovery.

```json
{"jsonrpc":"2.0","id":2,"method":"list_pools","params":{"state":"open","limit":1,"offset":0}}
{"jsonrpc":"2.0","id":2,"result":{"items":[{"pool_id":"pool-20261004-120000-0123456789","state":"open","goal":"Ship the observer","goal_truncated":false,"created_at":1791115200.0,"last_seq":7,"completed_at":null,"roster_revision":1,"members_count":2,"ready":1,"current_proposal_seq":4,"members":[{"slot":1,"name":"Reviewer","role":"review","agent_id":"ag-20261004-120000-0123456789","tip_status":"running"},{"slot":2,"name":"Builder","role":"implement","agent_id":"ag-20261004-120000-abcdef0123","tip_status":"running"}]}],"total":2,"offset":0,"limit":1,"next_offset":1,"complete":false}}
```

The CLI equivalent is `agent-run pools [--state open|completed] [--offset N]
[--limit N]`, alias `list-pools`; default output is JSON and `--text` prints
the compact MCP page. No worker capability is accepted. Discovery has no
long-poll: operator pool posts do not advance the existing event revision,
so it would miss changes to `last_seq`. Members provide the join with
`list_agents`; `AgentView` remains unchanged.

See [continuations](continuations.md) for native-context `resume`, inherited
authority, idempotency and history availability.

`start.required_constraints` is an optional array of unique policy constraint
names from tool discovery. Omission means no additional requirement. A named
constraint must have enforcement strong enough for that boundary before any
agent row is admitted; advisory evidence and unrelated tool filtering do not
satisfy isolation requirements. Unknown or duplicate names are invalid. The
effective requirement is the union of the role's `required_constraints` and the
request's, for every profile kind; a caller can add a boundary but no role or
request can drop one the other declared.

`request_id` replay is scoped to the caller namespace in the original request.
A later PostToolUse notification binding does not change that identity. Clients
that omit `orchestrator` share the unbound namespace across fresh connections.

`list_agents` accepts optional `after_revision` and `wait_seconds`. When the
current event revision is not newer, the call waits up to 60 seconds and wakes
as soon as a committed event advances it. The returned `revision` becomes the
next cursor, so terminal completion is observable without a notification
worker or polling at a fixed interval. Pages also return a
`message_revision` transcript watermark: journal rows (transcript text and
native tool counts) never advance the event revision, so an observer that
needs to wake on progress passes it back as optional `after_message_revision`
together with `after_revision`. Journal-only wakes are paced to at most one
per second per waiting follower; event wakes stay immediate.

The CLI equivalent is `agent-run agents [--active] [--offset N] [--limit N]`,
and `agent-run agents --follow` runs one persistent process instead of
respawning polls: it prints the first page immediately and then one NDJSON
snapshot per meaningful change (status, phase, name, usage, tool counts,
failure, delivery and answer facts), never reprinting a page whose only
movement is observation time. Ctrl-C ends the viewer only; supervised agents
keep running, and a closed output pipe terminates the process.

Durable steering outcomes are truthful about evidence. `steer` still queues
one command and returns the same acknowledgement; the command's recorded
outcome distinguishes a correlated native reply (`accepted:true` — the
engine accepted the input, which is not a claim the model consumed it), a
correlated native rejection (`accepted:false`, `native_rejected`), a refusal
to send under bounded backlog pressure (`accepted:false`,
`backlog_pressure_unsent` — provably nothing was written), and every bounded
end after a possible write (`accepted:null` with the finite reason, e.g.
`uncertain_timeout` or `uncertain_backlog_pressure` — the input may have been
taken). Interleaved engine notifications are never dropped to make room for
a control exchange: retention is fixed and bounded, a full backlog stops the
exchange before reading, and a late correlated reply is recorded as
metadata only.

`start` and `resume` accept optional `display_name` (CLI `--name`, alias
`--display-name`): a trimmed, nonblank UTF-8 human label of 1–64 Unicode
scalar values, with no control or unsafe directional formatting. Punctuation
and non-ASCII text are accepted. Omitted or null start labels mean unnamed;
omitted or null resume labels inherit the parent, and an explicit label replaces
it. The normalized label participates in request-id replay: changing it with the
same key returns `Conflict`. Public agent views expose it as nullable `name`;
labels confer no authority and are never inferred from task text.

Views expose nullable `usage` from the latest execution's existing `run_stats`
row and `usage_cumulative` across lineage executions. Missing or pruned history, missing rows or
unreported metrics remain null; an explicitly observed zero stays zero. Each
cumulative metric is available only when every lineage execution reported it.
Usage objects contain measurements, source and recording time, never internal
execution IDs. Codex resumes record a comparable parent-thread baseline before
launch and subtract it from native cumulative counters; absent, foreign-session,
foreign-model or decreasing counters remain unknown. Codex turn counts are null
unless the native protocol reports a counter, never inferred from messages,
turn IDs, tools or execution count. Compact MCP output uses `?` for unknown
measurements and names incomplete lineage evidence.

`transcript` accepts optional representation options while keeping raw rows
and forward `cursor`/`limit` semantics unchanged. `view` is `raw` (default) or
`blocks`: blocks group only consecutive journal rows that share one known
native reference (`raw_ref`), role, name and one execution/attempt scope, so
streamed fragments of one native message read as one entry with exact
whitespace and content, `first_seq`/`last_seq`, and a `fragment`-safe
`next_cursor`/`resume_cursor`. Rows without a native reference never merge,
and equal references of different executions stay separate; a `starts_block`
flag marks safe boundaries without exposing run or attempt IDs.
`tail_blocks: 1..=200` returns the last blocks chronologically with an
exclusive `previous_cursor` (pass it as `before_cursor` for older pages);
blocks are the paging unit, so a limit bounds blocks, never fragments inside
one. Reads are bounded index scans: no whole-journal load, and no raw history
or spool file is modified. Oversized or spooled content is never silently
dropped — `content_complete: false` and `omitted_bytes` say what is missing,
and `raw_ref` stays opaque.

Transcript rows and agent views also carry native tool evidence. A
`tool_result` row may carry `error` (boolean) with `error_source` naming the
allowlisted native field that supplied it: Claude `is_error`, Codex command
`exitCode`/`status`, or Codex MCP `status`/`error`. Only explicit native
markers set the flag; error-shaped words, agent exit codes and unrelated
statuses never do, and unreported results stay `null` (unknown), distinct from
an observed `false`. Agent views include nullable `tool_counts` for the latest
execution — `calls` counts unique native invocation IDs (started, completed
and fragment rows of one invocation count once), `failed` counts explicitly
failed invocations and stays `null` while any result is unknown, and
`unknown_results` counts results without consistent evidence. Executions
recorded before the versioned native observers, or with observed coverage
gaps, report all counts as `null` rather than a fake zero.

Agent views returned by `list_agents` include
`effort` — the reasoning effort requested at launch, or `null` when the
request did not set one.

Those views also include nullable `cleanup` evidence from the latest owned
process cleanup observation: attempted `signals`, `scope`, `group_gone`,
nullable `descendants_gone`, `confirmed`, and nullable `process_group_id`.
Confirmation requires the original group and the readable pre-signal owned set
to be gone. Page projections resolve progress, warnings, delivery evidence and
cleanup in one batched state query.

New rows also expose immutable `policy` evidence with the runtime/platform and
one entry for every known constraint: actual enforcement, support, whether the
caller required it, exact scope, and reason. Historical rows return `null`.

With schema 2, `capacity_order` accepts an optional exact `model` filter and
returns provider-only advice: `schema_version`, `config_revision`,
`capacity_revision`, `ranked_at`, and ordered `providers`. Each provider has its
id, `priority_multiplier`, nullable `score`, and models with `native_model`
and cached `quota` standing. It does not expose accounts or legacy physical-route
aliases. An unknown model is a `ValidationError`. The view reads one committed
snapshot without collecting quota, reserving capacity or starting work.

The CLI equivalent is `agent-run capacity order [--model MODEL]`. Select a
compatible model and canonical role using `delegation_guide`; `models` is a
structured compatibility read for existing clients.
Schema 1 retains its historical physical-route output and rejects a model
filter with `Unsupported`.

Three methods exist only on this socket transport: `tools` and `ping` above,
plus:

- `wait` — params `{"agent_id": "...", "timeout_seconds": 240}` (timeout
  optional, positive number; omitted = wait forever). Blocks until the
  agent is terminal, then returns the answer envelope (same shape as the
  `answer` tool). If the watcher timeout expires first, the result is a
  normal reply carrying `"timed_out": true` and the current status — not
  a JSON-RPC error.
A pending `wait` does not block other requests: run it on its own
connection and keep issuing calls on another.

## Typical integration loop

Open a Unix stream socket in the client language, write one compact JSON-RPC
object plus `\n`, and read one reply line. A shell smoke can use BSD netcat:

```bash
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"ping"}' \
  | nc -U ~/.agent-run/api.sock
```

For asynchronous work, call `start`, retain the returned `agent_id`, then call
`wait` on a separate connection. If `wait` returns `"timed_out": true`, the
agent is still running and the same id can be waited on again.

Notes for the loop:

- `start` returns immediately with a durable `agent_id`; the agent runs
  detached and survives your process.
- The returned agent view is a snapshot, not a promise of `starting`: a fast
  bootstrap failure may already be terminal. Match concurrent results by agent
  or request ID rather than submission order.
- Bound Codex/Claude chats receive completion notices automatically when
  delivery is configured. The MCP `start` description includes the shared
  notice format and handling contract; `agent-run doc completion` (or MCP
  `doc` with `{"topic": "completion"}`) serves the same contract. A notification ID is any nonblank
  string of at most 512 UTF-8 bytes, and delivery attempt evidence accepts only
  its declared fields. The `wait`
  example above is for an unbound API caller, not a bound-chat polling loop.
  Successful members of an open pool are represented by its single common pool
  completion notice; failed or otherwise non-successful members still receive
  individual completion notices.
- The one-shot CLI `agent-run start` submits through this resident socket too;
  it never owns an in-process start worker that would die with the CLI. A down
  daemon is reported as `BrokerUnavailable` instead of falling back locally.
- CLI `start --wait` repeatedly uses the private socket `wait` method and emits
  its terminal answer; interrupting that client leaves the durable run active.
- Use `limits` for stored quota windows, percentages, reset times and freshness.
  With schema 2 it adds `ranking`: provider/model standing, numerical scores and
  provider multipliers from the same committed snapshot, config revision and
  advice clock as `items`/`observed_at`. All governing windows and exhaustion
  facts participate; a healthy short window does not override a weekly zero.
  Schema 1 keeps its historical window response. MCP text omits account/pool
  identities and stays within the existing whole-response byte/row bounds.
- `models` and `capacity_order` retain their structured compatibility responses
  for existing CLI/socket clients, without joining the advertised MCP catalog.
- `delegation_guide` (optional exact `provider`, `model`, `profile` filters;
  schema 2 only) returns one compact plain-text
  routing guide — providers in capacity order with each exact model id, cached
  quota standing, admissible profiles, params, restrictions, and configured
  guidance prose — instead of reading the full `models` JSON just to pick a
  route. Its result is a JSON string here and real MCP text content on the MCP
  transport; the CLI equivalent is `agent-run delegation-guide` with optional
  `--provider`, `--model`, and `--profile` filters.
  Unknown filters are `ValidationError`; omitted filters keep default guidance.
  Before delegating a task, the orchestrator must call it and read the result
  before choosing provider, model, effort or profile. Routing advice belongs in
  provider/model `recommendations`, not a separately maintained delegation skill.
  The tool does not select a model, authorize new access, or replace the
  start/resume completion and permission contracts.
- `answer` re-fetches a finished agent's result any time later by id —
  results are durable, a dropped connection loses nothing.
- `start.write` is a compatibility intent flag; the selected canonical role
  owns actual write permission. Choose a role with `write = false` for read-only
  work: request `"write": false` is not an independent read-only guard, and
  `"write": true` cannot grant writes beyond the role.

## Errors

| Code | Meaning |
|---|---|
| -32700 | unparseable line, or line over 1 MiB |
| -32600 | not a JSON-RPC 2.0 request; batch array; bad `id` |
| -32601 | unknown method |
| -32602 | invalid params (message carries the validation detail); a more specific class such as `PathEscapeError` is kept in `error.data.code` |
| -32000 | domain error; `error.data.code` holds the agent-run error class (e.g. `AgentNotFound`, `selection_busy`, `no_eligible_account`, `quota_exhausted`), plus context fields |
| -32603 | internal error (bounded message, details in server log) |

Treat `-32602`/`-32000` as actionable (fix the request / the referenced
id); `-32603` as a bug to report. The CLI (`error.type`) and MCP tool
errors report the same class the broker returned; only an unknown class is
rendered as `RuntimeError`.

## Versioning and compatibility

- The tool surface is pinned to the MCP surface by a parity test; new
  tools appear in both transports simultaneously. Re-read `tools` after
  an agent-run upgrade instead of caching schemas across versions.
- Restart `api serve` after switching the verified sealed release at
  `~/.agent-run/standalone/current`.
- The current database schema is version 25, reached through the paired
  `agent-run config migrate`. Older resident processes refuse a newer database
  and must be restarted after an upgrade migrates it.
