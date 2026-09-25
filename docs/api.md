# agent-run JSON-RPC API (Unix socket)

Programmatic access to agent-run for external processes on the same machine.
This is the third transport next to the CLI and the stdio MCP server; all
three expose the same tool surface through one shared dispatcher
(`crates/agent-run-core/src/dispatch.rs`), so a tool that exists in MCP exists here
under the same name with the same parameters.

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
external service manager. Linux is not a published 0.12.3 target, and agent-run
does not generate systemd units. A user unit can use:

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
- The socket is `chmod 0600`; file permissions are the whole auth model.
  There is no network listener and no token.
- If a live server already owns the socket, a second `api serve` refuses
  to start (it probes with `ping`). A stale socket file left by a crash is
  replaced automatically.
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
answered in order. Open several connections for parallelism — dispatch is
serialized within two bounded owner lanes. Durable start/resume/cancel/steer
admission uses the control lane; list, answer, transcript and capacity reads
use a separate lane, so a slow read cannot starve cancellation. The server caps
connections and queued calls, reserves one connection slot for parsed control
methods, rejects ordinary overload with JSON-RPC code `-32001`, and
returns `-32002` when its request deadline expires. Input frames remain limited
to 1 MiB; idle reads and response writes also have finite deadlines. A client
holding the reserved slot without completing its first frame is disconnected
within 0.5 seconds.

The socket path is fenced by a lifetime native file lock. A pre-existing socket
is reclaimed only when connecting returns `ECONNREFUSED` and the inode is still
the one inspected. A slow or malformed ping is never evidence that an owner is
dead. Shutdown rejects submissions, resolves queued calls, closes active
connections, and closes each thread-affine service in its owner context.

## Method surface

Discover the authoritative surface at runtime:

- `tools` (no params) — returns the full tool table **with JSON schemas
  for every tool's parameters**. This is the contract; prefer it over any
  hardcoded list.
- `ping` (no params) — `{"ok": true}`; liveness probe.

Over MCP, every tool result renders as one compact plain-text page (see
`assets/mcp/*.txt.j2`) instead of the structured JSON below; `start`/`resume`
additionally keep a tiny `structuredContent` `{"agent_id": ...}` so automatic
PostToolUse binding stays machine-extractable. This socket API and the CLI
keep the structured contracts documented here.

The tool set (same names as the MCP server) is exactly `start`, `resume`,
`cancel`, `steer`, `list_agents`, `answer`, `transcript`, `capacity_order`,
`doc`, `models`, `delegation_guide`, and `limits`.

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
worker or polling at a fixed interval.

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

`capacity_order` takes no parameters. It returns fresh non-exhausted physical
quota routes in descending priority, plus deferred evidence, exhausted
`omitted` routes, and `unavailable_runtimes`. Each working route includes its
concrete runtime/account/quota-lane aliases, governing windows, raw score,
configured multiplier, manual-reset credit count and its bounded bonus, final
priority, and limiting exact key/reset. The manual reset bonus is only applied
to the stable Codex ``codex`` limit id after the route remains eligible: with
``n`` credits its factor is ``1 + n/(n+1)``. It never creates quota, changes
forecasts, or restores an exhausted route. The list
is role-independent: callers still choose the first alias whose models fit the
task. `insufficient_diversity` is true when fewer than two working physical
choices remain; the routes list is still authoritative and may contain one or
zero entries.

The equivalent human-facing command is `agent-run capacity order`. Its first
route is the highest capacity priority; it only reports a read-only order and
never launches work. The orchestrator still selects a compatible role and model
alias from that route's aliases.

Two extra methods exist only on this transport:

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
  string of at most 512 characters, and delivery attempt evidence accepts only
  its declared fields. The `wait`
  example above is for an unbound API caller, not a bound-chat polling loop.
- The one-shot CLI `agent-run start` submits through this resident socket too;
  it never owns an in-process start worker that would die with the CLI. A down
  daemon is reported as `BrokerUnavailable` instead of falling back locally.
- CLI `start --wait` repeatedly uses the private socket `wait` method and emits
  its terminal answer; interrupting that client leaves the durable run active.
- Use `capacity_order` to choose the first compatible available route.
- Use `models` for current runtime rosters and health; `limits` returns stored
  capacity projections without making provider calls.
- `delegation_guide` (no params, schema 2 only) returns one compact plain-text
  routing guide — providers in capacity order with each exact model id, cached
  quota standing, admissible profiles, params, restrictions, and configured
  guidance prose — instead of reading the full `models` JSON just to pick a
  route. Its result is a JSON string here and real MCP text content on the MCP
  transport; the CLI equivalent is `agent-run delegation-guide`.
- `answer` re-fetches a finished agent's result any time later by id —
  results are durable, a dropped connection loses nothing.
- Set `"write": true` in `start` params only when the agent must edit
  files; default is read-only.

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
- The current database schema is version 17, reached through the paired
  `agent-run config migrate`. Older resident processes refuse a newer database
  and must be restarted after an upgrade migrates it.
