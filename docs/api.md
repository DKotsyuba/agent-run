# agent-run JSON-RPC API (Unix socket)

Programmatic access to agent-run for external processes on the same machine.
This is the third transport next to the CLI and the stdio MCP server; all
three expose the same tool surface through one shared dispatcher
(`src/agent_run/dispatch.py`), so a tool that exists in MCP exists here
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
agent-run --home ~/.agent-run api launchd --binary "$(command -v agent-run)" > ~/Library/LaunchAgents/com.agent-run.api.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.agent-run.api.plist
```

The generated service runs `BINARY --home HOME api serve` with `RunAtLoad` and
`KeepAlive` enabled. A foreground process can still be run under another
supervisor when launchd is unavailable.
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

The tool set (same names as the MCP server) is exactly `start`, `resume`,
`cancel`, `steer`, `list_agents`, `answer`, `transcript`, `capacity_order`,
`doc`, `models`, and `limits`.

See [continuations](continuations.md) for native-context `resume`, inherited
authority, idempotency and history availability.

`start.required_constraints` is an optional array of unique policy constraint
names from tool discovery. Omission means no additional requirement. A named
constraint must have enforcement strong enough for that boundary before any
agent row is admitted; advisory evidence and unrelated tool filtering do not
satisfy isolation requirements. Unknown or duplicate names are invalid.

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
concrete runtime/account/quota-lane aliases, current governing windows, raw score,
configured multiplier, manual-reset credit count and its bounded bonus, final
priority, and limiting exact key/reset. The manual reset bonus is only applied
to the stable Codex ``codex`` limit id after the route remains eligible: with
``n`` credits its factor is ``1 + n/(n+1)``. It never creates quota or restores
an exhausted route. The list
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

```python
import json, socket

class AgentRun:
    def __init__(self, path="~/.agent-run/api.sock"):
        import os
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(os.path.expanduser(path))
        self.file = self.sock.makefile("rwb")
        self.next_id = 0

    def call(self, method, **params):
        self.next_id += 1
        request = {"jsonrpc": "2.0", "id": self.next_id, "method": method}
        if params:
            request["params"] = params
        self.file.write((json.dumps(request) + "\n").encode())
        self.file.flush()
        reply = json.loads(self.file.readline())
        if "error" in reply:
            raise RuntimeError(f"{method}: {reply['error']}")
        return reply["result"]

api = AgentRun()
started = api.call(
    "start",
    runtime="qwen", model="opencode/MiniMaxM3", profile="review",
    task="Summarize the diff in one line.",
    workdir="/path/to/repo", timeout_seconds=600,
)
final = api.call("wait", agent_id=started["agent_id"], timeout_seconds=600)
if final.get("timed_out"):
    ...  # still running; call list_agents or wait again
else:
    print(final["content"])  # the agent's answer text
```

Notes for the loop:

- `start` returns immediately with a durable `agent_id`; the agent runs
  detached and survives your process.
- The returned agent view is a snapshot, not a promise of `starting`: a fast
  bootstrap failure may already be terminal. Match concurrent results by agent
  or request ID rather than submission order.
- Bound Codex/Claude chats receive completion notices automatically when
  delivery is configured. The MCP `start` description includes the shared
  notice format and handling contract; `agent-run doc completion` (or MCP
  `doc` with `{"topic": "completion"}`) serves the same contract. The `wait`
  example above is for an unbound API caller, not a bound-chat polling loop.
- The one-shot CLI `agent-run start` submits through this resident socket too;
  it never owns an in-process start worker that would die with the CLI. A down
  daemon is reported as `BrokerUnavailable` instead of falling back locally.
- CLI `start --wait` repeatedly uses the private socket `wait` method and emits
  its terminal answer; interrupting that client leaves the durable run active.
- Use `capacity_order` to choose the first compatible available route.
- Use `models` for current runtime rosters and health; `limits` returns current
  fresh capacity readings without history, forecasts, burn, risk, or advice.
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
| -32602 | invalid params (message carries the validation detail) |
| -32000 | domain error; `error.data.code` holds the agent-run error class (e.g. `UnknownAgent`), plus context fields |
| -32603 | internal error (bounded message, details in server log) |

Treat `-32602`/`-32000` as actionable (fix the request / the referenced
id); `-32603` as a bug to report.

## Versioning and compatibility

- The tool surface is pinned to the MCP surface by a parity test; new
  tools appear in both transports simultaneously. Re-read `tools` after
  an agent-run upgrade instead of caching schemas across versions.
- Restart `api serve` after upgrading the installed package. Operators using
  the optional sealed-release layout restart it after switching
  `~/.agent-run/standalone/current`; ordinary pip/pipx installs use the
  `agent-run` executable on `PATH`.
- Schema version 9 adds immutable per-attempt delivery evidence. Older resident
  processes refuse the migrated database and must be restarted after upgrade.
