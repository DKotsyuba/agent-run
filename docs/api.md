# agent-run JSON-RPC API (Unix socket)

Programmatic access to agent-run for external processes on the same machine.
This is the third transport next to the CLI and the stdio MCP server; all
three expose the same tool surface through one shared dispatcher
(`src/agent_run/dispatch.py`), so a tool that exists in MCP exists here
under the same name with the same parameters.

Audience: an integrating agent or developer who has never seen this repo.
Everything needed to connect is on this page.

## Starting the server

```bash
agent-run --home ~/.agent-run api serve
```

- Foreground process; run it under your own supervisor (launchd, tmux, a
  service manager) if you need it long-lived.
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
{"jsonrpc": "2.0", "id": 1, "method": "status", "params": {"agent_id": "ag-..."}}
{"jsonrpc": "2.0", "id": 1, "result": {"agent_id": "ag-...", "status": "running", ...}}
```

One connection may send many requests; on a single connection they are
answered in order. Open several connections for parallelism — dispatch is
serialized server-side, so calls are cheap-interleaved, not truly parallel
(`wait` methods are the exception, see below).

## Method surface

Discover the authoritative surface at runtime:

- `tools` (no params) — returns the full tool table **with JSON schemas
  for every tool's parameters**. This is the contract; prefer it over any
  hardcoded list.
- `ping` (no params) — `{"ok": true}`; liveness probe.

The tool set (17 at the time of writing, same names as the MCP server):
`start`, `status`, `answer`, `cancel`, `steer`, `summary`, `transcript`,
`list_agents`, `models`, `limits`, `fast`, `doc`, and the workflow verbs
`workflow_start`, `workflow_status`, `workflow_answer`, `workflow_cancel`,
`workflow_resume`.

Two extra methods exist only on this transport:

- `wait` — params `{"agent_id": "...", "timeout_seconds": 240}` (timeout
  optional, positive number; omitted = wait forever). Blocks until the
  agent is terminal, then returns the answer envelope (same shape as the
  `answer` tool). If the watcher timeout expires first, the result is a
  normal reply carrying `"timed_out": true` and the current status — not
  a JSON-RPC error.
- `workflow_wait` — same contract with `run_id` for workflow runs.

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
    ...  # still running; poll `status` or wait again
else:
    print(final["content"])  # the agent's answer text
```

Notes for the loop:

- `start` returns immediately with a durable `agent_id`; the agent runs
  detached and survives your process.
- Model rosters and health come from `models`; remaining quota and risk
  from `limits`. Check them before fanning out work.
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
