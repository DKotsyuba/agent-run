# Upstream bug report draft — `POST /api/session/{id}/prompt` resolves no MCP clients

Hand this to the opencode maintainers as-is. It is written to stand alone: it
names no agent-run internals and reproduces on a stock config.

---

## Summary

A turn admitted through the durable v2 endpoint
`POST /api/session/{sessionID}/prompt` runs with an **unresolved session
context**: no MCP client is instantiated, so no MCP tool is offered to the
model, and the configured agent's `permission` map is not applied to the tool
set either.

The same session, same config, same server, prompted through
`POST /session/{sessionID}/prompt_async?directory=<dir>` resolves MCP normally
and applies the agent's permission map.

This is not a configuration error: it reproduces on a **stock config with no
custom agent at all**, and on every 1.18.x release we tested.

## Affected versions

Reproduced on `opencode` v1 stable **1.18.18, 1.18.19, 1.18.20, 1.18.21,
1.18.22 and 1.18.23** (darwin-arm64, `opencode-darwin-arm64` npm artifacts,
each binary verified via `GET /global/health`). 1.18.23 was the `latest`
dist-tag at the time of writing.

## Environment

- macOS (darwin arm64), binaries taken straight from the
  `opencode-darwin-arm64@<version>` npm tarballs.
- Isolated `XDG_CONFIG_HOME` / `XDG_DATA_HOME` per run, so no user state leaks in.
- Server started as `opencode serve --hostname 127.0.0.1 --port <port>`, with
  `OPENCODE_SERVER_PASSWORD` set (HTTP Basic `opencode:<password>`).

## Reproduction

### 1. Config (`$XDG_CONFIG_HOME/opencode/opencode.json`)

Stock — no `agent` block, no `default_agent`, one local MCP server. `provider`
points at any OpenAI-compatible endpoint you can observe; the point of the
repro is to read the `tools` array in the outbound request.

```json
{
  "$schema": "https://opencode.ai/config.json",
  "share": "disabled",
  "autoupdate": false,
  "model": "probeprov/probe-model",
  "provider": {
    "probeprov": {
      "name": "Probe",
      "npm": "@ai-sdk/openai-compatible",
      "options": { "baseURL": "http://127.0.0.1:<capture-port>/v1", "apiKey": "probe-key" },
      "models": { "probe-model": { "id": "probe-model" } }
    }
  },
  "mcp": {
    "probe_mcp": {
      "type": "local",
      "command": ["/usr/bin/python3", "/abs/path/fake_mcp_server", "/abs/path/marker.txt"],
      "environment": {},
      "enabled": true
    }
  }
}
```

`fake_mcp_server` is any stdio MCP server exposing a single tool
(`probe_ping`). Have it append a line to `marker.txt` on startup, so you can
also observe whether an MCP child was spawned at all.

### 2. Start the server and create a session

```
POST /api/session
{ "model": { "providerID": "probeprov", "id": "probe-model" },
  "location": { "directory": "<workdir>" } }
```

### 3a. Prompt through the durable v2 endpoint — BROKEN

```
POST /api/session/{sessionID}/prompt
{ "prompt": { "text": "Say the word ready." } }
```

Observed outbound `tools` array (1.18.23, stock config), 12 entries:

```
apply_patch, bash, edit, glob, grep, question, read, skill,
todowrite, webfetch, websearch, write
```

`probe_mcp_probe_ping` is absent. `marker.txt` is **never created**: the MCP
child is never spawned for this turn.

### 3b. Prompt the *same* server/config through the legacy endpoint — WORKS

```
POST /session/{sessionID}/prompt_async?directory=<workdir>
{ "parts": [ { "type": "text", "text": "Say the word ready." } ],
  "model": { "providerID": "probeprov", "modelID": "probe-model" } }
```

Observed outbound `tools` array (1.18.23, same stock config), 12 entries:

```
bash, edit, glob, grep, probe_mcp_probe_ping, question, read, skill,
task, todowrite, webfetch, write
```

`probe_mcp_probe_ping` is present, and `marker.txt` records the full MCP
handshake:

```
spawned
rpc:initialize
rpc:notifications/initialized
rpc:tools/list
```

## Expected vs actual

| | Expected | Actual on `POST /api/session/{id}/prompt` |
|---|---|---|
| MCP clients resolved for the turn | all `enabled` servers in `mcp` | none |
| MCP tools offered to the model | `<server>_<tool>` for each | none |
| MCP child process spawned | yes | no |
| Agent `permission` map applied to the tool set | yes | no (see below) |

### Secondary symptom — the agent's permission map is ignored too

With a config that defines an agent whose `permission` map sets
`bash/edit/write/webfetch/websearch/question` to `deny`:

- legacy `prompt_async` offers 7 tools: `glob, grep, probe_mcp_probe_ping,
  read, skill, task, todowrite` — denied tools correctly withheld;
- v2 `prompt` offers the same 12 raw builtins as the stock case, including
  `bash`, `edit` and `write`, which the agent denies.

The v2 turn behaves exactly as if **no agent and no MCP config were resolved**
for it — which is why we believe both symptoms share one root cause in the v2
prompt path's session/context resolution, rather than being two bugs.

## Things ruled out

Each of these was tested and does **not** change the outcome:

- `agent.<name>.tools` map with an explicit wildcard for the MCP tools;
- `permission` entries for the MCP tools;
- warming `GET /mcp` (or `GET /mcp?directory=<workdir>`) before prompting — the
  server does report the MCP server as `connected` and does spawn the child for
  that call, but the subsequent v2 prompt still resolves nothing;
- using the stock `build` agent instead of a custom one;
- omitting the custom agent entirely (stock config, shown above);
- both directory scopes, and an extra undeclared `?directory=` on the v2 prompt;
- a second turn in an already-warm session;
- creating the session through the legacy `POST /session?directory=<dir>`
  endpoint and then prompting through v2 — still broken, so the defect is in
  the prompt path, not in session creation.

## Additional observation — the two paths write to different scopes

After a turn, the messages are only visible through the endpoint family that
admitted it:

| prompt admitted via | `GET /api/session/{id}/message` | `GET /session/{id}/message?directory=` |
|---|---|---|
| `POST /api/session/{id}/prompt` | 2 messages | 0 |
| `POST /session/{id}/prompt_async?directory=` | 0 | 2 messages (`user`, `assistant`) |

This may be the same root cause surfacing: the v2 prompt appears to run in a
directory/workspace scope distinct from the one `location` established at
session-create time.
