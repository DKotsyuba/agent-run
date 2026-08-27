# OpenCode legacy-pipeline switch — design and measured cost (T051 output)

Input for the next task. Everything here was measured live against real
`opencode serve` processes; nothing is inferred from source reading.

## 1. Verdict of the version probe

No available v1 stable release fixes the MCP gap. `latest` at the time of
probing was 1.18.23; our pin is 1.18.18.

| version | v2 `POST /api/session/{id}/prompt` resolves MCP | legacy `prompt_async?directory=` |
|---|---|---|
| 1.18.18 (pinned) | no | yes |
| 1.18.19 | no | not probed |
| 1.18.20 | no | not probed |
| 1.18.21 | no | not probed |
| 1.18.22 | no | not probed |
| 1.18.23 (`latest`) | no | yes |

Method: each binary taken from its own `opencode-darwin-arm64@<version>` npm
tarball into isolated scratch, version asserted through `GET /global/health`,
isolated `XDG_*`, one stdio MCP server, and a local OpenAI-compatible capture
server standing in for the provider so the exact outbound `tools` array is
recorded. No OmniRoute quota was spent. Harness and raw per-run JSON live in the
session scratchpad (`probe`, `fake_mcp_server`, `fake_provider`, `runs/`).

The bug reproduces on a **stock config with no custom agent at all**, so it is
not caused by anything agent-run generates. Full upstream repro:
[`upstream-opencode-v2-prompt-mcp.md`](upstream-opencode-v2-prompt-mcp.md).

### Additionally ruled out this round

- Creating the session through the legacy `POST /session?directory=<dir>` and
  then prompting through v2 — still broken. **The defect is in the prompt path,
  not in session creation**, so no session-creation trick recovers MCP.
- Stock config, no `agent` block — still broken.

## 2. What the v2 prompt path actually loses

The v2 turn behaves as if neither the agent config nor the MCP block were
resolved for it:

| | v2 prompt | legacy prompt |
|---|---|---|
| MCP tools offered | none | `<server>_<tool>` present |
| MCP child spawned | never | full `initialize`/`tools/list` handshake |
| agent `permission` map filters the tool list | **no** | yes |
| tools offered with our generated agent | 12 raw builtins incl. `bash`, `edit`, `write` | 7, denied tools withheld |

### Safety note — the isolation invariant still holds

The v2 path *offers* `bash`/`edit`/`write` even though the generated agent
denies them, but it still **refuses to execute** them. Probed by driving a
`bash` tool call that would have written a marker file: the file was never
created and no permission ask was raised. So this is prompt-surface noise and
wasted tokens, not a sandbox escape. Worth recording because the offered list
contradicts the config and will mislead anyone reading a transcript.

## 3. The two endpoint families are disjoint scopes

This is the fact that decides the size of the migration. A turn is only visible
through the family that admitted it:

| prompt admitted via | `GET /api/session/{id}/message` | `GET /session/{id}/message?directory=` | `GET /api/session/active` |
|---|---|---|---|
| v2 `/api/session/{id}/prompt` | 2 messages | 0 | reports the session |
| legacy `prompt_async?directory=` | 0 | 2 messages (`user`, `assistant`) | `{"data": {}}` even mid-turn |

Consequence: **swapping only the prompt call is not a viable partial step.**
`wait()` would poll `/api/session/{id}/message`, see zero messages forever, and
every opencode run would time out. Prompt, message list and permissions have to
move together.

## 4. Legacy surface inventory (all probed live)

### 4.1 Prompt — works

```
POST /session/{sessionID}/prompt_async?directory=<workdir>
{ "parts": [ { "type": "text", "text": ... } ],
  "agent": "<agent>",
  "model": { "providerID": ..., "modelID": ... } }
```

Returns `204`. Note `modelID` here, versus `id` in the v2 `ModelRef` — the two
families disagree on that key.

### 4.2 Message list — works, different shape

```
GET /session/{sessionID}/message?directory=<workdir>
```

| | v2 entry | legacy entry |
|---|---|---|
| top-level keys | `agent, content, cost, finish, id, model, time, tokens, type` | `info, parts` |
| role | `type: "assistant"` | `info.role: "assistant"` |
| text | `content: [{type:"text", text}]` | `parts: [{type:"text", text}]` |
| tool state | — | `parts[].state.status` (e.g. `completed`) |
| terminal marker | `finish`, `time.completed` | `info.time.completed` |

This is the **bulk of the migration cost**: `normalize.py` (447 lines) is
written entirely against the v2 shape.

### 4.3 Permission list — works, and the doc's old blocker claim was wrong

`docs/architecture.md` §8 previously said the legacy family has no per-session
permission-list endpoint and that a switch would therefore need the SSE event
stream first. **That is false.** This endpoint exists and is pollable:

```
GET /permission?directory=<workdir>
```

Live sample of a pending ask:

```json
{ "id": "per_04...", "sessionID": "ses_fb...", "permission": "external_directory",
  "patterns": ["/etc/*"], "metadata": {"filepath": "/etc/hosts", "parentDir": "/etc"},
  "always": ["/etc/*"], "tool": {"messageID": "msg_04...", "callID": "call_probe_1"} }
```

Key mapping against the v2 `PermissionV2Request` our broker parses today:

| broker reads | v2 key | legacy key |
|---|---|---|
| action | `action` | `permission` |
| resources | `resources` | `patterns` (plus `always`) |
| id | `id` | `id` (same) |
| session | `sessionID` | `sessionID` (same) |

**Caveat:** the legacy list is *directory*-scoped, not session-scoped. With the
shared managed service, two agents running in the same workdir would see each
other's asks, so the broker must filter on `sessionID` — which every entry
carries.

### 4.4 Permission reply — works, body unchanged

```
POST /permission/{requestID}/reply?directory=<workdir>
{ "reply": "once" | "always" | "reject" }
```

Proven end to end: ask appears → reply `once` → the `read` tool part goes to
`completed`, messages go 2 → 3, pending list returns to 0. The body our broker
already builds (`{"reply": "once"|"reject"}`) is accepted as-is; **only the path
changes.**

### 4.5 Settle signal — needs a new source

`GET /api/session/active` returns `{"data": {}}` even while a legacy turn is
mid-flight and blocked on a permission ask, so `_status()` / `is_settled()` lose
their input. Two measured alternatives:

- **Transcript-terminal** (recommended): `info.time.completed` on the assistant
  message plus `parts[].state.status`. This is the direction T17B already moved
  in ("settle only on terminal transcript, not active-gap").
- **SSE** `GET /event?directory=<workdir>` emits `session.idle` at end of turn.
  Also carries `permission.asked` with `id`, `sessionID` and `permission`, and
  `permission.replied` — so SSE *is* a viable permission transport, it is simply
  not required now that the poll endpoint is confirmed.

Observed SSE event types on one turn: `server.connected`, `message.updated`,
`message.part.updated`, `message.part.delta`, `session.updated`, `session.status`,
`session.diff`, `session.idle`, `permission.asked`, `permission.replied`,
`plugin.added`, `catalog.updated`, `reference.updated`, `integration.updated`,
`server.heartbeat`.

### 4.6 Interrupt — unverified

`POST /api/session/{id}/interrupt` answers `204` after a legacy turn, but it was
not proven to actually abort a *running* legacy turn. Legacy alternatives were
not probed. **Open question for the next task**; per-agent cancel depends on it.

## 5. Cost of the switch

| area | file | change | size |
|---|---|---|---|
| workdir plumbing | `opencode/http.py` | client must know the agent workdir to build `?directory=` | small |
| 4 endpoints | `opencode/http.py` | prompt, message list, permission list, permission reply | small |
| permission shape | `opencode/permissions.py` | `action`→`permission`, `resources`→`patterns`, filter by `sessionID` | small |
| **message shape** | `opencode/normalize.py` | whole module rewritten against `{info, parts}` — `extract_answer`, `has_reported_error`, `normalize_outcome`, token/cost accounting | **large** |
| settle source | `opencode/adapter.py` | drop `/api/session/active`, settle on terminal transcript | medium |
| tests | `tests/test_opencode_adapter.py` | ~30 fixtures pinned verbatim to live v2 captures need legacy twins | **large** |
| cancel | `opencode/adapter.py` | depends on the unresolved interrupt question (4.6) | unknown |

Session creation stays on v2 `POST /api/session` (proven compatible with legacy
prompting), as do service start, isolation proof, config-hash and version pin.

## 6. Recommendation

**Do not migrate yet. File the upstream bug and keep the current v2 pipeline.**

Reasoning, shortest form:

1. The migration's cost is concentrated in `normalize.py` and its live-captured
   test fixtures — the two places where the v1 contract was most expensively
   earned (T041, T17B, T18B). Rewriting both against an endpoint family upstream
   itself calls legacy trades a documented, contained bug for a large, fragile
   change with an unresolved cancel story (4.6).
2. What is actually lost today is bounded: opencode children run without
   `agent_lsp`/`codegraph` but keep `read`/`grep`/`glob`/`skill`. The isolation
   invariant is intact (§2). OpenCode is our third runtime, not the primary one.
3. The upstream repro is now airtight and stock-config — it is cheap for
   maintainers to accept, and a fix upstream costs us a version bump instead of a
   rewrite.

**If the owner decides MCP on opencode is required before upstream moves**, the
migration is well-understood and the blocker the old doc named does not exist:
build it in the order §4.1 → §4.2 → §4.5 → §4.3/§4.4, resolving §4.6 first,
because cancel has no fallback.

Interim honesty fix, independent of either path: the generated config's
`mcp` block for opencode is server-side correct but has no effect on the model.
It stays declared (the service does connect), and §8 of the architecture doc now
records why.
