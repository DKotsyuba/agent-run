# agent-run

Local supervisor for coding agents. Start Codex, Claude Code, GLM, Qwen
Code children as **durable asynchronous jobs** on your own
machine — with one state store, honest outcome verification, quota
tracking, and three equal access layers: a CLI, an
MCP server, and a Unix-socket JSON-RPC API.

Built for orchestration: one agent (or script, or human) hands out work to
many engine children, keeps working, and collects verified answers later —
across process restarts.

```
you / your agent / your app
        │
   CLI ─┼─ MCP (stdio) ─── JSON-RPC (unix socket)      ← three transports,
        │                                                 one tool surface
   AgentService ── SQLite state (durable agents, events,
        │          transcripts, deliveries, run stats)
   adapters + supervisor
        │
   codex · claude · glm · qwen                          ← engine CLIs you
                                                          already have
```

## Why

- **Durable, not fire-and-forget.** Every agent gets an id and a row in
  SQLite before it runs. Kill your terminal; the child keeps running under
  its supervisor, and `answer <id>` works tomorrow.
- **Verified outcomes.** "Succeeded" is derived from recorded evidence
  (completion sentinels, answer hashes, classified failure kinds) — not
  from an engine's exit code. Error-only replies are classified, not
  celebrated; legacy stall and timeout outcomes remain readable.
- **One tool table, three transports.** The same tool surface is exposed via
  CLI, MCP, and the socket API, generated from a single dispatcher; a
  parity test keeps them from drifting.
- **Isolated children.** Each run gets a generated home: no ambient
  skills, MCP servers, or hooks leak in unless declared in config. What an
  agent may read or write is explicit (`--write`, `--read-root`).
- **Quota-aware.** A capacity collector samples remaining limits per
  provider (native engine data, [codexbar](https://github.com/steipete/codexbar),
  or a local router), computes usage priorities from burn rate and reset time,
  and injects an ordered summary when it changes. The orchestrator chooses the
  first role-compatible route; `limits` remains available for diagnostics.
- **Locked dependencies.** Runtime packages are declared in `pyproject.toml`,
  resolved in the committed `uv.lock`, and release installs verify a hashed
  dependency closure before the application wheel.

## Install

Requirements: Python ≥ 3.14, macOS or Linux, plus the engine CLIs you intend
to drive (`codex`, `claude`, `qwen` — any subset).

| Feature | macOS | Linux |
|---|---:|---:|
| Core CLI, MCP, socket API | yes | yes |
| Environment/file-based runtime auth | yes | yes |
| Keychain auth fallback and launchd helpers | yes | no |
| Optional codexbar / local OmniRoute capacity sources | when installed | when installed |

```bash
pipx install \
  https://github.com/DKotsyuba/agent-run/releases/download/v0.3.1/agent_run-0.3.1-py3-none-any.whl
# or use the same wheel URL with `python -m pip install` / `uv tool install`
```

Versioned wheel and source archives are attached to each
[GitHub Release](https://github.com/DKotsyuba/agent-run/releases). After
installing, confirm the selected version:

```bash
python -c 'from importlib.metadata import version; print(version("agent-run"))'
```

To install a tagged source tree instead of a release artifact:

```bash
python -m pip install \
  git+https://github.com/DKotsyuba/agent-run.git@v0.3.1
```

Then bootstrap the home directory (default `~/.agent-run`, override with
`AGENT_RUN_HOME` or `--home`):

```bash
agent-run init
```

### Configure

Everything lives in one fail-closed file, `~/.agent-run/config.toml`
(unknown keys are rejected — a typo cannot silently disable a rule).
Minimal single-runtime example:

```toml
schema_version = 1

[runtimes.claude]
enabled = true
adapter = "agent_run.adapters.claude.adapter:ADAPTER"
binary  = "/opt/homebrew/bin/claude"          # your engine CLI
home    = "/Users/you/.agent-run/runtimes/claude"
models  = ["sonnet", "opus"]
```

Add more `[runtimes.<name>]` blocks for other engines (`codex`, `qwen`,
`glm`) the same way. Per-runtime options cover auth (env-var
names or file links — never secret values in config), allowed skills,
declared MCP servers, lifecycle hooks, plugins, and the limits source
(`native` / `codex_appserver` / `codexbar` / `omniroute` / `none`).
`priority_multiplier = 1.0` is the optional positive finite weight used by
capacity ordering; it scales only viable routes and never revives an exhausted
window.

Optional `priority_account_multipliers` and `priority_lane_multipliers` tables
override that weight for an account or quota lane: account wins over lane,
which wins over the runtime default. Values are absolute weights, not products;
all must be positive and finite. Shared-pool aliases remain one capacity choice,
using the highest applicable weight rather than adding their weights.

For Codex, `codex_appserver` reads each configured account through a
short-lived local app-server process. Standard and model-specific buckets
(including Spark when the plan exposes it) remain separate routes, and one
account failure does not erase fresh evidence from the others.

**Multiple accounts** (codex): declare labels on the runtime —
`accounts = ["personal1", "personal2"]` —
then log each one in via the engine's own OAuth flow:

```bash
agent-run auth personal2 codex     # opens the browser login once
agent-run start --runtime codex --account personal2 ...
```

Omitting `--account` uses the native global Codex account. Labelled credentials
live in `<home>/accounts/codex/<label>/`; each account gets
its own child-home lineage, and `--account` works identically over MCP
and the socket API. With no accounts declared, nothing changes in account
selection. A configured model is launchable only when the selected account's
app-server roster reports it. `gpt-6-astra` permits only read-only
`role-architect` and `role-review` launches. Delegation reserves this expensive,
high-demand model for the hardest architecture and review decisions; coding
and routine work use other models.

Claude uses its native global CLI credential state when no label is supplied:

```bash
agent-run login claude
```

When Claude declares `accounts`, select one explicitly with
`agent-run login claude --account personal`. Labelled runs use private
`CLAUDE_CONFIG_DIR` state; unlabelled runs use the native global directory.

The built-in operator guide documents every section:

```bash
agent-run doc            # index
agent-run doc config     # config.toml rules
agent-run doc models     # rosters; also: skills, plugins, mcp-servers,
                         # service, releases, migrations, troubleshoot
```

Check the installation:

```bash
agent-run doctor
```

## Quick start (CLI)

The one-shot `start` command submits to the resident Unix-socket daemon so an
accepted asynchronous launch survives the CLI process. Start `agent-run api
serve` first, or install the launchd job below; if the daemon is unavailable,
`start` returns an actionable `BrokerUnavailable` error.

```bash
# start one read-only agent; returns immediately with a durable id
# --timeout remains accepted for compatibility and does not stop execution
agent-run start --runtime claude --model sonnet --profile review \
  --task "Summarize what this repo does in three lines." \
  --workdir ~/projects/myrepo --timeout 600

# block until it finishes (exit code maps the outcome class)
agent-run wait ag-20260831-...

# fetch the verified answer (works any time later, too)
agent-run answer ag-20260831-...
```

Useful verbs beyond that: `status`, `transcript --follow`, `steer`,
`cancel`, `agents` (list), `models`, `limits`, `summary`,
`stats backfill`. All output is line-delimited JSON — pipe it into `jq`.

## Use as an MCP server

`agent-run mcp` is an official MCP SDK stdio server over the resident Unix-socket
daemon. The SDK owns protocol negotiation, request parsing, cancellation, and
EOF lifecycle; each tool callback opens its own broker client, so an MCP client
disconnect never cancels an already admitted durable agent run.
Start the daemon in the foreground with `agent-run api serve`; MCP requires it
to be running and reports `BrokerUnavailable` when it is down. The one-shot
CLI `start` command uses the same resident path for lifecycle safety.

For a long-lived macOS setup, generate and install a launchd job:

```bash
agent-run api launchd --binary "$(command -v agent-run)" > ~/Library/LaunchAgents/com.agent-run.api.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.agent-run.api.plist
```

The proxy exposes the same tool surface as the resident daemon: `start`,
`status`, `answer`, `wait`-free async flow, `cancel`, `steer`, `summary`,
`transcript`, `list_agents`, `models`, `limits`, `capacity_order`, `fast`, and `doc`.

**Claude Code:**

```bash
claude mcp add agent-run -- agent-run --home ~/.agent-run mcp
```

**Codex** (`~/.codex/config.toml`):

```toml
[mcp_servers.agent-run]
command = "agent-run"
args = ["--home", "/Users/you/.agent-run", "mcp"]
```

**Any MCP client** — generic stdio server config:

```json
{"command": "agent-run", "args": ["--home", "/Users/you/.agent-run", "mcp"]}
```

Use an absolute path to `agent-run` if the client's PATH is minimal. The
orchestrating session gets bound to the agents it starts, and terminal
notifications are delivered back to it.

## Use over the JSON-RPC socket API

For programs that are not MCP clients (services, UIs, other tools):

```bash
agent-run api serve          # binds ~/.agent-run/api.sock, chmod 0600
```

Plain JSON-RPC 2.0, method = tool name, plus `tools` (schema discovery),
`ping`, and blocking `wait`. Full integration guide with
a copy-paste Python client: [docs/api.md](docs/api.md).

## What's in the box

| Surface | Command | Notes |
|---|---|---|
| CLI | `agent-run <verb>` | line-JSON output, honest exit codes |
| MCP server | `agent-run mcp` | stdio, shared tool surface |
| JSON-RPC API | `agent-run api serve` | Unix socket, file permissions as auth |
| Operator guide | `agent-run doc` | built into the package |
| Self-diagnosis | `agent-run doctor` | config, binaries, auth, hooks, capacity freshness |
| Capacity collector | `agent-run capacity collect` | + launchd plist generator |
| Capacity priority | `agent-run capacity order` | read-only, role-independent route order |
| State | `~/.agent-run/state.db` | SQLite, versioned schema + migrations |

Engine adapters included: **codex** (app-server JSON-RPC),
**claude** (Claude Code CLI), **glm** (Claude Code CLI pointed at Z.ai's
Anthropic-compatible endpoint), **qwen** (Qwen Code headless with sandbox-safe
macOS Git bootstrap).

## Documentation

- [docs/architecture.md](docs/architecture.md) — how the pieces fit
- [docs/api.md](docs/api.md) — socket API integration guide
- [docs/delegation-authorization.md](docs/delegation-authorization.md) — owner-adopted delegation and context-transfer authorization
- [docs/releasing.md](docs/releasing.md) — version, CI, and GitHub Release procedure
- [CHANGELOG.md](CHANGELOG.md) — user-visible changes by version
- [CONTRIBUTING.md](CONTRIBUTING.md) — development and pull-request checks
- [SECURITY.md](SECURITY.md) — supported versions and private reporting
- `agent-run doc` — operator guide (config, models, releases, …)
- [AGENTS.md](AGENTS.md) — rules for working on this codebase

## License

[MIT](LICENSE)
