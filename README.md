# agent-run

`agent-run` is a self-contained Rust supervisor for Codex, Claude Code, and
GLM coding agents. It runs children as durable asynchronous jobs, records
verified outcomes in SQLite, tracks provider capacity, and exposes the same
tool surface through its CLI, MCP stdio server, and Unix-socket JSON-RPC API.

The broker and its release artifact do not require Python. Engine CLIs remain
external dependencies and must be installed and authenticated separately.

## Release targets

Native releases are published only for macOS Apple silicon
(`aarch64-apple-darwin`), the qualified release platform. Linux x86-64 remains
an unqualified, non-blocking validation target; its release and qualification
are deferred. Keychain and launchd integration remain macOS-only.

## Install

Use the same command for a fresh installation or an update:

```bash
curl -fsSL https://github.com/DKotsyuba/agent-run/releases/latest/download/install.sh | sh
```

Or use wget:

```bash
wget -qO- https://github.com/DKotsyuba/agent-run/releases/latest/download/install.sh | sh -s -- --downloader wget
```

**Availability:** the installer ships with the next release after 0.13.3.
Releases through 0.13.3 do not contain its script/helper. Until that release is
published, build from source below; the commands above require the new assets.

The installer verifies the download and release manifest, retains immutable
versions under `~/.agent-run/standalone/releases`, and places a launcher in
`~/.local/bin`. Add that directory to `PATH`. No Cargo, Python or sudo is needed.
Repeat the command to select the latest compatible version. Existing config,
accounts and database are preserved; updates include a state/config backup.

Stop the broker and other agent-run services before updating. Active agents,
incompatible configuration or a schema migration requirement block the update.
For an existing 0.13.x schema-2 home, first use the new candidate's
[paired configuration/database migration](docs/provider-migration.md#existing-schema-2-homes).
Services are not stopped or restarted automatically. Afterward, restart your
configured services and run `agent-run doctor`.

To pin a version or choose directories, download `install.sh` and run:

```bash
sh install.sh --version X.Y.Z --home "$HOME/.agent-run" \
  --prefix "$HOME/.agent-run/standalone" --bin-dir "$HOME/.local/bin"
```

For a fresh install, run `agent-run init`, then configure engine CLIs and
accounts. The installer does not install engines or sign in to providers.

Build from source with the pinned Rust toolchain (also usable before publication):

```bash
release_root="$(mktemp -d)"
cargo xtask release build-native --output "$release_root" --version 0.14.0
"$release_root/releases/0.14.0/bin/agent-run-deploy" install \
  --release "$release_root/releases/0.14.0" --version 0.14.0 \
  --prefix "$HOME/.agent-run/standalone" --home "$HOME/.agent-run" --bin-dir "$HOME/.local/bin"
```

Use a new version for changed source: the installer never overwrites an
existing version with different bytes. The temporary build directory may be
removed after installation; the selected release is copied into the prefix.

The home defaults to `~/.agent-run`; override it with `AGENT_RUN_HOME` or
`--home`.

## Configure

Configuration lives in `<home>/config.toml`. Schema 2 declares native harnesses,
named providers, explicit models and account bindings. See
[provider configuration](docs/provider-config-v2.md) for a complete example.
Each provider selects its external quota executable:

```toml
# Inside an existing provider declaration:
limits_source = "exec"
collector = { command = "/bin/bash", args = ["/opt/agent-run/collectors/glm.sh"], source = "glm-quota" }
```

The same contract accepts Python scripts or native programs: private JSON on
stdin, normalized quota JSON on stdout. The native archive includes external
scripts under `collectors/`; those examples require Bash/jq, plus curl for HTTP.
See [quota collectors](docs/quota-collectors.md) for credentials, dependencies,
the output contract and migration from retired Lua/app-server settings.
Credentials stay in protected account stores. The broker checks configuration
every minute and on requests; invalid revisions keep the last valid settings.

Declare shared background backends under `services.<id>`. The broker warms them
before harness launch, retains them while agents are active, and stops them
after thirty minutes of inactivity. Native MCP clients connect to their usual
backend; their tool permissions remain defined by the role. See
[managed services](docs/managed-services.md), including the CodeGraph example.

Initialize and inspect the installation:

```bash
agent-run init
agent-run doctor
```

## Run the broker

`start` is broker-owned: it submits through the resident Unix socket and never
falls back to a worker owned by the short-lived CLI process.

```bash
agent-run api serve
```

For a long-lived macOS installation:

```bash
plist="$HOME/Library/LaunchAgents/com.agent-run.api.plist"
agent-run api launchd --binary "$(command -v agent-run)" \
  | plutil -extract plist raw -o "$plist" -
launchctl bootstrap "gui/$(id -u)" "$plist"
```

Future Linux builds can run the same `agent-run api serve` command under an
external service manager such as systemd. Linux is not a qualified or published
0.12.3 platform, and agent-run does not generate systemd units.

## Use the CLI

```bash
agent-run models                       # providers, explicit models, roles, standing
agent-run start --provider codex --model gpt-6-sol --profile review \
  --task "Review this repository." --workdir "$PWD"

agent-run agents
agent-run answer ag-...
agent-run transcript ag-...
agent-run transcript ag-... --follow --format text
agent-run resume ag-... --task "Continue with the highest-priority finding."
```

Output is line-delimited JSON. `transcript --follow` streams a live view:
model text, tool activity, and results as they arrive, exiting when the agent
reaches a terminal state and its journal is drained; Ctrl-C exits only the
viewer and never cancels the agent. `--format text|json` picks the rendering,
defaulting to text on a terminal and JSON when output is piped. In text mode
each journal fragment is sanitized and written to the pipe immediately as it
arrives — there is no line buffering — with one newline per message or tool
row; untrusted roles, names, and content are stripped of terminal escape
sequences. The Codex runtime journals deltas and completion tails of one
message under a shared item identity, so they render as one continuous row;
the Claude runtime journals assistant fragments and the completion tail of one
message under its native message id (or one producer-owned fallback per
message boundary when the engine omits ids), so each message renders as one
continuous row and distinct messages stay distinct. Other commands
include `steer`, `cancel`, `models`, `limits`, `capacity order`,
`delivery status`, and `doc`.

## Use the MCP server

The MCP process is a thin stdio proxy over the resident broker:

```json
{"command":"agent-run","args":["--home","/absolute/path/to/agent-run-home","mcp"]}
```

It exposes `start`, `resume`, `cancel`, `steer`, `list_agents`, `answer`,
`transcript`, `capacity_order`, `doc`, `models`, and `limits`.

## Use the socket API

The broker binds `<home>/api.sock` with mode `0600`. It accepts newline-framed
JSON-RPC 2.0 and provides the tool methods plus `ping`, `tools`, and `wait`.
See [docs/api.md](docs/api.md).

## Reliability contract

- SQLite admission precedes execution, so accepted jobs remain durable.
- Success requires answer proof, completion evidence, and verified cleanup.
- Generated runtime homes include only declared auth bridges, tools, skills,
  hooks, plugins, and policy.
- The broker checks configuration hashes every 60 seconds and on requests.
- Release directories are immutable, checksummed, and gated by `COMPLETE`.

Development and release procedures are in [CONTRIBUTING.md](CONTRIBUTING.md)
and [docs/releasing.md](docs/releasing.md).
