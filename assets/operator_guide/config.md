# config

Schema 2 separates native harnesses, named providers and global accounts.
Configuration contains nonsecret references, never token values. Register each
account before using its id in a provider binding.

```toml
schema_version = 2
[harnesses.codex]
binary = "/absolute/path/to/codex"
home = "/absolute/path/to/agent-run/codex"
[harnesses.claude-code]
binary = "/absolute/path/to/claude"
home = "/absolute/path/to/agent-run/claude"

[providers.codex]
harness = "codex"
connection = { kind = "native" }
auth_family = "openai"
limits_source = "exec"
collector = { command = "/bin/bash", args = ["/opt/agent-run/collectors/codex.sh"], source = "codex-appserver" }
[[providers.codex.models]]
id = "gpt-6-sol"
[[providers.codex.bindings]]
label = "main"
account = "acct-codex-native"
```

Quota collection executes the configured command and literal arguments.
Python, Bash and native programs share one JSON stdin/stdout contract. Stdin
contains account identity, explicit model scope, time and protected authentication
context. Stdout must be a single version-1 quota document. Never put credentials
in arguments or script logs. Example scripts remain external files under
`collectors/` in the native archive and require Bash/jq, plus curl for HTTP.

The collector's `timeout_seconds` defaults to 30 (1–300 allowed). `env_from`
grants additional environment variable names. Missing commands, nonzero exit,
timeout, oversized output and invalid quota facts fail without replacing the
last good observations. `limits_source = "none"` disables collection. Former
`lua` and `codex_appserver` sources require migration; there is no built-in
fallback. Account aliases share observations and failure backoff.

Canonical Markdown profiles select skills, MCP servers, permissions and required
constraints. Shared `[mcp.<name>]` declarations contain a transport, command,
arguments and optional environment names; profiles select their ids. Harnesses
own native settings, hooks, plugins and workspace roots. Provider models carry
explicit native aliases, effort settings, recommendations and restrictions.

Tune an existing harness without overriding managed permissions:

```toml
[harnesses.codex.native_settings]
model_context_window = 500000
model_auto_compact_token_limit = 400000
```

Managed controls such as provider routing, credentials, sandbox and MCP selection
cannot be overridden through native settings. Workspace roots and network grants
remain explicit; read-only roles do not inherit write access. A write role under
one authorized root receives the configured root set. `workspace_network = true`
requires workspace roots and retains the harness's normal approval policy.

The broker checks configuration every 60 seconds and at request boundaries.
Invalid revisions retain the last valid configuration. Existing runs keep their
frozen settings; new runs receive the validated revision. Script edits are read
on each collection and do not require a binary rebuild. Use `agent-run doctor`
to inspect configuration and freshness, and `agent-run capacity collect --once`
to execute one collection round.
