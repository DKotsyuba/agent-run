# config

Schema 2 separates native harnesses, named providers and global accounts.
Configuration contains nonsecret references, never token values. Register each
account before using its id in a provider binding.

Agent executions have no wall-clock limit. Start and resume accept no lifetime
parameter; continuations may be resumed repeatedly. Cancellation, native completion
and failure retain their usual lifecycle and cleanup proof requirements. The four
retired core lifetime/warning keys are rejected in live configuration. Historical
requests and snapshots remain readable after the schema-28 upgrade. Collector,
service and transport operation timeouts remain separate.

```toml
schema_version = 2
[harnesses.codex]
fast_mode = false # set true to force Fast across this harness
binary = "/absolute/path/to/codex"
home = "/absolute/path/to/agent-run/codex"
[harnesses.claude-code]
binary = "/absolute/path/to/claude"
home = "/absolute/path/to/agent-run/claude"

[providers.codex]
harness = "codex"
connection = { kind = "native" }
auth_family = "openai"
limits_source = "none"
[[providers.codex.models]]
id = "gpt-6.1-sol"
[[providers.codex.bindings]]
label = "main"
account = "acct-codex-native"
```

For a Claude custom Messages provider with `limits_source = "none"`,
`ordered_accounts = true` orders bindings by descending
`priority_multiplier` (declaration order breaks ties). A binding may select a
separate native Claude login with `connection = { kind = "native" }` and a
registered `named:claude-code:<label>` account. Billing failure on a custom
binding or an authoritative quota rejection can continue the verified session
on the next binding; pinned runs never switch. Billing is not proof of a zero
balance, and without usable Quota data each new task tries the first eligible
binding again. An unproven input, unfinished tool or unconfirmed cleanup stops the
handoff. See `docs/provider-config-v2.md` for the complete example.

Agent Quota owns collection and quota calculations. Configure its service
separately, then set `capacity.quota.socket` to its absolute Unix socket path.
An optional `capacity.quota.expected_service_uid` specifies the trusted service
UID; when absent, Run expects its own effective UID. A missing or unavailable
service leaves quota unknown and does not block a launch. Run does not send
native quota failures back to Quota.

Older collector settings remain as migration inputs, but Run does not execute
them. Previously installed collection jobs require a separate migration to
Agent Quota; they do not supply the new Run selection path.

Canonical Markdown profiles select skills, MCP servers, permissions and required
constraints. Entries in `[skills].directory` may be directory symlinks to shared
skills elsewhere. Absolute and relative links are resolved into immutable run
snapshots, so later source or link changes do not affect resume. Broken links,
cycles, non-directory targets and links inside skill contents are rejected.

Shared `[mcp.<name>]` declarations contain a transport, command,
arguments and optional environment names. In schema 2, `global = true` selects
a server for all new runs. `allowed_tools = ["read"]` limits its tools; omit
the field for all tools or use `[]` for none. Profiles accept both server names
and `{ name = "server", allowed_tools = ["read"] }` entries; their lists only
narrow the catalog cap. See [MCP controls](../../docs/provider-config-v2.md#mcp-selection-and-tool-caps)
for native harness limits. Harnesses
own native settings, hooks, plugins and workspace roots. Provider models carry
explicit native aliases, effort settings, recommendations and restrictions.

`harnesses.codex.fast_mode = true` forces Fast for every Codex provider/model
and pool member, without a per-call flag. Default false keeps explicit `fast`.
Resume reads the current flag; retries keep the admitted effective mode. Running
processes do not change. Other harnesses reject true; reasoning effort is separate.
Fast uses subscription limits/credits at higher rates.

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
frozen settings; new runs receive the validated revision. Agent Quota owns quota
collection; see its operator documentation to configure collection. Use
`agent-run limits` to inspect available quota and its completeness marker.
The old `agent-run capacity collect` and `capacity launchd` commands are no
longer available. Existing scheduled jobs are not changed automatically;
moving a macOS launchd job requires a separate migration.
