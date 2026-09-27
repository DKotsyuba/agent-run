# Provider configuration v2

Schema v2 declares two native harnesses and any number of named providers.
Capacity sources are `exec` and `none`. Every collector is an external
executable explicitly selected by its provider; no collector code is embedded.
Provider ids are operator-chosen; names such as `codex-plus` and `glm` carry
no built-in subscription or model-intelligence meaning. The orchestrator
selects a configured model id. Profiles, skills, and MCP definitions remain
in their existing canonical role catalogs. Harness settings own native
binary/home, hooks, plugin assets, workspace policy, and unowned native
settings. Account weights and prose recommendations live only in TOML.

```toml
schema_version = 2

[harnesses.codex]
binary = "/usr/local/bin/codex"
home = "/var/lib/agent-run/codex"

[harnesses.claude-code]
binary = "/usr/local/bin/claude"
home = "/var/lib/agent-run/claude"

[providers.codex]
harness = "codex"
connection = { kind = "native" }
auth_family = "openai"
limits_source = "exec"
collector = { command = "/bin/bash", args = ["/opt/agent-run/collectors/codex.sh"], source = "codex-appserver" }
recommendations = ["Use for coding work."]

[[providers.codex.models]]
id = "gpt"
native_model = "gpt"
allowed_params = { effort = ["medium", "high"] }
params = { effort = "medium" } # applied when start omits effort
restrictions = ["web_tools_disabled"]

[[providers.codex.bindings]]
label = "personal"
account = "acct-personal"

[providers.codex-plus]
harness = "codex"
connection = { kind = "native" }
auth_family = "openai"
limits_source = "exec"
collector = { command = "/bin/bash", args = ["/opt/agent-run/collectors/codex.sh"], source = "codex-appserver" }
priority_multiplier = 5.0

[[providers.codex-plus.models]]
id = "gpt"

[[providers.codex-plus.bindings]]
label = "plus"
account = "acct-personal"
priority_multiplier = 2.0
models = ["gpt"]

[providers.glm]
harness = "claude-code"
connection = { kind = "custom", endpoint = "https://gateway.example/api/anthropic", protocol = "messages" }
auth_family = "anthropic"
limits_source = "exec"
collector = { command = "/bin/bash", args = ["/opt/agent-run/collectors/glm.sh"], source = "glm-quota" }

[[providers.glm.models]]
id = "glm-5.3"
native_model = "glm-5.3[1m]"

[[providers.glm.bindings]]
label = "work"
account = "acct-glm"
```

`native` preserves the harness's existing login and endpoint. It has no
custom URL or implicit API-key conversion. `custom` requires an explicit
HTTPS endpoint and protocol: Codex accepts `responses`, Claude Code accepts
`messages`. Custom connections use bearer authorization by default and may
set `auth_header = "x_api_key"` for compatible gateways. Loopback HTTP
requires `allow_loopback_http = true` inside the
custom connection. Credentials are selected from registered account
references, never embedded in this file; see the [account registry](account-registry.md).
Codex's Responses provider settings
and Claude's Messages gateway environment are materialized by their later
adapter consumer; this config parser does not launch either CLI. See the
[Codex custom-provider configuration](https://learn.chatgpt.com/docs/config-file/config-advanced)
and [Claude gateway configuration](https://code.claude.com/docs/en/llm-gateway-connect)
for the native wire settings.

Historical model aliases such as `fable` → `claude-fable-5-1` and
`glm-5.3` → `glm-5.3[1m]` become explicit `native_model` entries when
migrated. Any model-specific hard policy belongs in typed `restrictions`;
the loader does not infer it from a model name. Existing role and request
constraints remain authoritative.

The quota/session handoff is `ProviderConfig::load_if_changed(home, revision)`
for exact-byte SHA reload, `ProviderConfig::resolve_catalog(account_records)`
for registry-checked provider bindings, and `ProviderConfig::snapshot()`
for a secret-free digest and stable ids. A failed reload returns an error;
the caller retains its last valid cached value. Public starts use schema 2;
the v1 `Config` remains only for historical rows, migration inputs, and shared
operator controls. It is not a public runtime launch alias.
The [provider materialization contract](provider-materialization.md) names
the sealed adapter APIs that consume this configuration.
The [one-time migration plan](provider-migration.md) requires explicit
runtime, model, account, and harness mappings before any later apply step.

## MCP selection and tool caps

Declare a stdio server once in the shared catalog. `global` is optional and
defaults to false; when true it joins every new run's profile-selected MCPs.
Only selected servers are materialized. Existing running/resumed sessions keep
their frozen selection rather than importing newly configured global servers.

```toml
[mcp.tracker]
transport = "stdio"
command = "/opt/tracker/connect"
env_from = ["TRACKER_TOKEN"]
global = true
allowed_tools = ["get_context", "update_task"]
```

The profile's Markdown TOML front matter supports both forms:

```toml
mcp = ["codegraph", { name = "tracker", allowed_tools = ["get_context"] }]
```

Omitting `allowed_tools` means all tools; an empty list means none. Exact,
case-sensitive names use 1–128 ASCII letters, digits, underscores, hyphens or
dots, with no wildcard patterns. The profile cap intersects the catalog cap,
so it cannot add permissions. Duplicate server selections, duplicate tool names
and unknown fields are rejected. Selected servers retain profile order, followed
by remaining global servers in catalog order. Existing string-only profiles and
historical snapshot digests remain compatible. These controls require schema 2.

| Harness | Enforcement |
|---|---|
| Codex | Generated `mcp_servers.<name>.enabled_tools` is the native allowlist. |
| Claude Code, including GLM | Before each attempt/resume, Agent Run initializes the selected stdio MCP with the launch environment, reads every `tools/list` page, and passes the complement to native `--disallowedTools`. The harness connects directly to the original server. |

Claude discovery has a 15-second per-server and 30-second total budget, plus
bounded process teardown. A missing allowed name, incomplete/malformed catalog,
catalog change during discovery, authentication failure or timeout refuses the
restricted launch. An empty allowlist uses `mcp__server__*` without discovery.
No filter means no discovery. Temporary discovery processes are cleaned on
success, failure and cancellation; captured identities allow crash recovery.

Claude's inversion is a **launch-time catalog snapshot**, not a permanent
positive allowlist: a tool added after discovery or exposed differently to
another client/connection can escape the generated deny list. Use a stable
catalog for the duration of a run. Restricted discovery currently rejects
`${...}` interpolation in command/arguments and native names containing dots
or other unsupported characters instead of guessing Claude's normalization.
Server names containing `__` or ending in `_` are also rejected for ambiguous
native namespace boundaries. Declared `env_from` values remain supported.

The frozen role records each selection's source (`global`, `profile`, `both`)
and effective cap. Start/resume diagnostics summarize these **selected** servers;
they do not assert live availability. Resume refuses newly revoked server/tool
rights and never expands a previous session's frozen cap. Tool filtering is a
harness feature, not operating-system isolation or server-side authorization
against separate connections made with other granted tools.

## External quota execution

For current configurations, use `limits_source = "exec"` and a collector with
an absolute `command` and optional literal `args`, or `limits_source = "none"`.
The former `lua` and `codex_appserver` choices are retired. See
[the executable contract](quota-collectors.md) for stdin/stdout, credentials,
script installation, source identity and replacement behavior.
