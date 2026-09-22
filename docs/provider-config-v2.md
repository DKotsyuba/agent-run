# Provider configuration v2

Schema v2 declares two native harnesses and any number of named providers.
New capacity source kinds are `codex_appserver`, `lua`, and `none`; older
v1 source names are migration input only. Lua collection configuration is
owned by the quota integration.
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
limits_source = "codex_appserver"
recommendations = ["Use for coding work."]

[[providers.codex.models]]
id = "gpt"
native_model = "gpt"
allowed_params = { effort = ["medium", "high"] }
restrictions = ["web_tools_disabled"]

[[providers.codex.bindings]]
label = "personal"
account = "acct-personal"

[providers.codex-plus]
harness = "codex"
connection = { kind = "native" }
auth_family = "openai"
limits_source = "codex_appserver"
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
limits_source = "lua"

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
the caller retains its last valid cached value. The current v1 `Config`
continues serving existing launch consumers during staged integration. V1 is
historical/migration input, not a promise of permanent public v1 launches.
