# config

`config.toml` declares runtime binaries/models, one profile directory, one skill
catalog, and shared MCP definitions. It contains references, never credential
values.

```toml
schema_version = 1

[profiles]
directory = "/absolute/path/to/profiles"

[skills]
directory = "/absolute/path/to/skills"

[mcp.codegraph]
transport = "stdio"
command = "/absolute/path/to/codegraph"
args = ["mcp"]
approval_mode = "auto"

[runtimes.codex]
enabled = true
adapter = "agent_run.adapters.codex.adapter:ADAPTER"
binary = "/absolute/path/to/codex"
home = "/absolute/path/to/agent-run/codex"
workspace_root = "/Users/you/projects"
workspace_network = true
models = ["gpt-6-astra"]
accounts = ["personal2"]
limits_source = "codex_appserver"

[runtimes.codex.native_settings]
model_context_window = 500000
model_auto_compact_token_limit = 400000

[runtimes.claude.native_settings]
spinnerTipsEnabled = false
```

Revisioned Markdown profiles own write/network grants, external read-root policy,
skills, MCP selection, and required constraints. ``gpt-6-astra`` accepts only the
public read-only profiles ``review`` and ``architect``; provider-facing agent-type
names such as ``role-review`` are not profile identifiers. Legacy profiles and
runtime
`skills`/`mcp` lists remain readable until migration, but canonical and legacy
asset declarations cannot be mixed.

`workspace_root` affects write-capable Codex profiles only. Their workdir must
be inside the configured tree; read-only profiles do not inherit write access.
`workspace_network = true` explicitly enables shell network in that Projects
profile and keeps `curl` behind Codex's normal approval reviewer; it defaults
to false and requires `workspace_root`.
MCP `approval_mode = "approve"` is appropriate only for a locally trusted server
whose own runtime enforces downstream permissions.

Omitting an account uses native global auth. An explicit label selects separate
credential state. Legacy `default_account`, environment, and Rust declarations
are readable but do not provision tools or redirect unlabelled starts.

`runtimes.<name>.native_settings` retunes a runtime's own generated preference
file (Codex `config.toml`, Claude/GLM `settings.json`, Qwen `.qwen/settings.json`)
from this common config, with no Python edits and no package reinstall. Values
may be strings, booleans, integers, finite floats, arrays, or nested tables;
keys are plain identifiers without dots. Codex defaults to a 1,000,000-token
context window and 780,000-token total compaction limit; a declared key
overrides its default, and omitted keys keep it. The same table works for
claude, glm, and qwen runtimes. Reserved control roots are rejected with a
validation error instead of being applied: model/effort selection, provider and
auth routing, credentials and environment (including `shell_environment_policy`,
`notify`, `apiKeyHelper`, and the AWS/GCP credential helpers), sandbox,
permissions, approvals, hooks and hook-disablement, MCP and plugin enablement,
and Qwen's `tools.sandbox`. Those stay owned by agent-run or the runtime's
security model. An unknown-but-unreserved key is accepted as a plain tuning
value; that is a convenience, not a safety claim about every upstream key.
Edit the TOML, parse it, run `agent-run doctor`, then restart or reload the
broker: new launches pick up the settings. There is no live hot reload, and
existing sessions keep the config they were prepared with. The config snapshot
records the declared settings, so a change alters snapshot identity and forces
regeneration on the next launch.

Before restarting a service after manual edits, parse the TOML and run
`agent-run doctor`. Doctor checks binaries and role assets; runtime start does
not run language-toolchain readiness probes.
