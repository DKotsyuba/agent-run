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

[runtimes.codex]
enabled = true
adapter = "agent_run.adapters.codex.adapter:ADAPTER"
binary = "/absolute/path/to/codex"
home = "/absolute/path/to/agent-run/codex"
models = ["gpt-6-astra"]
accounts = ["personal2"]
limits_source = "codex_appserver"
```

Revisioned Markdown profiles own write/network grants, external read-root policy,
skills, MCP selection, and required constraints. ``gpt-6-astra`` accepts only the
public read-only profiles ``review`` and ``architect``; provider-facing agent-type
names such as ``role-review`` are not profile identifiers. Legacy profiles and
runtime
`skills`/`mcp` lists remain readable until migration, but canonical and legacy
asset declarations cannot be mixed.

Omitting an account uses native global auth. An explicit label selects separate
credential state. Legacy `default_account`, environment, and Rust declarations
are readable but do not provision tools or redirect unlabelled starts.

Before restarting a service after manual edits, parse the TOML and run
`agent-run doctor`. Doctor checks binaries and role assets; runtime start does
not run language-toolchain readiness probes.
