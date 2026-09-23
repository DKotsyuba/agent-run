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
adapter = "codex"
binary = "/absolute/path/to/codex"
home = "/absolute/path/to/agent-run/codex"
workspace_roots = ["/absolute/path/to/projects", "/absolute/path/to/worktrees"]
workspace_network = true
models = ["gpt-6-astra"]
accounts = ["personal2"]
limits_source = "codex_appserver"

[runtimes.codex.native_settings]
model_context_window = 500000
model_auto_compact_token_limit = 400000
```

Revisioned Markdown profiles own write/network grants, external read-root
policy, skills, MCP selection, and required constraints. `agent-run init`
creates the valid profile identifiers `role-review`, `role-architect`, and
`role-code`. Model policy is separate from general profile validity:
`gpt-6-astra` accepts only the exact read-only profile IDs `review` and
`architect`. A generally valid ID such as `role-review` is rejected for that
model. Compatibility profiles and runtime `skills`/`mcp` lists remain readable,
but canonical and compatibility asset declarations cannot be mixed.
Qwen is no longer a runtime: a Qwen adapter identifier is rejected with a
deprecation error, and the runtime must be removed from `config.toml`.

`workspace_roots` affects write-capable Codex profiles only. Their workdir must
be inside at least one configured tree; read-only profiles do not inherit write
access. A write role admitted under one root receives write access to every
configured root. The legacy singular `workspace_root = "..."` declaration is still
accepted and normalizes to one root; declaring both forms is rejected.
`workspace_network = true` explicitly enables shell network in that Projects
profile and keeps `curl` behind Codex's normal approval reviewer; it defaults
to false and requires at least one configured workspace root.
MCP `approval_mode = "approve"` is appropriate only for a locally trusted server
whose own runtime enforces downstream permissions.

Omitting an account uses native global auth. An explicit label selects separate
credential state. Compatibility-only `default_account`, environment, and Rust
declarations remain readable but do not provision tools or redirect unlabelled
starts; remove them from new configurations.

`runtimes.<name>.native_settings` retunes a runtime's own generated preference
file (Codex `config.toml` or Claude/GLM `settings.json`)
from this common config, without rebuilding the broker. The
table attaches to a runtime you have already declared with its mandatory
fields; for example, under an existing claude block:

```toml
[runtimes.claude.native_settings]
spinnerTipsEnabled = false
```

Values may be strings, booleans, integers, finite floats, arrays, or nested
tables; keys are plain identifiers without dots. Codex defaults to a
1,000,000-token context window and 780,000-token total compaction limit; a
declared key overrides its default, and omitted keys keep it. Reserved control
roots are rejected with a validation error instead of being applied: model and
reasoning-effort selection, provider and auth routing, credentials and
environment (including `shell_environment_policy`, `notify`, `apiKeyHelper`,
and the AWS/GCP credential helpers), sandbox, permissions, approvals, hooks and
hook-disablement, and MCP and plugin enablement. Those
stay owned by agent-run or the runtime's security model. An
unknown-but-unreserved key (Codex `model_verbosity`, for instance) is accepted
as a plain tuning value; that is a convenience, not a safety claim about every
upstream key. Never place secret values in `native_settings`: auth and
credential sources are never resolved through it, but the table is persisted
verbatim in the config snapshot. Edit the TOML, parse it, and run
`agent-run doctor`: the broker compares the file's SHA-256 every 60 seconds and
loads a changed valid revision without restarting. New starts and continuations
also check immediately; malformed changes are rejected instead of replacing the
last valid cached revision. Existing sessions keep the immutable config they
were prepared with. The config snapshot records the declared settings, so a
change alters snapshot identity and forces regeneration on the next launch.

In a `schema_version = 2` home, `agent-run capacity collect` polls
account-scoped quota sources only: each registered account is observed once
per round however many provider aliases bind it. `limits_source = "lua"`
providers bind a collector (`glm_quota`, `anthropic_usage`, or a custom
`script_file` with an explicit `auth` placement); `codex_appserver` providers
are probed through the account's own reference (`native:codex` or
`named:codex:<label>`), never a provider label. Native and named Claude
accounts read only their own login store, with no fallback to the default
login. Failures report fixed codes such as `credential_unavailable`, the
round's `ok` is false when any source failed, and a v2 config that fails to
load is an error rather than a switch to legacy sources. See
`docs/quota-collectors.md` for the per-source window mapping.

CodexBar is retired. Schema 2 rejects `capacity.codexbar_binary` and a
`codexbar` limits source. A schema-1 file that still declares them keeps
parsing so the one-time migration can read it, but a `codexbar` runtime is
never invoked: each round reports it failed with
`codexbar_retired_migration_required` and keeps its earlier samples.

A labelled Claude login (`agent-run auth`/`login --account <label>`) is stored
in `accounts/claude/<label>/claude-config` under the agent-run home, the same
directory runs and quota collection read. A login made by an earlier release
at `<runtime home>@<label>/claude-config` keeps working in place; if both
directories exist the label is ambiguous and fails until one is removed.

After manual edits, parse the TOML and run `agent-run doctor`. Doctor checks
binaries and role assets; runtime start does not run language-toolchain
readiness probes.
