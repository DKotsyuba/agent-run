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

## One-time paired migration to schema 2

This release pairs the schema-2 config with state database schema 17. While
the home still has an older database, every command except `config` and
`doc` refuses with `migration_required` instead of opening it (opening would
upgrade it unpaired); the broker refuses to start the same way.

Write one mapping file. It names the two harnesses, declares every global
account by nonsecret reference (no credential value is read), and maps each v1
runtime to its provider id, harness, connection, auth family, v2 limits source
(and collector for `lua`), an explicit native model for every historical
model, the account for requests that omitted an account (`global_account`), an
account for every v1 label, and optional recommendation prose:

```toml
[harnesses.codex]
binary = "/absolute/path/to/codex"
home = "/absolute/path/to/agent-run/runtimes/codex/home"
[harnesses.claude-code]
binary = "/absolute/path/to/claude"
home = "/absolute/path/to/agent-run/runtimes/claude/home"
[accounts.acct-codex-native]
auth_family = "openai"
reference = "native:codex"
[accounts.acct-codex-personal2]
auth_family = "openai"
reference = "named:codex:personal2"
[runtimes.codex]
provider = "codex"
harness = "codex"
connection = { kind = "native" }
auth_family = "openai"
limits_source = "codex_appserver"
global_account = "acct-codex-native"
labelled_accounts = { personal2 = "acct-codex-personal2" }
[runtimes.codex.native_models]
"gpt-6-sol" = "gpt-6-sol"
[runtimes.codex.model_recommendations]
"gpt-6-sol" = ["Use for connected implementation, cross-system diagnosis, security or concurrency reasoning, and substantive reviews with interacting constraints."]
```

```sh
agent-run config migrate --mapping mapping.toml --dry-run
agent-run config migrate --mapping mapping.toml --apply \
  --from-binary /path/to/installed/agent-run [--ack <marker>]...
agent-run config rollback --snapshot <home>/migrations/<id>-v1-to-v2
```

The dry run renders and validates the schema-2 config and reports the
database's current schema; it reads the database only through its file
header (or a read-only connection when live WAL frames exist) and writes
nothing. Invalid input and every refusal write nothing either.

`--apply` holds the broker startup lock for its whole run (a running broker
makes it refuse; no broker can start meanwhile), refuses while any agent is
active, and re-checks that `config.toml` still has the exact bytes it planned
from. It then creates a new snapshot directory exclusively — the exact v1
config, the mapping, an online SQLite backup read through a read-only
connection, and `manifest.json` recording the installed binary
(`--from-binary`: path, SHA-256, `--version` output) and this binary, the
config, mapping and backup digests, the source schema and a logical digest of
every row — writes `COMPLETE` last and makes the snapshot read-only. Only then
does it run the numbered store migration (the database *is* upgraded, to
schema 17), register the declared accounts and atomically publish the v2
config, followed by `<snapshot>.applied.json`. If any of those steps fails,
the snapshot database and the original config are restored.

Rollback restores the whole pair — the v1 config and the snapshot database
(schema 16 with its exact rows) — and only while the live config and every
database row still equal the applied record, nothing is live, and the
recorded pre-migration binary still exists with its recorded digest; it then
names that binary for the operator to run again. Any post-migration write (an
agent, an event, an account change, a quota sample) makes rollback refuse: it
cannot preserve that work. The snapshot is never modified.
