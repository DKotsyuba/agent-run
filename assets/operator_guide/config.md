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
  --from-release <prefix>/releases/<installed version> [--ack <marker>]...
agent-run config rollback --snapshot <home>/migrations/<id>-v1-to-v2
```

The dry run renders and validates the schema-2 config and reports the
database's current schema; it reads the database only through its file
header (or a read-only connection when live WAL frames exist) and writes
nothing. Invalid input and every refusal write nothing either.

Rollout preconditions. Stop the resident broker and unload the capacity and
delivery launchd jobs first (`launchctl bootout gui/$(id -u)/<label>`), and
let active agents finish. The migration enforces what it can: it holds the
broker startup lock for its whole run (a running broker makes it refuse; no
broker can start meanwhile), refuses while any agent is active, and every
newly started command — including a capacity or delivery job — refuses with
`migration_required` while the database is older, or `migration_incomplete`
while a journal exists. For a process that already has the store open, apply
and rollback take an exclusive SQLite lease on the live database before the
first live read and keep it — through the snapshot, the checks, the
replacement, the config and journal steps — until the journal is cleared:
if any other process holds the database open, they refuse ("in use") before
writing anything, and a writer arriving while the lease is held is refused
with `SQLITE_BUSY` until it is released, so no committed write is replaced.
Nothing is killed; stop such processes and rerun. Recovery only rolls back
to the v1 pair; there is no roll-forward.

`--from-release` names the installed release directory — a sealed release
as built by `xtask release` — not a bare executable. Its `COMPLETE`,
`SHA256SUMS` and `metadata.json` must verify, and the schema its metadata
records must equal the database's schema and be older than this binary's;
nothing is executed to learn its version. An unsealed, tampered or
incompatible release is refused before anything is written.

`--apply` then re-checks that `config.toml` still has the exact bytes it
planned from and creates a new snapshot directory exclusively — the exact v1
config, the mapping, an online SQLite backup, and `manifest.json` recording
the old release (path, version, schema, binary and `SHA256SUMS` digests), this
binary (digest and schema), the config, mapping and backup digests, the
source schema and a logical digest of every row — writes `COMPLETE` last and
makes the snapshot read-only. The numbered migration (to schema 17) and the
declared accounts are applied to a staged copy of the snapshot database. Only
then does it write `migrations/in-progress.json`, the journal recording the
source and target row digests and both config digests, and publish: the live
database is replaced from the staged target in one SQLite transaction, the v2
config is written (only over the exact v1 bytes), then
`<snapshot>.applied.json`, and the journal is removed.

If publication fails, only what this operation provably published is undone:
the database is restored while every row still equals the staged target, the
config is returned to v1 only while it equals the v2 bytes it wrote. A config
edited by someone else meanwhile is kept as is. If the process is killed, the
journal stays, every ordinary command refuses with `migration_incomplete`,
and `config rollback --snapshot <dir>` recovers from it.

Rollback (and recovery) restores the whole pair — the v1 config and the
snapshot database (schema 16 with its exact rows). It needs this snapshot's
applied record or its own journal, a live config equal to the snapshot's v1
or v2 bytes, every database row equal to the recorded source or target
digest, no active agent, and the recorded release still sealed with its
recorded digests. A matching config alone never authorizes replacing the
database: any post-migration write (an agent, an event, an account change, a
quota sample) makes rollback refuse and leaves config, database and journal
untouched. Rollback is itself journalled, so an interrupted rollback is
finished by rerunning it. It returns the release to reinstall; it does not
switch the installed pointer. The snapshot is never modified.

## New homes and native login

`agent-run init` on a home without `config.toml` writes the explicitly empty
schema-2 catalog `schema_version = 2` (no harness, provider or account;
`models` lists nothing and nothing can start until they are declared). A
schema-1 `config.toml` never initializes a new state database. `doctor`
reports such a home as `provider_catalog_empty` (information), not as an
invalid configuration; a configured schema-2 home is checked through its
harness executables.

In a schema-2 home, `agent-run auth <account> <provider>` and
`agent-run login <provider> [--account <account>]` take a native-connection
provider id and one of its bindings, by provider-local label or global
account id (optional when the provider binds exactly one). The login runs the
provider's harness executable against the bound account's own storage:
`native:<harness>` is the harness's global login, `named:codex:<label>` is
`accounts/codex/<label>`, `named:claude-code:<label>` the labelled Claude
directory. `login` accepts Claude Code providers only.
