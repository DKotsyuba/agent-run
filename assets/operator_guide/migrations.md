# migrations

state.db tracks its schema in SQLite's `PRAGMA user_version`. Numbered
`sql/migrations/NNN_slug.sql` deltas apply in order, each in a separate
`BEGIN IMMEDIATE` transaction. Current schema 27 adds a bounded, content-free incident ledger independent of
ordinary history retirement. Schema 26 added attempt-pinned pool enrollment
and worker catalog proof. Historical independence is unknown:
attaching an older root is refused, not assumed safe from missing pool history.

## Older stores refuse ordinary commands

For an older database, commands except `config` and `doc` refuse with
`migration_required`, and the broker will not start; use paired migration
below. This preflight also stops public `agent-run doctor` before its internal
`state_migration_pending` diagnostic. A database newer than the binary supports
is refused, never partially opened; see `releases`.

## Upgrading an existing schema-2 home

Use the new candidate binary with an explicit complete target configuration:

```sh
/absolute/path/to/new/agent-run --home <home> config migrate \
  --target-config /absolute/path/to/config-v2.toml --dry-run
/absolute/path/to/new/agent-run --home <home> config migrate \
  --target-config /absolute/path/to/config-v2.toml --apply \
  --from-release <prefix>/releases/<installed-version>
```

This upgrades the database and configuration as one recoverable pair. Existing
accounts, references and history are preserved; account registration is not
part of this path. The target must use current settings, including configured
external quota executables instead of retired Lua bindings. agent-run never
chooses a vendor collector automatically. The target is checked against the
read-only account registry and again against the staged database.

`--mapping` and `--target-config` are mutually exclusive. Rollback uses the
printed snapshot exactly as below. Historical snapshot filenames and v1/v2
digest key names mean before/after even for an already-v2 source. File hashes
stream in fixed-size chunks, and SQLite sorts canonical row text with disk
spill; migration does not build a whole-database string in Rust memory.

## Paired migration from schema 1 to schema 2

This migration pairs schema-2 configuration with the current database schema.

One mapping file names both harnesses and every global account by nonsecret
reference; no credential value is read. Map each v1 runtime to provider,
harness, connection, auth family, v2 limits source (with collector for `exec`),
every historical model's native id, `global_account` for unlabelled requests,
each v1 label's account, and optional recommendations:

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
limits_source = "exec"
collector = { command = "/bin/bash", args = ["/opt/agent-run/collectors/codex.sh"], source = "codex-appserver" }
global_account = "acct-codex-native"
labelled_accounts = { personal2 = "acct-codex-personal2" }
[runtimes.codex.native_models]
"gpt-6.1-sol" = "gpt-6.1-sol"
[runtimes.codex.model_recommendations]
"gpt-6.1-sol" = ["Use for connected implementation, cross-system diagnosis, security or concurrency reasoning, and substantive reviews with interacting constraints."]
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
nothing. Early validation refusals also write nothing. A later apply refusal
may retain a sealed backup while leaving the original config/database pair
unchanged.

Schema 2 requires canonical role files with explicit `revision`, `write`,
`network`, `allow_external_read_roots`, `skills`, `mcp`, and
`required_constraints` fields. Migration does not rewrite legacy role files.
Preserve the old profile directory for rollback, prepare a separate canonical
catalog, and point `profiles.directory` at it before admitting agents. Verify
that `models` lists the expected roles for each provider. Claude Code loads
plugins as a whole: every plugin-exported skill must appear in the selected
role's skill list, or the launch is refused before the engine starts.

Stop the resident broker, capacity and delivery jobs, and let active agents
finish. Apply and rollback hold both broker startup locks (including the
service-manager lock for custom sockets) and an exclusive SQLite lease through
publication and journal removal. A pre-existing database user causes an "in use"
refusal; later writers receive `SQLITE_BUSY`. No process is killed. Ordinary
commands refuse older schemas with `migration_required` and unfinished journals
with `migration_incomplete`. Recovery restores the original pair; there is no
roll-forward.

`--from-release` names the immutable installed release directory — a sealed release
as built by `xtask release` — not a bare executable. Its `COMPLETE`,
`SHA256SUMS` and `metadata.json` must verify, and the schema its metadata
records must equal the database's schema and be older than this binary's;
nothing is executed to learn its version. An unsealed, tampered or
incompatible release is refused before anything is written.

When starting from `<prefix>/current`, resolve it first with
`old_release="$(cd "<prefix>/current" && pwd -P)"` and pass `"$old_release"`.
The snapshot records this path; a movable `current` symlink would resolve to
the new release after switching and prevent rollback verification. Retain the
recorded release while rollback remains possible.

`--apply` rechecks the original config bytes and creates an exclusive snapshot:
config, operator input, online SQLite backup, and a manifest binding the old
release, target binary, schemas and content digests. `COMPLETE` is written last;
the snapshot becomes read-only. Migrations and any legacy account registrations
run on a staged copy. The journal then records both config and row digests before
the live database and config are replaced, the applied record is written, and
the journal is removed.

If publication fails, only what this operation provably published is undone:
the database is restored while every row still equals the staged target, the
config is restored only while it equals the target bytes it wrote. A config
edited by someone else meanwhile is kept as is. If the process is killed, the
journal stays, every ordinary command refuses with `migration_incomplete`,
and `config rollback --snapshot <dir>` recovers from it.

Rollback restores exact original config and rows (schema 16 for v1, schema
17 for 0.13.x v2). It requires this snapshot's applied record or its own journal,
live config matching its v1/v2 bytes, all rows matching the recorded source or
target digest, no active agent, and the recorded release's seal and digests.
Matching config alone is insufficient: any later database write (agent, event,
account or quota sample) refuses rollback without changing config, database or
journal. Rerun an interrupted rollback to finish its journal. It returns the
release to reinstall, never switches the installed pointer or alters the snapshot.
