# migrations

state.db's schema version is tracked in SQLite's own `PRAGMA user_version`.
Each schema change beyond the initial schema is a numbered SQL delta file
under `sql/migrations/` (`NNN_slug.sql`), applied in order, each in its own
`BEGIN IMMEDIATE` transaction. The current schema is version 18.

## Older stores refuse ordinary commands

While a home's database is older than the running binary's schema, every
command except `config` and `doc` refuses with `migration_required` instead
of opening it, and the broker refuses to start: schema upgrades belong to
the paired config migration below. `agent-run doctor` reports the condition
as `state_migration_pending`. A newer store than the binary supports is
refused clearly rather than opened partially; see `releases`.

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

This release pairs the schema-2 config with state database schema 18. While
the home still has an older database, every command except `config` and
`doc` refuses with `migration_required` instead of opening it (opening would
upgrade it unpaired); the broker refuses to start the same way.

Write one mapping file. It names the two harnesses, declares every global
account by nonsecret reference (no credential value is read), and maps each v1
runtime to its provider id, harness, connection, auth family, v2 limits source
(and executable collector for `exec`), an explicit native model for every historical
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
limits_source = "exec"
collector = { command = "/bin/bash", args = ["/opt/agent-run/collectors/codex.sh"], source = "codex-appserver" }
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

Schema 2 requires canonical role files with explicit `revision`, `write`,
`network`, `allow_external_read_roots`, `skills`, `mcp`, and
`required_constraints` fields. Migration does not rewrite legacy role files.
Preserve the old profile directory for rollback, prepare a separate canonical
catalog, and point `profiles.directory` at it before admitting agents. Verify
that `models` lists the expected roles for each provider. Claude Code loads
plugins as a whole: every plugin-exported skill must appear in the selected
role's skill list, or the launch is refused before the engine starts.

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
to the original pair; there is no roll-forward. The service-manager lock also
excludes brokers using a custom socket.

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
makes the snapshot read-only. The numbered migrations (to the current schema) and the
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
