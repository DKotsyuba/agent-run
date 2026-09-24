# Configuration and database migration

## Existing schema-2 homes

When upgrading from 0.13.x to schema 19, prepare a complete replacement
configuration using external quota commands as described in
[quota collectors](quota-collectors.md). Run the **new candidate binary** while
the old broker and other agent-run writers are stopped:

```bash
candidate=/absolute/path/to/new-sealed-release/bin/agent-run
runtime_home="$HOME/.agent-run"
old_release="$HOME/.agent-run/standalone/current"

"$candidate" --home "$runtime_home" config migrate \
  --target-config /absolute/path/to/config-v2.toml --dry-run
"$candidate" --home "$runtime_home" config migrate \
  --target-config /absolute/path/to/config-v2.toml --apply --from-release "$old_release"
```

`--target-config` and `--mapping` are mutually exclusive. The v2 path validates
the target against the existing registry, preserves all accounts and history,
and never picks vendor scripts or changes credentials automatically. Retired
Lua settings may appear in the original configuration; the replacement must
use the current configuration contract. The target is checked again against
the staged database before publication.

The printed snapshot supports `config rollback --snapshot DIR` through the
same candidate, while no later writes have occurred. It restores the original
configuration bytes and original database together. Historical snapshot names
such as `config.v1.toml` and `v1_config_sha256` mean *before migration* even when
the original config was already schema 2. Existing snapshots remain readable.
File hashes stream in fixed-size chunks; row digests retain the historical
format while SQLite performs a disk-backed sort, avoiding a whole-database
allocation in Rust. After migration, install the new sealed version and perform
the explicit service restart and readiness checks in [releasing](releasing.md).

## Legacy schema-1 homes

`provider_migration::plan_v1(old_config, harnesses, mappings,
account_records, home)` is a deterministic dry run. It performs no write,
backup, credential read, CLI launch, or database migration. Its output is a
validated proposed `ProviderConfig`, a map from raw v1 runtime names to
recorded provider/harness evidence for `decode_legacy_request`, and stable
nonsecret `manual_review` markers. The implemented `config migrate --apply`
path backs up the paired config/state before publication and requires every
review marker to be acknowledged.

Each v1 runtime requires an explicit `RuntimeMapping`: new provider id,
Codex or Claude Code harness, native/custom connection, auth family, v2
limits source, every historical model id with an explicit `native_model`,
model hard restrictions, a global `AccountId` for omitted account requests,
and global ids for every labelled account. The planner never maps `main`
from spelling alone. GLM's old adapter name can map only to an explicitly
configured Claude Messages gateway. The old stored request name stays raw;
the returned `legacy_runtime_map` supplies evidence only when a historical
resume needs it.

V1 account override weights are absolute. The plan preserves their effective
value by dividing each override by the provider multiplier before setting
its binding multiplier. V1 lane overrides, missing model aliases, missing or
extra account labels, disabled runtimes, duplicate provider ids, and
unregistered/incompatible account references refuse rather than silently
change selection. Old auth declarations or differing stable harness settings
produce manual-review markers without copying secret values.

The public `agent-run config migrate --mapping <file> --dry-run` uses this
planner and reports the current database schema without changing the home.
`--apply` requires the verified installed release, explicit acknowledgement
of every `manual_review` marker, a stopped broker, and no active agents. It
seals the original config and database before migrating the staged copy to
the current database schema, then publishes the v2 pair under a journal. Ordinary commands
refuse an older or interrupted home. `agent-run config rollback --snapshot
<dir>` restores the verified v1 pair only if no later writes have changed it.
The exact commands, refusal rules, and recovery sequence are in the embedded
[`migrations` operator guide](../assets/operator_guide/migrations.md).

The migration does not rewrite legacy profile files. Before admitting agents
under schema 2, select a separate catalog of complete canonical roles and
confirm `models` lists eligible roles for each intended provider. Claude Code
loads declared plugins as a whole, so each role must list every skill those
plugins export; an undeclared export is refused before launch.
