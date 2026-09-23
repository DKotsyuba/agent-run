# One-time provider configuration migration plan

`provider_migration::plan_v1(old_config, harnesses, mappings,
account_records, home)` is a deterministic dry run. It performs no write,
backup, credential read, CLI launch, or database migration. Its output is a
validated proposed `ProviderConfig`, a map from raw v1 runtime names to
recorded provider/harness evidence for `decode_legacy_request`, and stable
nonsecret `manual_review` markers. A future coordinated publication path
must back up before applying any plan and must resolve every review marker.

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
schema 17, then publishes the v2 pair under a journal. Ordinary commands
refuse an older or interrupted home. `agent-run config rollback --snapshot
<dir>` restores the verified v1 pair only if no later writes have changed it.
The exact commands, refusal rules, and recovery sequence are in the embedded
[`migrations` operator guide](../assets/operator_guide/migrations.md).

The migration does not rewrite legacy profile files. Before admitting agents
under schema 2, select a separate catalog of complete canonical roles and
confirm `models` lists eligible roles for each intended provider. Claude Code
loads declared plugins as a whole, so each role must list every skill those
plugins export; an undeclared export is refused before launch.
