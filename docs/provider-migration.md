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

This is the independent planning boundary. Public `--apply`, legacy launch
cutover, backup publication, and adapter/session dispatch wait for the
remaining real consumers. Schema v17 and historical sealed blobs remain
untouched by this API.
