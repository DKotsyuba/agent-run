# Managed artifact snapshots

Adapter-managed skill trees are copied into a generated runtime home before a
child starts. A snapshot accepts only real directories and regular files; source
symlinks, devices, sockets, and pipes are rejected. Its canonical manifest lists
every portable relative path and type, plus each file's byte count and SHA-256.
The manifest is published after all content with the same durable file writer.

`inspect_managed_snapshot()` is read-only recovery evidence. It reports:

- reserved agent-run temporary files;
- unreferenced orphan paths;
- paths referenced by metadata but missing on disk;
- entries whose type, size, or hash differs from metadata.

Verification succeeds only when all four sets are empty. Inspection never
removes files. Publication also removes nothing: the destination must be empty,
or an already verified snapshot with the same path/type topology.

Each fresh start uses its own generated lineage home. Continuations and later
account-switch attempts reuse it only after its managed snapshot and recorded
revision verify; they do not rematerialize from live skill sources. Native
session logs and caches remain mutable runtime state outside the asset snapshot.
Provider continuation separately verifies a recorded native-history seal.

The current provider supervisor records sealed assets as
`snapshot:v2:<runtime-index-sha256>`. The durable `ProviderLaunchIdentity`
contains the original request, exact admitted configuration-byte digest,
validated provider configuration, normalized configuration snapshot, frozen
authority and generated-home index digest. The generated `provider-launch.json`
binds the provider, harness, connection, model, working directory and normalized
configuration digest into that asset index. Continuation verifies this frozen
authority and the indexed assets; missing or modified proof is refused, not
reclassified as a historical launch.

The index records each managed root's manifest SHA-256, adapter-owned flat
configuration files, declared credential-link paths and targets, and the
materialization revision. Deleting a whole indexed skill directory cannot hide
its missing manifest. The configured executable path is frozen, but the index
does not pin the external executable's bytes or version.

The current Codex provider auth link is deliberately outside the sealed asset
index. After immutable assets verify and previous process cleanup is proven,
an eligible attempt can rebind `auth.json` to its selected account without
changing the frozen role or tool assets. This exception does not permit
rewriting indexed configuration or skill files.

Executable contract scenarios live in
`crates/agent-run-adapters/tests/snapshots.rs`:

1. Copy a skill containing a manifest, script, and empty directory; changing
   only script bytes changes the snapshot revision.
2. Reject a source symlink and copy only explicitly selected assets.
3. Detect a missing indexed root, a changed root manifest, a symlinked manifest
   or intermediate directory, and an incomplete index shape.
4. Detect tampered content, a missing referenced file, an extra orphan and a
   manually constructed partial tree without its manifest.

Separate `crates/agent-run-platform/tests/publish.rs` tests inject faults during
file publication: mid-write and before rename preserve the old published name
and leave a temporary file; after rename the full new content is visible but
the call reports failure. A later failed group entry does not roll back earlier
entries. These are per-file publication checks, not an atomic multi-file
snapshot transaction or proof of power-loss persistence on every filesystem.
Directory fsync is used when the filesystem supports it.

## Historical configuration snapshot helpers

`build_config_snapshot()` and `inspect_config_snapshot()` preserve the tested
Python-v1 configuration-document format; the current provider launch path does
not call them. The builder binds the materialized file revision and runtime
index digest to the runtime name, adapter API version, config schema version,
sanitized runtime declaration and effective profile grants. Configured
environment values are represented by hashes. An already-known native version
may be supplied as provenance; the helper does not probe the executable.

`inspect_config_snapshot()` reads that historical configuration metadata as a
no-follow regular file, checks its recorded hash, and requires canonical
version-one JSON. Historical `snapshot:v1:` revisions and runtime request
spellings remain compatibility data; schema-2 continuation does not remap a
schema-1 run into a provider run.

Persisted revisions, snapshot manifests, and replay fingerprints hash one shared serialization
(`agent-run-domain::canonical`): sorted keys, `,`/`:` separators, and
CPython-compatible string escaping and float rendering, with `ensure_ascii`
chosen explicitly per document. Any change to those bytes makes existing runs
unresumable or falsely rejects a replay, so JSON used only as a wire frame or a
Rust-local manifest may use its own serializer but must not replace this one at a
persisted boundary.

## Native assets

For Codex, preparation writes the native per-workdir `trust_level = "trusted"`
receipt before this index is finalized. The receipt is one exact project table
for the resolved launch directory; a continuation never adds or reseals it.
Changes to the generated config, including hook trust receipts, still fail
snapshot verification.

The Claude Code harness may snapshot explicitly declared non-secret plugin
assets, as may historical Claude and GLM adapters. The optional
`plugin_snapshot_assets` mapping is keyed by configured plugin basename;
each value lists exact relative files or directories. Declared directories are
recursive. No globbing, import tracing, discovery, or secret-name heuristic is
performed: the trusted declaration owns complete transitive coverage. A plugin
without a declaration retains its legacy live path and therefore does not claim
immutable plugin configuration.
