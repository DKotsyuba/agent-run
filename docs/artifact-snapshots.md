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

Each fresh attempt uses its own generated lineage home. A continuation reuses
that lineage home only after its managed snapshot and recorded revision verify;
it does not rematerialize from live skill sources. Native session logs and caches
remain mutable runtime state outside the snapshot. Historical attempts without a
lineage home retain their compatibility path.

Executable contract scenarios live in `tests/test_snapshots.py`:

1. Copy a skill containing a manifest, script, and empty directory; changing
   only script bytes changes the snapshot revision.
2. Replace a source entry with a symlink or special file; publication fails
   before producing verified metadata.
3. Fail final metadata publication; copied files are classified as orphans and
   the snapshot remains unverified.
4. Add an owned temporary and unowned orphan, then remove a referenced file;
   recovery reports all three without deleting any path.

These fault injections verify ordering and fail-closed behavior in the process.
They do not prove persistence through power loss on every filesystem. Directory
fsync is used when the filesystem supports it.

`build_config_snapshot()` produces canonical attempt metadata binding the
materialized file revision to the runtime name, adapter API version, config
schema version, complete runtime declaration, and effective profile body and
grants. Configured environment values are represented by hashes, so content-only
edits change the revision without copying credential-like values into metadata.
An already-known native runtime version is recorded when available; snapshot
creation does not run an additional version probe.

Each published tree is also recorded in the generated home's snapshot index.
Resume checks that index, so deleting an entire skill directory cannot hide its
missing manifest. The finalized index also binds adapter-known flat config files
and the materialization revision; resume requires both its stored SHA-256 and
revision to match. `inspect_config_snapshot()` likewise reads the attempt's
configuration metadata as a no-follow regular file, checks its recorded hash,
and requires canonical version-one JSON before reuse.

Claude and GLM may snapshot explicitly declared non-secret plugin assets. The
optional `plugin_snapshot_assets` mapping is keyed by configured plugin basename;
each value lists exact relative files or directories. Declared directories are
recursive. No globbing, import tracing, discovery, or secret-name heuristic is
performed: the trusted declaration owns complete transitive coverage. A plugin
without a declaration retains its legacy live path and therefore does not claim
immutable plugin configuration.
