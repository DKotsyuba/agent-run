# Native directory cache lifecycle

Retained Codex homes each duplicate the same unindexed native directory
caches: the generated `skills/.system` skills tree and the
remote-downloaded plugin parents under `plugins/cache/<market>/<plugin>`.
`agent-run-core::native_tree_cache` removes those duplicates while idle
without changing what the native SDK sees or does.

## Kinds and eligibility

- `skills/.system` — the whole root. Native discovery reads through a
  whole-root link, and an SDK marker upgrade replaces the private link with
  a freshly extracted private directory while the shared tree stays
  untouched, so this root may keep its link across harness runs and is
  re-frozen on the next confirmed idle pass.
- `plugins/cache/<market>/<plugin>` with both components from the safe
  identity charset and `market != personal` — but only when the parent
  carries a valid native `.codex-remote-plugin-install.json` marker
  (`schema_version` exactly 1, nonempty `remote_plugin_id`). The native
  store writes version updates into the parent itself, so these roots are
  thawed before every native invocation that could mutate the cache —
  resume, retry, account switch, and probes alike.

Everything else is refused or skipped unchanged: the managed `personal`
marketplace (covered by managed parent views), `plugins/data`, shallower or
deeper paths, any overlap with the home's frozen runtime index
(`.agent-run-snapshots.json` roots, files, or links — a malformed index is
an explicit refusal), and any tree holding a symlink or special entry. A
missing or invalid remote marker returns `FreezeOutcome::SkippedUnchanged`
with the parent untouched; a directory is never guessed disposable because
it is named like a cache.

## Store layout and locking

There is no second content-addressed store. Freeze captures the private
tree (bounded walk: 4096 entries, 16 MiB per file, 128 MiB aggregate,
depth 8, 10 s), builds a managed-snapshot staging copy inside the
operation backup using the platform's APFS-clone snapshot writer, imports
it through the existing `shared_assets::import_shared_tree` publisher — so
trees land in the existing readonly `trees/<scope>/<manifest-sha>`
namespace with payloads deduplicated into the existing
`blobs/<scope>/<sha>-<mode>` blobs — and every later check reuses
`verify_shared_tree`, `shared_tree_root`, and `shared_tree_blob_names`.

`<scope>` is the caller-supplied trusted account/connection compatibility
domain (64 lowercase hex, `shared_assets::is_scope`), never a name or
label; identical content under different scopes never converges, so one
account's native cache is never restored for another.

Crash and reference safety:

- The `op.json` record inside `<home>/.agent-run-native-<uuid>/` is written
  before import runs, pinning the about-to-be-published tree for
  [`scan_refs`] and recovery — no untracked-reference window survives a
  crash between publication and linking.
- The home-side switch (original into the backup, exact whole-root symlink,
  proven backup removal) runs under one global `SharedStoreLock` hold, so
  store-wide guard scans and collectors never observe a half-switched home.
  The import's own internal lock hold is never nested inside ours.
- The backup is removed only after re-capturing it still hashes to the
  recorded manifest; a tampered backup is an explicit error left in place.
- `recover` completes or rolls back every interrupted freeze or thaw from
  the records alone: a freeze missing its link is relinked from its
  verified tree, a freeze that never moved its original drops the
  disposable staging, a thaw is completed from a fully proven staged clone
  or rolled back to the exact link. Missing or malformed records are
  explicit failures that preserve contents. History and index bytes are
  never touched.
- `thaw` restores each payload through the platform's APFS-clone snapshot
  writer — an independent writable inode with the manifest's logical mode,
  never an external hardlink — and the restored native tree contains
  exactly the captured entries (the import manifest stays in the store).

Implementation note: a no-follow directory handle's `list` is single-shot
per handle (its duplicate shares the enumeration offset), so every walk in
this module enumerates through a fresh handle.

## Census for collection

`scan_refs(store_root, homes)` pins, for a caller-supplied page of extant
retained homes, every controlled cache-root link and every pending
`.agent-run-native-*` operation record into `NativeRefScan { complete,
trees, blobs, homes, backups }`, reading exact link text and manifests only
— it re-hashes no payload; the shared collector later proves each pinned
tree under the store lock. The pass is bounded per home (4096 parents, 32
backups) and in aggregate (200 000 blob paths, one 10 s budget) but never
by page size: a larger retained-home set is paged by the caller, resuming
at the returned `homes` offset and merging with `NativeRefScan::merge`. A
proven-absent directory is simply empty; any other unreadable home,
directory, link, record, or manifest sets `complete == false` while keeping
the partial references collected so far. Collectors must treat
`complete == false` as partial evidence and retain, never as an empty
reference set, and must drain obsolete native-cache trees and blobs only
through this census plus their own managed-asset reference sets.

The native cache is rebuildable (the SDK re-extracts it), so a failed
optional cache operation is reported rather than allowed to fail a valid
managed-asset resume; eviction-to-claim-savings is not a collection goal.

## Caller wiring (not in this unit)

The supervisor and storage-admin surfaces decide when a home is quiescent,
supply the trusted scope identity, call `freeze` on idle passes, and call
`thaw` before every native invocation that could mutate the cache.
Registry anchoring for cache-only homes, GC passes over `trees` and
`blobs`, and the `.system`-keeps-its-link policy are integrated there.
