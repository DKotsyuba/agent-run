# Native directory cache lifecycle

Retained Codex homes each duplicate the same unindexed native directory
caches: the generated `skills/.system` skills tree, the
remote-downloaded plugin parents under `plugins/cache/<market>/<plugin>`,
and the native curated plugin marketplace clone's working tree and Git
packs under `.tmp/plugins`.
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
- `.tmp/plugins/plugins` (`CuratedMirror`) — exactly the working-tree
  plugins directory of the native curated marketplace clone, frozen only
  while the clone's `.tmp/plugins/.git` is a real directory. The native
  startup sync (Codex `core-plugins/src/startup_sync.rs`) fetches into the
  existing private `.git`, stages a replacement repository and activates it
  by renaming the whole `.tmp/plugins`, and the plugin manager copies
  installed payloads out of this tree, so it is thawed before every native
  invocation exactly like a remote plugin parent.
- `.tmp/plugins/.git/objects/pack` (`CuratedPacks`) — exactly the clone's
  Git pack directory, frozen only while it holds nothing but regular
  `pack-<40|64 hex>.<pack|idx|rev|bitmap|keep|mtimes|promisor>` files.
  Packs are immutable and byte-identical across homes cloned from the same
  upstream state (measured: a 24,205,581-byte pack plus index and reverse
  index), and are thawed before every native invocation so fetch and
  repack keep native behavior.

Only those two exact paths of the clone are ever shared. HEAD, refs,
index, logs, config and locks under `.git`, `.agents`, the clone's root
files and the `.tmp/plugins.sha`/`.tmp/plugins.sync.lock` siblings stay
private and untouched (bytes, inode and modification time), so native Git
state, the fast-path SHA check and the per-home sync lock keep exactly
their native behavior; recovery never drops them. A clone without a real
`.git`, an empty pack directory, a `multi-pack-index`, a temporary pack or
any other shape is skipped unchanged.

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
tree (bounded walk: 16384 entries, 32 MiB per file, 128 MiB aggregate,
depth 16, a 4 MiB canonical manifest, 10 s), builds a managed-snapshot
staging copy inside the operation backup — one APFS directory clone with
file modes normalized, or per-file clones where directories cannot be
cloned — imports
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
- `thaw` restores the verified tree as one APFS directory clone (or
  per-file clones where directories cannot be cloned) — independent
  writable inodes with the manifest's logical modes, never an external
  hardlink — and the restored native tree contains exactly the captured
  entries (the import manifest stays in the store).
- Disposable staging copies, staged clones and proven originals are built
  and removed without per-entry flushes. A staged thaw clone becomes
  authoritative only after every staged file and directory was pushed with
  plain `fsync(2)` (`Dir::push_tree`) and one `Dir::sync` barrier returned;
  a proven freeze original is first renamed to `discard` inside its backup
  so a crash mid-removal leaves a subtree recovery removes without
  re-proving. A freeze whose captured manifest's store tree already exists
  and fully verifies skips the staging copy and import entirely.

Durability semantics (from the local std implementation and `fcntl(2)`):
`File::sync_all` — every "durable" primitive — is `F_FULLFSYNC` on Apple
hosts, which drains the device queue so data "fsync'd on the same device
before is guaranteed to be persisted when this call returns"; plain
`fsync(2)` alone may still be reordered or lost by the device. Batches
therefore push each changed object with `fsync(2)` and cross one
`F_FULLFSYNC` barrier before anything depends on them; a single directory
flush never stands in for its children. On other hosts `fsync(2)` is the
durable operation, so the same ordering keeps the prior durable behavior.
The store publisher uses two barriers: every new payload streams into an
exclusive temporary blob and is pushed, one barrier persists them all,
and only then are the temporaries renamed to canonical names (so a
canonical name never refers to non-durable data); the staged tree's
directories, links and readonly modes are pushed per directory and a
second barrier persists them before the no-replace rename publishes the
tree. A failure before the first barrier publishes nothing; leftovers of a
later crash (canonical blobs, temporary blobs, staging trees) are verified
on reuse or collected as orphans.

Shared-tree bounds (the platform store and this unit agree): 16384 manifest
entries, 32 MiB per payload, a 4 MiB manifest read bound
(`shared_assets::MAX_TREE_MANIFEST_BYTES`) for shared-tree manifests only,
256 MiB aggregate per store tree; the launch guard scans at most 400 000
store paths. Runtime indexes, `op.json`, markers, plugin views and managed
snapshots keep their 64 KiB bounds; manifest bytes and digests are
unchanged.

Measured on two homes with the measured curated shape (5380 files, 2352
directories, depth 10, ~53.6 MB, plus the 24 MB pack), release build:
unique-inode bytes 156.1 MB private → 79.1 MB idle-shared (157.1 MB with
one home thawed, as clones), first publication of both curated roots
≈11 s (was ≈130–150 s with a full flush per object), freeze of a home
whose trees already exist ≈3 s, refreeze of an unchanged home ≈3.3 s,
thaw of both roots ≈2.4 s (≈1.5 s is the full content verification),
store guard scan ≈0.14 s. The remainder is per-entry metadata syscalls
(`linkat`, `openat`, `fchmodat`, `unlinkat` at ~0.1–0.5 ms each on APFS
for 7732 entries), not flushes. Run the benchmark with
`cargo test --release -p agent-run-core --features test-fixtures --test
native_tree_cache measure_curated_increment -- --ignored --nocapture`.

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
the partial references collected so far — and an unrecognized link in a
supported cache slot (relative, foreign, or malformed, including a
relative spelling that resolves onto a live store tree) is exactly such
unknown reference evidence: the link is left untouched, never followed or
normalized, and the pass reports itself incomplete so a collector retains
rather than concluding the slot references nothing. Collectors must treat
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
