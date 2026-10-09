# Native metadata-cache storage

Codex keeps two best-effort cold-start metadata caches under a home, both
written by `codex-rs/connectors/src/connector_runtime/persistence.rs`
(rust-v0.156.1): `cache/codex_apps_tools/<identity>.json` (disk schema 4)
and `cache/codex_apps_server_info/<identity>.json` (disk schema 1). Each
file is bounded at 32 MiB, and `<identity>` is the SHA-1 of the account key
— a 40-lowercase-hex filename that excludes `CODEX_HOME` and is never an
account label. `agent-run-core::native_cache` stores each eligible file's
bytes **once** in the shared managed-asset store and leaves one exact file
symlink in the home, without changing any native behavior:

- Native reads use `File::open`, which follows the file symlink to the
  shared object.
- Native refresh writes a `NamedTempFile` inside the private cache
  directory and atomically `persist`s (renames) it over the cache path.
  Rename replaces the symlink itself and leaves the shared object
  untouched, so a refresh silently re-privatizes that entry until a later
  repack. The cache directories stay real private directories — native
  stages its temporaries inside them — and no directory is ever linked.
- The disk caches are best-effort cold-start data only; a live fetch still
  contacts the server, and readers apply their own config. Nothing here
  reads, extracts or rewrites auth material, config, account bindings,
  history, plugins, data, or any managed index.


Optional packing/freezing is deferred to explicit offline `storage compact
--apply`, under its existing startup locks and all-terminal ownership checks.
Live completion retains private native caches and does not perform this work
before notifying the owner or schedule it after terminal state. Nonblocking
publication-lock skips and cooperative budgets do not preempt filesystem I/O.

## Layout and scope

Objects live in one fixed namespace inside the caller-owned shared store,
beside the existing `trees/`, `blobs/` and `plugin-views/` namespaces, under
the same store-wide publish/GC lock (`.publish.lock`) and the same guard:

```
<app home>/shared-assets/v1/native-cache/<scope>/<content-sha256>
```

- `<content-sha256>` is the SHA-256 of the entry's captured bytes; the
  object is a regular file at mode `0o400`, published no-replace and never
  rewritten. No tree payload is copied and no per-run timestamp ever enters
  a name: one payload under one scope is exactly one object.
- `<scope>` is the SHA-256 of the exact preimage

  ```
  native-cache-v1\n<cache dir>\n<schema>\n<identity file>\n<effective uid>\n<compatibility domain>
  ```

  so payloads never alias across file kind (tools vs server info), native
  disk schema, native identity, effective user, or the endpoint/harness
  compatibility domain the trusted caller supplies. A bare filename never
  proves compatibility across endpoints; the domain label is explicit
  trusted input (`NativeCacheDomain`), bounded to 256 UTF-8 bytes.

- The home entry becomes one symlink whose target is exactly that object
  path — absolute, derived only from the validated store root.

## Packing

`pack_native_cache(store_root, home, domain)` covers one home and one of
the two cache directories (the domain's kind). Admission is not the
packer's: the caller proves quiescence and the account/identity binding
through the existing store checks. The packer owns the filesystem safety
envelope only:

- The store root and home must be canonical absolute real directories owned
  by the effective user, aliasing neither the other nor being aliased by it.
- Before anything is touched, a finalized managed index
  (`.agent-run-snapshots.json`) in the home is parsed read-only through the
  bounded v1 schema. Any indexed root, flat managed file or managed link
  overlapping the cache directory — above it, below it or equal to it —
  refuses the whole pack (`ManagedIndexOverlap`), because packing would
  rewrite indexed material; an index that cannot be read or parsed refuses
  the same way (`ManagedIndexUnreadable`). The index bytes are never
  rewritten, and a symlinked cache path (the shape a converted managed root
  would leave) is refused as `ManagedRootOverlap`.
- Every mutation holds `SharedStoreLock`. Bytes publish first (exclusive
  temporary, mode `0o400`, the proven source mtime stamped and fsynced
  before the no-replace rename); only then does the home entry switch: one
  complete temporary symlink `.agent-run-native-*.tmp` is created and
  atomically renamed over the original name. An interrupted switch
  therefore always leaves either the original file or the complete shared
  link under the native name — never a missing name, never a partial link —
  plus, at worst, one recoverable temporary-link orphan that later packs
  classify as a foreign link and never delete. The name is re-verified to
  still hold the exact captured inode immediately before the replace, so a
  native refresh that already landed stays live.
- Eligible entries are exactly `<40 lower hex>.json` regular files owned by
  the effective user, at most 32 MiB, whose bytes parse as the kind's
  native disk schema (`schema_version` 4 or 1; other payload fields are
  opaque). Everything else is preserved in place with an explicit
  disposition: unsupported names or kinds, oversized, malformed,
  foreign-owned, foreign links, entries that changed identity during
  capture, and objects whose existing content-addressed name fails
  verification. An already-shared link is claimed `AlreadyShared` only
  after its target object is re-verified (owner, mode, content digest);
  a drifted target reports `ExistingObjectDrifted` instead of silently
  losing the reference.
- A source file changing during capture keeps the original: the captured
  descriptor's `(device, inode, length, mtime)` must still hold, and the
  name must still resolve to that inode, before anything is published or
  linked.

## Freshness semantics (exact)

Store-object mtimes are the *minimum proven source mtime*, never the import
wall clock:

1. First publication of a payload stamps the object with the captured
   source file's mtime, and that timestamp is flushed (set, then fsync)
   **before** the object becomes reachable under its final name and before
   any home entry links to it — a crash can never leave a freshly-stamped
   object for stale bytes.
2. A later pack of the same payload under the same scope from a home whose
   source mtime is **older** *lowers* the object's mtime to it, under the
   publish lock (`Packed { lowered_mtime: true }`), and the lowered
   timestamp is flushed before any link is published onto the object.
3. A newer source mtime never raises — never freshens — the object.

A reader resolving its cache link therefore sees data at least as old as
its own proven source: cold-start freshness can only under-estimate, never
fabricate. Content and mode are immutable for the object's lifetime.
Identical payloads supplied by many homes or runs keep exactly one object.

## Garbage collection contract

This unit never deletes anything. The collector integration consumes:

- `enumerate_native_cache_objects(store_root)` — every canonical object
  (owned regular file at `<64 hex>/<64 hex>`); anything else inside the
  namespace, including publisher staging temporaries
  (`.agent-run-staging-*.tmp`), is reported as `foreign` and retained.
  `complete == false` means the enumeration is not exhaustive: delete
  nothing.
- `collect_native_cache_references(store_root, homes)` — the exact live
  physical references: only home entries that are symlinks whose target is
  exactly `<trusted root>/native-cache/<64 hex>/<64 hex>`, where the target
  object exists, is owned, is at mode `0o400`, and its bytes hash to its
  own name. Tampered targets, dangling targets and unrecognized links are
  reported separately and are not references.

A deletion candidate is an object the complete object census found that no
complete reference census covers — and nothing else:
`deletion_candidates(&objects, &refs)` returns the empty set unless **both**
censuses are `complete`. Corrupt or missing evidence always pins: a drifted
link (target object fails verification), a dangling link (target missing),
an unrecognized link in a cache slot — relative, foreign, or malformed,
including a relative spelling that resolves onto a live store object; the
census never follows or rewrites such a link — an unscanned home, an
unreadable directory, a foreign shape inside the
namespace, or any scan bound forces `complete == false` — never an empty
success — so a still-linked tampered object is never deletable, and an
out-of-namespace link is unknown reference evidence that pins every
deletion candidate rather than proving none.
Physical references suffice: a complete reference census must include every
retained and extant protected home, including cache-only homes registered
in the runtime-storage layout registry. A pack's staging orphans and
`.agent-run-native-*.tmp` link orphans follow the same lock-protected,
exact-name-shape cleanup rules the other namespaces use; removal is the
collector's job, never the packer's.

## Units that stay separate

The packer does not own admission, account identity derivation, or the
compatibility-domain label; wiring supplies a proven quiescent home and a
trusted domain label. Directory-shaped caches (skills, remote plugin
parents) are a separate unit with its own namespace. Nothing in this unit
touches the snapshot index, history, credentials or the managed roots.
