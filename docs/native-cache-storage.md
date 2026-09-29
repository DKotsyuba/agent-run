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
- Every mutation holds `SharedStoreLock`. Bytes publish first (exclusive
  temporary, fsync, mode `0o400`, source mtime, no-replace rename); only
  then does the home entry switch by an exclusive rename into a
  `.agent-run-native-*.tmp` backup name followed by one no-replace symlink.
  An interrupted pack leaves either the original file or the shared link —
  never a partial entry. A native rewrite landing inside the window wins
  and stays live (`NativeRewrote`).
- Eligible entries are exactly `<40 lower hex>.json` regular files owned by
  the effective user, at most 32 MiB, whose bytes parse as the kind's
  native disk schema (`schema_version` 4 or 1; other payload fields are
  opaque). Everything else is preserved in place with an explicit
  disposition: unsupported names or kinds, oversized, malformed,
  foreign-owned, foreign links, entries that changed identity during
  capture, and objects whose existing content-addressed name fails
  verification.
- If a known cache path (or its `cache` parent) is itself a symlink — the
  shape a converted immutable managed root would leave — the whole
  directory is skipped untouched (`ManagedRootOverlap`).
- A source file changing during capture keeps the original: the captured
  descriptor's `(device, inode, length, mtime)` must still hold, and the
  name must still resolve to that inode, before anything is published or
  linked.

## Freshness semantics (exact)

Store-object mtimes are the *minimum proven source mtime*, never the import
wall clock:

1. First publication of a payload stamps the object with the captured
   source file's mtime.
2. A later pack of the same payload under the same scope from a home whose
   source mtime is **older** *lowers* the object's mtime to it, under the
   publish lock (`Packed { lowered_mtime: true }`).
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
  own name. Tampered targets, dangling targets and foreign links are
  reported separately and are not references.

A deletion candidate is an object the complete object census found that no
complete reference census covers. Both censuses fail **incomplete** — never
empty-success — on I/O failure, unreadable directories, foreign shapes or
bounds; `complete == false` must always mean "retain everything and retry".
Physical references suffice: a complete reference census must include every
retained and extant protected home, including cache-only homes registered
in the runtime-storage layout registry. A pack's staging orphans and
`.agent-run-native-*.tmp` backups follow the same lock-protected,
exact-name-shape cleanup rules the other namespaces use; removal is the
collector's job, never the packer's.

## Units that stay separate

The packer does not own admission, account identity derivation, or the
compatibility-domain label; wiring supplies a proven quiescent home and a
trusted domain label. Directory-shaped caches (skills, remote plugin
parents) are a separate unit with its own namespace. Nothing in this unit
touches the snapshot index, history, credentials or the managed roots.
