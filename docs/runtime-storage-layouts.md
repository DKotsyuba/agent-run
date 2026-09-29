# Runtime storage layouts

`agent-run-store::runtime_storage` is the durable registry that records which
shared managed trees back one runtime home, and the admission gate that keeps
a home whose layout is still being installed from gaining a new continuation.
It stores receipts only: every original frozen authority, asset-index and
native-history digest stays exactly where it was, and no physical file
operation happens in this module.

## Registry table

`runtime_storage_layouts` (schema 22) has one row per canonical absolute
`runtime_home`:

| column | meaning |
| --- | --- |
| `runtime_home` | primary key; canonical absolute path, `/`-rooted, no `.`, `..` or empty segments |
| `index_sha256` | SHA-256 of the home's original frozen runtime asset index |
| `layout_json` | the canonical (sorted, compact) layout bytes, bounded to 64 KiB |
| `layout_sha256` | SHA-256 of exactly those bytes |
| `state` | `prepared` or `committed` |
| `operation_token` | compare-and-swap token of the preparing operation |
| `owner_agent_id` | the agent whose preparation recorded a still-pending row, when known |
| `updated_at` | store clock of the last state change |

There are no per-blob reference counts and no filesystem-staging fields. A
row's state is the only pin: garbage collection retains and retries any
shared target a pending row names, even when the object is not imported yet,
because a layout can be planned from the index's root manifest hashes and a
validated scope before any import runs.

## Layout contract (version 1)

The layout is a small `deny_unknown_fields` object; unknown fields and
unknown versions are refused rather than reinterpreted:

```json
{
  "version": 1,
  "runtime_home": "/…/runtime-homes/alpha",
  "index_sha256": "<64 lowercase hex>",
  "roots": {
    "assets/plugins": {
      "scope": "<64 lowercase hex>",
      "manifest_sha256": "<64 lowercase hex>"
    }
  }
}
```

`roots` maps each relative managed tree path (nonempty, no `.`, `..` or empty
segments, no root a component ancestor of another, 1–256 entries) to the
shared object backing it, named by the caller's scope domain and the original
canonical manifest digest. No URL and no absolute target path is stored: the
physical store path is derived later, by core, from its own trusted app home
plus this validated pair. The registry accepts only text that is already the
canonical encoding and only a digest matching those bytes, so a duplicate key
or reordered object can never be stored.

## Operations

`Store::prepare_runtime_storage_layout(layout_json, owner)` registers a row
as `prepared` in one `IMMEDIATE` transaction:

- With no existing row, it refuses while any agent still holds the home
  unresolved: a non-terminal or `lost` agent whose frozen identity binds this
  home, or any attempt of such an agent that is not positively proven inert.
  An attempt is inert only when it is `prepared` with no recorded process
  identity (nothing was ever spawned) or `cleanup_complete` with a proof that
  parses to `confirmed: true` or `never_spawned: true`. A claimed-but-unproved
  spawn, an unknown phase, and a released attempt (`ownership_active=0`)
  without verified cleanup all keep holding the home.
- The one holder itself may register, passed as `owner`, when it proves it
  holds nothing live. A `running` owner — the consolidation window before its
  terminal commit — must show every attempt with a confirmed cleanup proof,
  never merely `prepared` attempts.
- With an existing row the call is strictly idempotent: a `prepared` row
  returns itself unchanged (same operation token, same bytes) only for the
  same digest and the same owner claim that recorded it, and only while that
  owner still holds nothing live. Anything else, and always a `committed`
  row, is a conflict. Nothing is replaced, dropped or re-tokened here; moving
  a home to a different mapping is a separate, later operation with its own
  rollback evidence.

`Store::commit_runtime_storage_layout(runtime_home, operation_token,
layout_sha256)` flips a `prepared` row to `committed` only on an exact token
and digest match. Retrying the same values is idempotent, before or after the
commit; any other values conflict and overwrite nothing. A `committed` row
describes the mapping while the home exists and pins nothing against that
home's later deletion.

`Store::runtime_storage_layout(runtime_home)` returns the validated row, and
`Store::pending_runtime_storage_layouts(limit)` enumerates pending rows
oldest-first for recovery and garbage collection, clamped to at most 1000
rows. Both re-validate the stored bytes against the stored digest, so a
corrupted row is an integrity error rather than a mapping. Neither performs
any filesystem cleanup; deciding and executing recovery is a later unit.

`Store::remove_runtime_storage_layout(runtime_home)` is the only deletion. It
refuses while any agent row still binds the home in its frozen identity, and
the caller must already hold every proof at once: the physical home is gone,
no configuration references it, and every service reference to it was
released. Rows are never removed by age, agent-history expiry or cascade.

## Admission gate

Both continuation admission paths — the provider path
(`Store::admit_provider_resume` through `admit_provider_lineage`) and the
legacy path (`Store::admit` / `admit_with_config_revision` with a parent) —
check the registry inside the same `IMMEDIATE` transaction that inserts the
child. The home looked up is the frozen parent's recorded `runtime_home`,
never a request-supplied path. A `prepared` row refuses the new continuation
(`continuation_unavailable`); a `committed` row, or a parent with no recorded
home, admits unchanged. An already-created matching request replay returns
before the gate, so idempotent replays are not new executions. This closes
the race between a layout being installed into a home and a resume attaching
a new child to that home.

## Filesystem coordinator (`agent-run-core::runtime_storage`)

The coordinator owns the physical switch itself, above the registry and the
platform store. The supervisor calls it after the actual harness guard is
validated, never through a request-supplied path.

`store_root(app_home)` derives `<canonical app home>/shared-assets/v1`
read-only. A missing namespace is derived, not created (the launch path may
create the empty trusted root before guard validation); an existing namespace
must be a real directory reached without symlinked components, checked
through no-follow descriptors and canonical form.

`plan(store, app_home, runtime_home, expected, scope)` mutates nothing. It
strictly verifies the original home — exact index bytes hashing to the frozen
`expected` digest and every indexed root still a verified private directory —
or returns an existing committed row's layout when it binds the same digest.
Root references come only from the index's own `manifests` map, all under the
caller's validated scope; an index without managed roots plans `None`. A
prepared row is not a binding: a mid-switch home fails planning and directs
the caller to recovery.

`install(store, app_home, layout, owner)` runs the switch: strict original
verification, registry `prepare` under the store publish lock (pins before
any import; the lock is dropped before each import reacquires it — no nested
locks), idempotent imports of every mapped tree, per-root
move-into-backup + exact whole-tree link, shared-bridge verification of the
unchanged original index, compare-and-swap `commit`, then deletion of only
backups proven to still hold the replaced assets by manifest digest and
strict inspection. An existing committed row for the same layout verifies
idempotently; an existing prepared row for the same layout is finished by
roll-forward recovery; any other row is a conflict. Replaced originals are
staged inside the runtime home at
`.agent-run-storage-<operation_token>/<root>`, so the prepared row's
retention pin covers the recovery material itself; the token is validated as
`rt_`-prefixed lowercase hex before it becomes path material. Frozen index
bytes, native-history files, credential links and authority digests are
never rewritten. `install_with_fault` is the test-only seam that simulates a
crash at `BeforeRename`, `AfterRename` (the window where the home root name
does not exist), `AfterLink` (before the registry commit) or `AfterCommit`
(before cleanup).

`verify(store, app_home, runtime_home, expected)` chooses the verifier from
the registry: no row runs the original strict verifier unchanged, a committed
row binding the same digest runs the shared bridge with that row's
references, and a prepared row, a digest mismatch or a corrupt row is an
explicit failure.

`recover(store, app_home, runtime_home)` finishes only provably-owned work.
No row means nothing to do. A prepared row is rolled forward from its own
layout and token: still-private roots are imported and swapped, roots
stranded between rename and link are relinked from their staged backup,
already-correct links are left alone, foreign or missing roots are explicit
failures, the converted home must bridge-verify, and the row is committed. A
committed row only finishes cleanup. A backup that cannot be proven is left
in place with an explicit failure. Recovery never guesses from age and never
re-downloads: imports are idempotent reuses of pinned or existing objects.
Full operator-driven rollback remains a later unit; recovery here is
roll-forward only.
