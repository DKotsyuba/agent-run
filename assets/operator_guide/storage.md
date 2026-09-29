# Storage: shared managed assets, compaction, recovery

Runtime homes keep their frozen index, flat config, credential link and native
history private. Their indexed managed trees can live in the caller-owned
shared store under `<home>/shared-assets/v1` instead, one committed layout row
per home binding the home to the shared trees it installed. See
`docs/shared-runtime-assets.md` and `docs/runtime-storage-layouts.md` for the
physical contracts.

## `agent-run storage status`

Read-only. It reports the state schema, the store root, the store's measured
unique inode bytes (a bounded walk; a partial one is labelled `incomplete`,
never passed off as a total), the filesystem's own free space as a separate
number, and one entry per retained runtime home with its classification:

- `shared` — a committed layout row backs the home.
- `eligible` — a private home the read-only planner can still strictly verify.
- `prepared` — an interrupted relocation holds the home; run `storage recover`.
- `protected` — an active or lost holder keeps it, or it has no managed roots.
- `gone` — the physical home is gone; only its registry row may remain.
- `unknown` — evidence is missing, unreadable or tampered. Never touched.

Homes come only from frozen identities and configured run roots. A home no
identity records is reported `unknown` and never migrated. Logical savings
(unique inode bytes) are never reported as physical free-space gains: clones,
snapshots and hardlinks mean the two numbers move independently.

## `agent-run storage compact` / `--apply`

The default is a dry run: the survey above plus exactly what collection would
reclaim, writing nothing. `--apply` additionally:

1. holds the broker and service-manager startup locks (`.api.sock.lock`,
   `.services.lock`) — the same exclusion `config migrate --apply` uses, so a
   resident broker or service manager cannot race it;
2. refuses while any agent is active;
3. relocates each eligible home behind the real supervisor guard
   preflight, replayed from that agent's recorded identity, frozen config and
   recorded account — never the current provider, never a rewritten config. A
   home whose workdir, binary, grants or sandbox boundary cannot be verified
   is skipped with its reason and preserved byte count;
4. runs one reference-aware collection pass over the shared store.

## `agent-run storage recover`

Offline (same locks, same active-agent refusal). It finishes only relocations
this home provably owns, resuming the exact operation token, shared
references and staged backups: still-private roots are imported and swapped,
roots stranded mid-switch are relinked from their staged backup, the converted
home must bridge-verify, and the row commits. A foreign or tampered home is
reported `refused` and left exactly as found. Recovery never starts a model
and never rolls a committed layout back — restoring a private home is a
separate, explicit operation.

## Collection rules

Collection runs inside the existing bounded housekeeping and socket
maintenance cycles — no new daemon. It takes the store's publish/GC lock
nonblocking (a busy publisher means the pass simply retries), re-derives every
reference while holding it, and only then unlinks: a `prepared` row always
pins what it names, a `committed` row pins while its home exists, and any
configuration, frozen identity, credential or unreleased service path pointing
into the store pins the tree, view or blob it names — including the tree
behind a protected view.

**A complete proof precedes any deletion.** A reference census that is
partial, unreadable, corrupt or beyond its bound retains every candidate and
reports `incomplete` for retry; nothing is ever deleted from partial
evidence. A retained tree is proved by the store's own full verifier, not a
bare manifest read; an unverifiable tree, a missing pinned tree, a foreign
name in a namespace, an unresolvable home (permission errors are **not**
proof a home is gone) or an unreadable configuration each stop destructive
work for the pass. Views are collected before the trees and payloads beneath
them; blobs only after one pass enumerated every remaining tree, and only
canonical payload names are candidates. Committed rows are removed only once
their home is conclusively gone and unreferenced; `prepared` rows never age
out. Staging orphans need exact owned provenance, never age, and a drain that
cannot finish stays resumable in place.

Passes are bounded and converge: namespaces stream in fixed-size batches,
referenced objects are checked by name so retained objects ahead of garbage
cannot starve it, and a verified tree's references are remembered for the
process. The dry run writes nothing anywhere — not even the lock file: a
store without one reports `lock_busy` instead of a fabricated preview.

Homes are classified from all their recorded holders together: a resume
lineage of terminal executions sharing one home is eligible, only an active
or lost holder protects it, and qualification uses the latest terminal
execution's seal. The report prints a holder count, never per-run
identifiers.

## Unsupported

- A state database older than the current schema is refused with
  `migration_required`; run `agent-run config migrate` first. Storage commands
  never upgrade the database implicitly.
- Native unindexed caches are not shared or collected here.
- A historical resume whose native session is broken stays broken: recovery
  repairs interrupted relocations, not native sessions.
