# History retention

The resident broker automatically expires completed database history by two
independent rules: **14 days after `finished_at`**, and a **bounded count of
logical sessions**. A logical session is one resume lineage — every run
sharing a `root_agent_id`, however many resume rows it holds, counts as one
session. All logical sessions are ranked by the latest `created_at` admitted
in the lineage, newest first, with the root id as a deterministic
tie-breaker; everything ranked below the newest **100** becomes eligible for
count expiry however recent it is. The cap only ever adds expiry: the newest
hundred are protected from count expiry but not from the fourteen-day age
rule, which still applies to them on its own schedule. A recent resume
therefore counts once and lifts its whole lineage into the protected set,
matching the intuition that resumed work is still current history.

Database expiry runs on startup and then hourly; a
backlog drains in small transactions with a one-second pause between batches.
Filesystem cleanup keeps its own independent schedule — a one-second cadence
while entries remain, hourly when idle — so a filesystem backlog never forces
a database writer transaction every second, and a database backlog never
starves reclaiming disposable files. A pass that finds nothing eligible
proves the database idle with a read-only query (age windows plus a cheap
session-count superset) and skips the writer transaction entirely. Failures
and deferrals retry after a minute. No cron job
or provider-specific configuration is needed.

Maintenance yields to normal workload. Every maintenance connection uses a
100 ms busy timeout, so a held writer makes a prune or compaction pass defer —
busy, locked and interrupted SQLite outcomes are classified as deferrals, not
data-integrity failures and never as completed passes — and the work resumes
on the next retry. Diagnostics for a failed or deferred pass log only a static
operation and stage plus SQLite numeric result codes and static code names
(for example `sqlite_primary=5 sqlite_primary_name=DatabaseBusy
sqlite_extended=517 SQLITE_BUSY_SNAPSHOT`); SQLite messages, SQL text, paths
and payloads are never logged because they can echo configuration or
transcript content.

Expired agents lose their transcripts, events, commands, delivery attempts,
usage rows, attempts and process snapshots. Their completion notices, including
undelivered queued notices, expire with them. Old completed workflows, unused
orchestrator sessions, stopped service generations and quota samples are also
removed. Deleted agents are no longer available through history or resume.

Retention preserves:

- Active agents and attempts whose process ownership has not been released.
- Ancestors of retained continuation chains and agents still used by a retained
  workflow. Old chains are removed from the tail in successive batches.
- Agents with unreleased service leases and notices with a live sending lease.
- Provider accounts, credential references, current quota exhaustion latches,
  running services and unresolved readiness probes.

Every protection above wins over the numerical session cap. A count-expired
lineage that still holds active work, unresolved ownership, a live lease or a
workflow reference stays stored, so more than 100 logical sessions can survive
temporarily; the count never forces an unsafe deletion. Conversely the cap
never rescues age-expired history: a lineage older than fourteen days still
expires on schedule even when fewer than 100 sessions exist. A lineage with an
unknown completion time (`lost` runs without `finished_at`) is retained as
uncertain evidence under both rules.

A retired multi-run lineage drains tail-first: its newest leaf run is deleted
first, then the exposed predecessor, until the root goes. Removing a tail row
can only lower its lineage's latest `created_at`, so a draining lineage can
never drift back above the protected boundary mid-drain, and each batch
removes only fully drained agents so no ancestor is ever stranded behind a
broken reference. Sessions are recounted from durable rows on every pass, so
the boundary converges to at most 100 unprotected lineages without drifting.

An expired large transcript can disappear progressively before its agent row is
removed. Each transaction considers at most 32 agents and deletes at most 2,000
rows per large journal. Foreign keys stay enabled. SQLite's progress callback
interrupts a batch after two seconds; rollback preserves earlier committed work.

After the database backlog drains, the broker runs SQLite `incremental_vacuum` in batches
of at most 1,024 pages (4 MiB with the usual page size), pausing one second between
batches until free pages are reclaimed. Compaction waits for agents,
workflows and managed services to become idle. Unresolved terminal ownership
records are retained but do not prevent repacking pages. Compaction runs on a
blocking worker with a three-second SQLite progress deadline and a short lock
wait, so new requests do not inherit an unbounded database lock. Progress
deadlines are cooperative and do not interrupt an operating-system I/O call.
Interrupted compaction retries later; completed batches retain their progress.

The broker checkpoints/truncates the WAL when idle, including retries after a
reader delayed a previous checkpoint. Schema 19 enables incremental mode for
new databases. Upgrading an older database performs one full `VACUUM` under the
migration lock with a pre-migration backup, before committing schema 19; ordinary
broker maintenance never performs a full rebuild. This one-time preparation
needs temporary free disk space and can take time on large databases. During
the paired configuration upgrade it runs on the staged database with the broker
stopped. A failed later schema step keeps the backup and old version; a retry
can reuse completed physical preparation.

Filesystem cleanup removes recognized, owned data after durable references are
gone. Orphan `agents/<run-id>` and runtime `runs/<run-id>` trees expire after
14 days by the canonical run ID timestamp. In addition, while at least 100
logical sessions are stored, a tree whose id predates the count boundary —
the latest `created_at` of the hundredth-newest session, computed once per
pass inside the bounded protection snapshot — is reclaimed as count-retired
without waiting out that window, provided one fresh indexed store read proves
no agents row for the id exists. The boundary remains actionable at exactly
100 sessions, which is where database retention converges, and disappears
below the cap, where a recent orphan keeps the fourteen-day rule. The row
read happens after the pass has observed the tree, and run trees are only
created after their agent row commits, so an admission the pass's protection
snapshot missed — including a run admitted within the same wall-clock second,
an id generated before its admission, or a clock that rolled back — is still
found and its tree kept. The durable row, never the id's second-resolution
timestamp, is the authority. This is what makes disk reclamation converge for
count-expired sessions of any age. Obsolete configuration/profile
backups and completed deployment backups still expire after 14 days. Applied
migration snapshots expire only with their completion and applied markers;
an unfinished migration or deployment protects recovery data. Retained agents,
attempts, registered `file:` credentials and the parsed current configuration
protect every referenced path, and a retained row keeps both its `agents/` and
runtime trees whatever their age. Unreadable or malformed protection evidence
blocks deletion rather than becoming an empty reference set.
The retained-reference snapshot is read only and limited to 20,000 metadata
rows, 64 MiB of structural evidence and a cooperative two-second SQL deadline;
it excludes task text. If proof fails, filesystem cleanup retries after a
minute while database expiry and vacuum continue independently.

Component logs now append directly to UTC-daily
`<home>/logs/<component>.YYYY-MM-DD.log` files. Daily files older than 30 days
expire after their last write is also older than 30 days. Undated legacy
component logs and launchd stdout/stderr files remain: an idle open writer
cannot be proven closed from age or an advisory lock, so automatic unlinking
could lose its next line. Live Desktop relay sockets remain; a bounded
nonblocking probe unlinks only a refused socket whose inode is unchanged.

Each filesystem pass scans at most 1,024 entries, begins at most 16 tree
roots, unlinks at most 256 descendant entries and 64 standalone files, probes at
most 16 sockets, and stops after two seconds or 32 tree levels. The broker
remembers completed namespaces for one scan round, so differently sized
directories do not keep the one-second maintenance loop running indefinitely.
SQLite compaction does not wait for filesystem traversal to finish.
Live directory scans keep an undeletable first batch from starving later
entries. At most 20,000 namespace keys and 64 streams are retained; excess
bookkeeping resets the round and retries after a minute. The least recently
used scan restarts at the beginning if that limit is reached. A broker restart
also rescans from the beginning; immutable
names and fresh reference checks make partial cleanup safe to resume. Unknown,
foreign-owned, special or unreadable entries are retained.
