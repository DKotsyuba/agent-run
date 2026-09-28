# History retention

The resident broker automatically expires completed database history **14 days
after `finished_at`**. It runs on startup and then hourly. A backlog drains in
small transactions, with a one-second pause between batches; failures retry
after a minute. Filesystem cleanup runs on the same schedule even while the
database has a backlog. No cron job or provider-specific configuration is needed.

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

An expired large transcript can disappear progressively before its agent row is
removed. Each transaction considers at most 32 agents and deletes at most 2,000
rows per large journal. Foreign keys stay enabled. SQLite's progress callback
interrupts a batch after two seconds; rollback preserves earlier committed work.

After the backlog drains, the broker runs SQLite `incremental_vacuum` in batches
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
14 days by the canonical run ID timestamp. Obsolete configuration/profile
backups and completed deployment backups also expire after 14 days. Applied
migration snapshots expire only with their completion and applied markers;
an unfinished migration or deployment protects recovery data. Retained agents,
attempts, registered `file:` credentials and the parsed current configuration
protect every referenced path. Unreadable or malformed protection evidence
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
keeps live directory scans between passes so an undeletable first batch does
not starve later entries. At most 64 streams remain open; the least recently
used scan restarts at the beginning if that limit is reached. A broker restart
also rescans from the beginning; immutable
names and fresh reference checks make partial cleanup safe to resume. Unknown,
foreign-owned, special or unreadable entries are retained.
