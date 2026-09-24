# History retention

The resident broker automatically expires completed database history **14 days
after `finished_at`**. It runs on startup and then hourly. A backlog drains in
small transactions, with a one-second pause between batches; failures retry
after a minute. No cron job or provider-specific configuration is needed.

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

This policy applies to `state.db`. Run directories, sealed answers, runtime
homes, migration snapshots and installation backups on disk are not removed.
