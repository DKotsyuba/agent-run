# migrations

state.db's schema version is tracked in SQLite's own `PRAGMA user_version`.
Each schema change beyond the initial schema is a numbered SQL delta file
under `state/migrations/` (`NNN_slug.sql`), applied in order.

## Write paths migrate automatically

Any write-path open of the store runs pending migrations transactionally,
one `BEGIN IMMEDIATE` per migration, so an interrupted migration leaves the
store at its previous version rather than half-upgraded. Before migrating,
the store is snapshotted to `state.db.pre-vN.backup`; the backup is removed
only once the migration transaction commits, so a backup lingering on disk
after a restart is itself a signal that the last migration attempt did not
finish.

## Read paths never migrate

A read-only open of a store below the current schema version raises
`SchemaMigrationRequired` rather than silently upgrading it out from under
a process that did not ask to write. `agent-run doctor` reports this
condition as `state_migration_pending` — treat it as an instruction to run
a write-path open (e.g. any normal write command) rather than something to
route around.

## Version-too-new

If a store's `user_version` is higher than the running code's
`SCHEMA_VERSION`, that is an older release talking to a newer store — it is
refused clearly rather than opened partially. See `releases` for why older
processes must be respawned after a migration ships, not just left running.
