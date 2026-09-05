# releases

Public versions are built from an annotated `vX.Y.Z` tag by GitHub Actions.
The workflow verifies that the tag matches `pyproject.toml`, runs the full
suite, builds and checks wheel/sdist, performs a clean install smoke, records
`SHA256SUMS`, creates provenance attestations, and publishes the files in a
GitHub Release. Maintainer procedure: `docs/releasing.md` in the source tree.

Maintainers can publish and update an existing macOS sealed installation with
`python3 scripts/release.py X.Y.Z` from the source checkout. The command waits
for PR and main CI, the public GitHub Release, verified hashes and provenance,
then completes the local update. Repeat the same command to resume; an already
published version proceeds directly to verified installation. See
`docs/releasing.md` for prerequisites and options, including `--publish-only`.

## Operational runbook

Run from a clean checkout at the current `origin/main`:

```bash
# Publish and update the local runtime.
python3 scripts/release.py X.Y.Z

# Resume after interruption: use the same version.
python3 scripts/release.py X.Y.Z

# Publish without changing the local runtime.
python3 scripts/release.py X.Y.Z --publish-only
```

Wait for `Done: vX.Y.Z published and local runtime updated.` before treating the
release as complete. After a local update, reconnect existing MCP clients.

On failure, preserve the command output, `standalone/deploy.json`, and the
reported backup. Correct the reported cause and repeat the same command. Never
move a release tag, delete recovery evidence, or point an older runtime at a
newer database schema.

The sealed layout is not created by ordinary pip/pipx installation.

Sealed local releases live under `~/.agent-run/standalone/releases/<sha>`, one
immutable directory per shipped commit.

## Build steps

1. Download wheel, sdist and `SHA256SUMS` from the public GitHub Release.
2. Verify hashes and GitHub provenance, including the expected repository,
   release workflow, tag, commit and signed artifact subject.
3. Create a fresh venv inside the release directory using a managed Python 3.14
   interpreter, then install the wheel into it.
4. Write `SHA256SUMS` covering every file in the release directory except
   `COMPLETE` and `SHA256SUMS` itself.
5. Run isolated-home `init`, `doctor`, API ping/tools and MCP `tools/list` checks.
6. Write `COMPLETE` containing exactly `complete\n` — its presence is the
   signal that the release directory is safe to switch to. A release
   directory without `COMPLETE` must never be switched to, even if every
   file looks present.

## Atomic switch

The maintainer script waits for all active agents and workflows, closes API
admission under a SQLite writer reservation, stops loaded periodic jobs, and
rechecks quiescence. It backs up the database and config before migrating with
the new runtime, then switches `current` with a temporary symlink and
`os.replace`. Services restart on a schema-compatible runtime. Do not manually
switch to an older binary after a schema migration.

## Retention

The script keeps all sealed releases and backups. It never prunes old releases
while long-lived clients may still be running from them. Failed deployments
retain `standalone/deploy.json` and the reported backup directory for recovery.

## After a schema migration

Any long-lived process still running out of an older release directory
refuses to open a store that a newer release has already migrated — it
does not silently misread it. Such processes must be explicitly respawned
(from the new release) after a migration ships; a schema bump is not
transparent to processes that started before it.
