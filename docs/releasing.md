# Releasing agent-run

The canonical package version is `[project].version` in `pyproject.toml`.
Releases use annotated Semantic Versioning tags (`vX.Y.Z`); the release workflow
refuses a tag that does not match the package version.

## Runbook

Start from a clean maintainer checkout at the current `origin/main`. The command
validates that precondition before changing anything.

| Operation | Command |
|---|---|
| Publish and update the local runtime | `python3 scripts/release.py X.Y.Z` |
| Resume an interrupted release | Run the same command with the same version |
| Publish without a local update | `python3 scripts/release.py X.Y.Z --publish-only` |

For example:

```bash
python3 scripts/release.py 0.6.3
```

Wait for the command to finish. Success is reported as:

```text
Done: v0.6.3 published and local runtime updated. Reconnect existing MCP clients.
```

If the command fails, preserve its output, `<home>/standalone/deploy.json`, and
the reported backup directory. Correct the reported cause and run the same
command again. Do not move an existing tag, delete a backup or deployment
journal, or point an older runtime at a newer database schema. A failed CI or
Release workflow remains failed evidence; inspect it before retrying or issuing
a corrected patch version.

## What the command does

The command waits until the GitHub Release is public, verifies the downloaded
wheel and sdist against `SHA256SUMS` and GitHub attestations (repository,
workflow, tag, commit, and subject hash), then updates the local sealed runtime.
It requires authenticated `gh`, `git`, Python 3.14, and the existing macOS
standalone installation and API/capacity/delivery launchd plists.

For a new version, start from a clean committed checkout equal to `origin/main`.
The script prepares only `pyproject.toml` and `CHANGELOG.md` on the deterministic
`release/vX.Y.Z` branch in a dedicated worktree. It reuses an existing changelog
entry, or generates concise notes from public commit subjects. It opens/reuses
the pull request, waits for all CI jobs and nonempty required checks at the
exact head, merges without bypassing branch protection, waits for main CI at the
merge commit, and pushes an annotated tag. The existing Release workflow alone
publishes artifacts; the script never creates or overwrites a GitHub Release.

Run the same command after interruption. An already public version validates its
annotated tag and package version and goes directly to asset verification and
installation, even from a dirty checkout. Existing PRs, commits and tags are
reused; mismatched immutable state, failed/cancelled checks, missing evidence at
the deadline, and corrupt sealed releases fail explicitly. Correct the reported
cause (for example, rerun a failed workflow) and repeat the command.

Options: `--publish-only` skips local deployment; `--home` defaults to
`~/.agent-run`; `--python` defaults to `python3.14`; `--launchd-prefix` defaults
to `com.<login>.agent-run`; `--timeout` and `--poll` are positive seconds,
defaulting to 3600 and 10. Publication itself can run on other operating systems.
Timeouts stop waiting; they do not cancel a remote workflow or active agent.

Local deployment installs only the verified wheel, verifies or reuses a sealed
release, and runs isolated init/doctor/API/MCP checks. It waits for every active
agent and workflow, reserves the SQLite writer while stopping API admission and
loaded periodic jobs, then rechecks quiescence. Before migration it backs up
SQLite with its backup API, saves configuration and the previous pointer under
`<home>/standalone/backups/`, and records a private deployment journal. Migration
uses the new installed `StateStore.initialize`; the current symlink changes
atomically only after the target passes the COMPLETE/manifest gate. It restores
the previously loaded services and checks version, schema, database integrity,
API liveness/tool discovery and doctor. It keeps all old releases and backups.
After service bootstrap, it waits for API readiness before inspecting SQLite.
Transient SQLite open/lock failures are retried within the command deadline;
missing files, corruption and version/schema mismatches still fail immediately.

On failure, recovery chooses a binary compatible with the database's actual
schema. Once the database advances, recovery completes migration and starts the
new binary; it never silently points an old binary at a newer schema. The
journal at `<home>/standalone/deploy.json` remains for retry. If recovery itself
needs to wait for SQLite, it has an independent 120-second allowance. An already
migrated target is reused without running migration again. If recovery itself
fails, the error identifies the journal and backup and explicitly warns that
services may remain stopped. Preserve those files and resume the same version.
Incomplete candidate directories are preserved under an `.incomplete-<time>`
suffix before rebuilding; a corrupt or incomplete current release is refused.
Reconnect existing MCP clients after the update; application hosts are not killed.

## Manual publication

1. Update `pyproject.toml` and `CHANGELOG.md` in the same pull request.
2. Run the full test suite and build checks locally.
3. Merge only after the `CI` workflow passes.
4. Create and push an annotated tag from the accepted `main` commit:

   ```bash
   git tag -a v0.1.0 -m "agent-run 0.1.0"
   git push origin v0.1.0
   ```

The tag-triggered `Release` workflow repeats the tests, builds wheel and sdist,
runs package/install smokes, writes `SHA256SUMS`, creates GitHub provenance
attestations, creates a draft GitHub Release, and publishes it only after every
gate succeeds. A failed run leaves no public partial release.

Verify downloaded artifacts with:

```bash
# Linux
sha256sum -c SHA256SUMS
# macOS
shasum -a 256 -c SHA256SUMS
gh attestation verify agent_run-0.1.0-py3-none-any.whl \
  --repo DKotsyuba/agent-run
```

## Distribution boundary

GitHub Releases is the only public distribution channel. Each release carries
the Python wheel, source archive, checksums, and GitHub provenance attestation.
The project does not publish to PyPI, GitHub Packages, Docker, or GHCR.

## Repository settings

Require the `CI` checks on `main`, require pull requests, enable private
vulnerability reporting, retain read-only default workflow-token permissions,
and require immutable action references. These settings live on GitHub and are
reviewed separately from repository code.
