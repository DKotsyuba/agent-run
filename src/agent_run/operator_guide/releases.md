# releases

Public versions are built from an annotated `vX.Y.Z` tag by GitHub Actions.
The workflow verifies that the tag matches `pyproject.toml`, runs the full
suite, builds and checks wheel/sdist, performs a clean install smoke, records
`SHA256SUMS`, creates provenance attestations, and publishes the files in a
GitHub Release. Maintainer procedure: `docs/releasing.md` in the source tree.

The sealed layout below is an optional local deployment strategy. It is not
created by pip/pipx and is separate from the public distribution channel.

Sealed local releases live under `~/.agent-run/standalone/releases/<sha>`, one
immutable directory per shipped commit.

## Build steps

1. `git archive` the target commit — the release contains exactly that
   commit's tree, nothing dirty and nothing extra.
2. Build a wheel from the archive with `--no-deps --no-build-isolation`.
3. Create a fresh venv inside the release directory using the system
   `python3.11`, then install the wheel into it.
4. Write `SHA256SUMS` covering every file in the release directory except
   `COMPLETE` and `SHA256SUMS` itself.
5. Run an isolated-home smoke test against the new release: `init`,
   `doctor`, `models`, `limits`, and an MCP `tools/list` count check.
6. Write `COMPLETE` containing exactly `complete\n` — its presence is the
   signal that the release directory is safe to switch to. A release
   directory without `COMPLETE` must never be switched to, even if every
   file looks present.

## Atomic switch

Switch the "current" pointer with a temp symlink plus `os.replace`, and
rehearse the swap direction (old→new→old→new) before trusting it in a
script — `os.replace` on a symlink is atomic, but only if the code path
that builds and replaces the temp link is exercised both ways first.

## Retention

Keep the current release and the immediately previous one only; older
sealed releases are removed once a newer one is confirmed switched-to.

## After a schema migration

Any long-lived process still running out of an older release directory
refuses to open a store that a newer release has already migrated — it
does not silently misread it. Such processes must be explicitly respawned
(from the new release) after a migration ships; a schema bump is not
transparent to processes that started before it.
