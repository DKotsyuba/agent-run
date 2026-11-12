# Releasing agent-run

The canonical version is `workspace.package.version` in `Cargo.toml`. Releases
use annotated `vX.Y.Z` tags. The tag workflow refuses a tag that does not match
the workspace version.

The release workflow builds macOS Apple silicon (`aarch64-apple-darwin`) and
Linux x86-64 (`x86_64-unknown-linux-gnu`) on native runners. Both producers and
the source gate must succeed before publication. Linux is built on Ubuntu 22.04
against glibc. Historical releases may contain only the macOS archive.

Publication and installation are separate boundaries. A green publication
workflow does not prove a production cutover, and a local `release install` or
`update` never publishes to GitHub. Linux remains a candidate in the family
declaration until hosted release evidence exists; workflow configuration alone
does not certify a platform or a host deployment.

## Prepare

From a clean checkout of the accepted commit:

```bash
cargo xtask prepare
cargo xtask check
cargo deny --offline --locked check
cargo xtask archive --verify
cargo build --locked --release --package agent-run --bin agent-run
node --test scripts/check-desktop-transport.cjs scripts/check-codegraph-probe.cjs
```

Use short, canonical and non-overlapping roots for process temporary files and
test fixtures. The Codex shared-asset store must not sit inside its writable
`TMPDIR`. Hosted gates use sibling `agent-run-tmp` and `agent-run-fixtures`
directories beneath `RUNNER_TEMP`, and set `TMPDIR`, `TMP`, `TEMP` and
`AGENT_RUN_TEST_TMP` accordingly. On Linux a newly created protected `/run`
directory is used for `AGENT_RUN_TEST_SOCKET_TMP`, because operator-socket tests
also require protected ancestors without ACLs. The native no-symlink checks
remain mandatory. `cargo xtask check` additionally points test `TMPDIR`, `TMP`
and `TEMP` at a fresh private `0700` directory on tmpfs (`/dev/shm`) when it can
execute files and has at least 1 GiB free, and removes it afterwards; elsewhere,
or with `XTASK_TEST_TMPFS=0`, tests keep the `TMPDIR` set here. Release
acceptance should run the gate with `RUST_TEST_THREADS=3`, which the gate passes
to cargo-nextest as `--test-threads`.

Run the release gates on both macOS arm64 and Linux x86-64. Each native
producer builds and smoke-tests its own payload; a failed Linux job blocks
publication just as a failed macOS job does.

CI separates the source-check cache from the explicit native package workload.
Rust-cache additionally keys by toolchain, manifests, lockfile and build inputs.
Each workload has one trusted main producer; PRs and release jobs only restore.
Main also warms the verified cargo-deny executable cache. Advisory database
refresh remains an explicit network step, followed by the offline policy gate.
Cache hits never skip source gates or exact-payload verification. Hosted speed
changes require comparable job timings rather than a cache-hit flag alone.

For adapter or supervisor changes, also run a candidate through a supported
real engine in an isolated home. Verify the answer proof, transcript, process
cleanup, and continuation. Never modify an installed sealed release for this
smoke.

## Build and verify locally

```bash
release_root="$(mktemp -d)"
version="$(awk -F '"' '/^version = / { print $2; exit }' Cargo.toml)"
target="$(rustc -vV | sed -n 's/^host: //p')"
cargo xtask package --target "$target" --output "$release_root" --version "$version"
cargo xtask release verify --release "$release_root/releases/$version"
cargo xtask archive --revision HEAD \
  --output "$release_root/agent-run-$version-source.tar" --verify
```

`build-native` seals the current host binaries. The sealed directory contains the
runtime, the separate `bin/agent-run-tui` observer, `bin/agent-run-deploy` (the native xtask deployment entry point),
systemd examples, metadata, `SHA256SUMS`, and the `COMPLETE` marker. Quota collector assets belong to Agent Quota.
It contains no
interpreter, virtual environment, or package installation. `release verify` and the
installer treat that directory as a closed inventory of regular files: every file
except `SHA256SUMS` and `COMPLETE` must be listed with a matching hash, and an
unlisted executable or asset (including a stray `bin/agent-run-tui`), a symlink or a
special file is refused. Legacy releases that never listed a TUI stay valid. This
is local candidate integrity, not an attestation of the downloaded archive. Cross-platform
release archives are built on their matching hosted runners, not by relabelling
one host's binary.

## Publish

1. Run `cargo xtask release prepare X.Y.Z` to inspect the version plan. Explicit
   `--apply` updates only the local Cargo version, lockfile, changelog and
   the source registration descriptor's product-version mirror.
2. Merge only after the `CI` workflow passes.
3. Record acceptance against the exact clean source, then create the annotated
   tag with `cargo xtask release tag X.Y.Z --accepted-commit FULL_SHA
   --evidence FILE`. Tag creation is local; push is a separate owner action.

   The publisher accepts only a source commit reachable from `origin/main`.
   It never substitutes the moving branch tip for the accepted full SHA.

The release source gate and both native payload builds run independently, and
publication needs all three. Installed binaries are built once per attempt; the
publisher compiles verification tooling only and receives those exact bytes.
Observed checks and an exact-payload smoke bind the archive digest, source,
target, compiler and actual workflow/run/attempt. Local evidence has no
fabricated GitHub run or qualified-host identity.

The publisher validates the closed inventory and evidence, verifies GitHub
attestations for the expected repository/workflow/source, creates a complete
draft, downloads and verifies it, publishes, and verifies the public release
again. A failure can leave a draft for manual reconciliation; it never silently
deletes that evidence or overwrites a published version. An existing publication
is a no-op only when identity, attempt and bytes match exactly.

Repository-wide serialization uses `queue: max` with cancellation disabled:
up to 100 pending runs are retained; excess new arrivals are rejected. See
[GitHub's concurrency contract](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-syntax#concurrency).
Tags and published bytes are immutable; changed bytes require a new version.

## Observe an exact publication

```bash
scripts/wait-release.sh --repo DKotsyuba/agent-run --tag vX.Y.Z \
  --commit FULL_40_CHARACTER_SHA --workflow release.yml \
  --run-id RUN_ID --attempt ATTEMPT \
  --result-file /absolute/private/release-result.json --timeout 1800 --interval 15
```

The thin shell wrapper calls Rust `release wait`. Without an explicit run,
bounded discovery refuses ambiguous candidates. It checks repository/tag/run
identity, the published stable release, manifest and every artifact's size/hash,
then rechecks identity. A monotonic deadline bounds API calls, downloads and
polling. It writes one final JSON to stdout and a create-new mode-0600 result;
progress goes to stderr, including changes to the source, Linux, macOS and
publication jobs for the selected attempt. Existing result paths are never overwritten.
To use a trusted prebuilt observer without rebuilding, set
`AGENT_RUN_RELEASE_WATCH_XTASK` to its absolute path before running the script.

The event explicitly reports `installed=false`, `agent_awakened=false` and
provenance verification `not_performed`: integrity observation is not permission
to execute or install. Optional trusted `--notify-exec /absolute/adapter` and
repeated `--notify-arg VALUE` pass the saved event through stdin. Only an exact
event-id acknowledgement counts as adapter receipt, not model consumption.
Notifier failure is separate from the publication fact; no private agent relay
or fabricated agent identity is used.

Exit codes: 0 verified/acknowledged; 1 workflow failure; 2 deadline; 3 arguments;
4 access/preflight; 5 identity/integrity/result file; 6 notifier failure; 130
controlled cancellation. SIGKILL or disk failure cannot guarantee a saved result.

## Install or update a sealed runtime

The primary public entry point is the [one-line installer](../README.md#install).
It uses gh, curl or wget for downloads and requires trusted gh and jq. It checks
the exact annotated tag, external manifest, inventory, archive size/hash and
GitHub attestation before extracting or executing the helper. Archive paths,
normalized duplicates, links, special entries, modes, entry count and unpacked
bytes are bounded and rejected before execution. There is no silent trust
downgrade. Earlier releases retain their own version-bound bootstraps.
`install.sh --version X.Y.Z` pins a published release; omitting the version uses
the latest stable GitHub Release. The selected release must contain an archive
for the current supported macOS arm64 or Linux x86-64 host.
An older, saved macOS-only bootstrap rejects the expanded inventory; download
the current release's installer before updating. The new bootstrap can still
verify an older macOS-only manifest on macOS.

The standalone helper takes `install --release DIR --version X.Y.Z --prefix DIR
--home DIR --bin-dir DIR`. It serializes installation, holds the broker startup
lock and a SQLite writer reservation, validates current provider configuration,
requires an identical store schema, and checks active work before cutover.
Explicit config/schema migrations remain separate operations. No services are
stopped automatically; an occupied installation fails without killing processes.
It copies into the permanent releases directory before calling the journalled
deployer, then creates managed `agent-run` and `agent-run-tui` launchers with
the selected default home. Both executable names use the same release version.
All bundled launcher paths are checked before switching the current release;
a foreign TUI command is never overwritten. Legacy releases without the TUI
remain installable.
An explicit `AGENT_RUN_HOME` or CLI `--home` still overrides that default.
Foreign launchers, conflicting bytes at an existing version and pending
deployment recovery are refused. An identical selected release is a no-op.

For offline operator recovery, the same helper exposes the existing `release`
commands below without Cargo (replace `cargo xtask` with its absolute path).
Use explicit paths for every deployment operation:

```bash
release=/absolute/path/to/sealed-release
prefix="$HOME/.agent-run/standalone"
home="$HOME/.agent-run"

cargo xtask release install --release "$release" --prefix "$prefix" --home "$home"
cargo xtask release update --release "$release" --prefix "$prefix" --home "$home"
cargo xtask release roll-forward --prefix "$prefix" --home "$home"
cargo xtask release rollback --prefix "$prefix" --home "$home"
```

`--release` is required for install and update; roll-forward and rollback use
the retained deployment journal and therefore accept only `--prefix` and
`--home`. The deployer verifies the manifest before switching `current`, checks
schema compatibility, backs up state, and records a
private deployment journal. Rollback restores each retained file through a
private same-directory temporary, syncs it, atomically renames it and syncs the
directory. A copy failure preserves the previous file. This is atomic per file,
not across the database and configuration together; recovery still uses the
journal and operator-established quiescence. SQLite WAL sidecars are preserved.
The raw xtask recovery/deploy commands require operator-established quiescence;
the extra broker and SQLite locks belong to the standalone `install` wrapper.

Service control and post-switch API/MCP validation remain explicit operator
steps:

1. Stop admission and verify no active agents or birth-verified writers remain.
2. Run the xtask install/update command against the verified release.
3. Restart the API and delivery launchd jobs on macOS. An existing capacity
   collector job needs a separate migration to Agent Quota before it can be
   relied on with this CLI; this procedure does not migrate or remove that job.
4. Verify release metadata, database integrity, `agent-run doctor`, API
   `ping`/`tools`, and the applicable MCP lifecycle (legacy initialize or modern
   `server/discover` and per-request `tools/list`).
5. Run one provider-free broker fixture before admitting real work.

On failure, preserve `<home>/standalone/deploy.json`, the reported backup, and
the command output. Recover with the journal-aware xtask command; never point an
older binary at a database already migrated by a newer schema.

## Distribution boundary

GitHub Releases is the only public distribution channel. Each release carries:

- `agent-run-X.Y.Z-aarch64-apple-darwin.tar.gz`;
- `agent-run-X.Y.Z-x86_64-unknown-linux-gnu.tar.gz`;
- `agent-run-X.Y.Z-source.tar`;
- `install.sh`;
- `release-manifest.json`;
- `acceptance.json`;
- `SHA256SUMS`;
- GitHub provenance for the checksummed subjects.

The project does not publish package-manager artifacts, containers, or runtime
dependency bundles.

The external manifest hashes payload, source, installer and acceptance evidence;
it does not hash itself or SHA256SUMS. SHA256SUMS includes the manifest without
hashing itself. Historical internal metadata/SHA256SUMS/COMPLETE seals remain
readable: COMPLETE is a packaging-completion marker, never authorship or proof
that the bundle was installed. Real publication, installation and host delivery
must be qualified separately; fixture tests do not certify them.
