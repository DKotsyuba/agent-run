# Releasing agent-run

The canonical version is `workspace.package.version` in `Cargo.toml`. Releases
use annotated `vX.Y.Z` tags. The tag workflow refuses a tag that does not match
the workspace version.

Release automation publishes one native target: qualified macOS Apple silicon
(`aarch64-apple-darwin`). Linux x86-64 remains a visible non-blocking CI
validation lane; its release and qualification are deferred.

Publication and installation are separate boundaries. A green publication
workflow does not prove a production cutover, and a local `release install` or
`update` never publishes to GitHub. Linux stays labelled unqualified until a
hosted full-suite run and sealed-release smoke exist for it.

## Prepare

From a clean checkout of the accepted commit:

```bash
cargo xtask check
cargo xtask archive --verify
cargo build --locked --release --package agent-run --bin agent-run
node --test scripts/check-desktop-transport.cjs
```

Run the release gates on macOS arm64. The non-blocking Linux validation lane
runs the workspace and sealed-build checks independently, but its result does
not gate or add an artifact to the qualified native release.

For adapter or supervisor changes, also run a candidate through a supported
real engine in an isolated home. Verify the answer proof, transcript, process
cleanup, and continuation. Never modify an installed sealed release for this
smoke.

## Build and verify locally

```bash
release_root="$(mktemp -d)"
cargo xtask release build-native --output "$release_root" --version 0.15.0
cargo xtask release verify --release "$release_root/releases/0.15.0"
cargo xtask archive --revision HEAD \
  --output "$release_root/agent-run-0.15.0-source.tar" --verify
```

`build-native` seals the current host binaries. The sealed directory contains the
runtime, `bin/agent-run-deploy` (the native xtask deployment entry point), external
quota scripts, metadata, `SHA256SUMS`, and the `COMPLETE` marker. It contains no
interpreter, virtual environment, or package installation. Cross-platform
release archives are built on their matching hosted runners, not by relabelling
one host's binary.

## Publish

1. Update `Cargo.toml`, `Cargo.lock`, and `CHANGELOG.md` together.
2. Merge only after the `CI` workflow passes.
3. Create and push an annotated tag from that accepted commit:

   ```bash
   git tag -a v0.15.0 -m "agent-run 0.15.0"
   git push origin v0.15.0
   ```

After CI accepts the commit, the tagged `Release` workflow runs the macOS
workspace checks and builds and verifies the sealed macOS artifact. The macOS
gate job additionally runs Desktop transport and source-archive gates once. The workflow then generates one `SHA256SUMS`, attests
the listed macOS and source artifacts plus `install.sh`, and publishes a GitHub Release only after
every required job succeeds. A failed run leaves no public partial release.
Tags are immutable; corrections ship as a new patch version.

## Install or update a sealed runtime

The primary public entry point is the [one-line installer](../README.md#install).
It uses curl or wget, verifies the archive checksum, rejects unsafe tar members,
and runs the sealed native helper. It is available in releases after 0.13.3.
`install.sh --version X.Y.Z` pins a published release; omitting the version uses
the latest stable GitHub Release. macOS Apple silicon is the only accepted host.

The standalone helper takes `install --release DIR --version X.Y.Z --prefix DIR
--home DIR --bin-dir DIR`. It serializes installation, holds the broker startup
lock and a SQLite writer reservation, validates current provider configuration,
requires an identical store schema, and checks active work before cutover.
Explicit config/schema migrations remain separate operations. No services are
stopped automatically; an occupied installation fails without killing processes.
It copies into the permanent releases directory before calling the journalled
deployer, then creates a managed launcher with the selected default home.
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
private deployment journal.
The raw xtask recovery/deploy commands require operator-established quiescence;
the extra broker and SQLite locks belong to the standalone `install` wrapper.

Service control and post-switch API/MCP validation remain explicit operator
steps:

1. Stop admission and verify no active agents or birth-verified writers remain.
2. Run the xtask install/update command against the verified release.
3. Restart the API, capacity, and delivery launchd jobs on macOS.
4. Verify release metadata, database integrity, `agent-run doctor`, API
   `ping`/`tools`, and MCP `initialize`/`tools/list`.
5. Run one provider-free broker fixture before admitting real work.

On failure, preserve `<home>/standalone/deploy.json`, the reported backup, and
the command output. Recover with the journal-aware xtask command; never point an
older binary at a database already migrated by a newer schema.

## Distribution boundary

GitHub Releases is the only public distribution channel. Each release carries:

- `agent-run-X.Y.Z-aarch64-apple-darwin.tar.gz`;
- `agent-run-X.Y.Z-source.tar`;
- `SHA256SUMS`;
- GitHub provenance for the checksummed subjects.

The project does not publish package-manager artifacts, containers, or runtime
dependency bundles.
