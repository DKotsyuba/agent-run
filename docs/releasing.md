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
node --test scripts/check-desktop-transport.cjs scripts/check-codegraph-probe.cjs
```

Run the release gates on macOS arm64. The non-blocking Linux validation lane
runs the workspace and sealed-build checks independently, but its result does
not gate or add an artifact to the qualified native release.

CI and release jobs share Rust dependency caches on the same runner OS and
architecture. Rust-cache also keys them by the toolchain, manifests, lockfile
and build environment. Pull requests only restore caches; main and tagged
release runs may save them. Project binaries and sealed release directories
are rebuilt and verified on every run, and all existing checks remain enabled.
The first cache miss still performs a full dependency build; hosted savings
must be measured from subsequent cache-hit runs.

For adapter or supervisor changes, also run a candidate through a supported
real engine in an isolated home. Verify the answer proof, transcript, process
cleanup, and continuation. Never modify an installed sealed release for this
smoke.

## Build and verify locally

```bash
release_root="$(mktemp -d)"
cargo xtask release build-native --output "$release_root" --version 0.19.1
cargo xtask release verify --release "$release_root/releases/0.19.1"
cargo xtask archive --revision HEAD \
  --output "$release_root/agent-run-0.19.1-source.tar" --verify
```

`build-native` seals the current host binaries. The sealed directory contains the
runtime, the separate `bin/agent-run-tui` observer, `bin/agent-run-deploy` (the native xtask deployment entry point), external
quota scripts, metadata, `SHA256SUMS`, and the `COMPLETE` marker. It contains no
interpreter, virtual environment, or package installation. Cross-platform
release archives are built on their matching hosted runners, not by relabelling
one host's binary.

## Publish

1. Update `Cargo.toml`, `Cargo.lock`, and `CHANGELOG.md` together.
2. Merge only after the `CI` workflow passes.
3. Create and push an annotated tag from that accepted commit:

   ```bash
   git tag -a v0.19.1 -m "agent-run 0.19.1"
   git push origin v0.19.1
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

## Local Linux validation environment

`docker/linux-check.Dockerfile` is a validation environment, not a distribution
artifact. Its Rust 1.98.1 Debian 12 base is pinned by digest; it installs rustfmt,
clippy, Node, jq and bubblewrap before any offline gate. Build without sending a
checkout (or host credentials) as Docker build context:

```bash
mkdir -p target/linux-docker/config
export DOCKER_CONFIG="$PWD/target/linux-docker/config"
docker build -t agent-run-linux-check:local - < docker/linux-check.Dockerfile
```

Run `scripts/linux-check.sh` in a clean, writable checkout inside the image,
as its non-root `checker` user, with `--init`. Keep `/cache/cargo` and
`/cache/target` writable and task-scoped. The script prints the exact source
commit and environment, runs `cargo fetch --locked` online, then executes the
existing offline gates with two build jobs. Record the image ID, source commit,
commands and complete exit results outside the source tree. Never mount an
operator home, Docker configuration, accounts or credentials into this image.

Default Docker may deny the user and mount namespaces required by bubblewrap.
That refusal is capability evidence, not positive shared-tree protection proof;
do not silently skip guard tests or relax the container security profile.
For an explicitly authorized namespace test, `docker/linux-guard-seccomp.json`
retains Moby's default `SCMP_ACT_ERRNO` policy and 60 baseline entries. It adds
`clone` with other namespace flags excluded, `unshare` only for user/mount
namespaces, and `mount`, `umount2`, `pivot_root`. It adds no capabilities and does
not enable `setns` or `clone3`. Its source is
[Moby's default profile](https://github.com/moby/profiles/blob/main/seccomp/default.json),
SHA-256 `6416b47770785a41ac59073cdc77d9fe98517df2799dc83ef207e622de3053f6`;
the derived profile is `bccb49ff5abad5381567318c5c25a5c990e32729fb57b593e5f87211cfc8a540`.
The profile does not override host AppArmor or kernel user-namespace restrictions.

Prepare a committed source bundle to avoid mounting a worktree whose Git metadata
points outside the container. This recipe exposes only that read-only bundle and
isolated writable caches; it never mounts a host home:

```bash
git bundle create target/linux-docker/source.bundle HEAD
docker run --rm --init \
  --security-opt "seccomp=$PWD/docker/linux-guard-seccomp.json" \
  --mount "type=bind,src=$PWD/target/linux-docker/source.bundle,dst=/tmp/source.bundle,readonly" \
  --mount type=volume,src=agent-run-linux-check-cargo,dst=/cache/cargo \
  --mount type=volume,src=agent-run-linux-check-target,dst=/cache/target \
  agent-run-linux-check:local bash -c \
  'git clone /tmp/source.bundle /work/source && cd /work/source && bash scripts/linux-check.sh'
```

Use fresh cache volume names to measure a cold dependency fetch. Retained caches
accelerate later runs; source and release directories are rebuilt independently.
A supported Linux host must prove the guard and lifecycle tests before release
qualification. Native arm64 containers exercise the Linux kernel but do not
qualify GNU x86-64. An amd64 container on an arm64 host is emulated: compilation
or seal verification there remains preparation evidence only. The first planned
Linux release baseline is native x86-64 Debian 12 with glibc 2.36; ARM64 Linux
artifacts are outside the release scope.

The shell installer maps Linux x86-64 to `x86_64-unknown-linux-gnu` only with
`--allow-unqualified --version X.Y.Z`. Default Linux installation still refuses
before downloading. No Linux archive is currently published; the opt-in prepares
the verified archive path for isolated fixture validation, not a supported public
installation. Local candidates use the sealed `agent-run-deploy install` helper
with disposable home, prefix and bin paths. Its existing checksum/seal checks,
ownership checks, schema compatibility, broker/service locks and SQLite writer
reservation apply unchanged. GNU x86-64 candidates require Debian 12/glibc 2.36
or a separately proven compatible host. Linux ARM64 and musl distribution are
outside scope.
