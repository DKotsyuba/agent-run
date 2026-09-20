# releases

Public versions come from annotated `vX.Y.Z` tags. The tag must match the
workspace version in `Cargo.toml`.

Release automation produces one sealed macOS Apple-silicon artifact. Linux
x86-64 remains an unqualified, non-blocking validation target; its release is
deferred. Each release also includes a verified source archive, `SHA256SUMS`,
and provenance.

## Local release check

```bash
cargo xtask check
cargo xtask qualify --release
cargo xtask evidence verify
cargo xtask archive --verify
cargo build --locked --release --package agent-run --bin agent-run
node --test scripts/check-desktop-transport.cjs

release_root="$(mktemp -d)"
cargo xtask release build-native --output "$release_root" --version X.Y.Z
cargo xtask release verify --release "$release_root/releases/X.Y.Z"
```

A valid sealed directory contains `bin/agent-run`, `metadata.json`,
`SHA256SUMS`, and `COMPLETE`. Never switch `current` to a directory that fails
`cargo xtask release verify`.

## Update and rollback

The xtask deploy path verifies the release, requires writer quiescence, backs up
SQLite and configuration, records `standalone/deploy.json`, migrates state, and
switches `current` atomically. It does not control the platform service manager
or perform the final API/MCP smoke.

After an update, restart the launchd jobs on macOS. Verify version, database
integrity, doctor, API
`ping`/`tools`, MCP `initialize`/`tools/list`, and one provider-free broker run.
Preserve the deployment journal and backup after any failure. Use the
journal-aware recovery command; never manually point an older binary at a newer
schema.

See `docs/releasing.md` in the source archive for the maintainer runbook.
