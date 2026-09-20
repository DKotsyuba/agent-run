# Rust release recovery

Use the sealed-release deployment commands documented in
[docs/releasing.md](../docs/releasing.md). Inspect the retained deployment
journal before acting; do not infer a missing pointer or backup.

1. Stop or wait for active agents and other workflow writers.
2. Verify the named release and database backup.
3. Run the appropriate `cargo xtask release recover`, `roll-forward`, or
   `rollback` command with explicit `--prefix`, `--home`, and release paths.
4. Never start an older release against a newer unsupported schema. Restore the
   matching verified database backup with the matching release instead.
5. Restart the service, then verify API ping, MCP discovery, doctor, one broker
   run, answer proof, cleanup, and socket lifecycle.
6. Preserve the deployment journal and record the result in a dated copy of
   [post-cutover-report.md](post-cutover-report.md).

The frozen Python branch is source-history fallback, not an automatic runtime
rollback path. Production rollback uses the retained installed release and its
matching database evidence.
