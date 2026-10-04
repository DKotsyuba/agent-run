#!/bin/bash
# Run from a writable, credential-free checkout in the validation image.
# Fetch once while online; every compilation and gate then uses the locked cache.
set -euo pipefail
export CARGO_BUILD_JOBS=2
rustc --version
cargo --version
uname -m
uname -r
ldd --version | sed -n '1p'
bwrap --version
node --version
jq --version
printf 'SOURCE_COMMIT=%s\n' "$(git rev-parse HEAD)"
test -z "$(git status --porcelain --untracked-files=no)"
test -w "$CARGO_HOME"
test -w "$CARGO_TARGET_DIR"
cargo fetch --locked
cargo xtask check
cargo xtask archive --verify
node --test scripts/check-desktop-transport.cjs scripts/check-codegraph-probe.cjs
cargo build --offline --locked --release --package agent-run --bin agent-run
# Guard tests are part of the gate; unsupported container capabilities fail closed.
cargo test --offline --locked -p agent-run-platform --all-features --lib shared_asset_guard -- --include-ignored --nocapture
