#!/bin/sh
# Read-only checks once Cargo.lock has been resolved and reviewed.
set -eu
cd "$(dirname "$0")/.."
if ! command -v cargo >/dev/null 2>&1; then
  printf '%s\n' 'cargo is required; Rust checks were not run.' >&2
  exit 2
fi
if [ ! -f Cargo.lock ]; then
  printf '%s\n' 'Resolve and review Cargo.lock first: cargo generate-lockfile' >&2
  exit 2
fi
cargo fmt --all -- --check
cargo check --locked --all-targets --features test-fixtures
cargo clippy --locked --all-targets --features test-fixtures
cargo test --locked --all-targets --features test-fixtures
