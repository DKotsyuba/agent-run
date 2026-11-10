# Provenance

Source: crates.io `tokio-rusqlite` 0.8.0 (MIT, programatik29/tokio-rusqlite,
commit 3aa3388f "Version 0.8"), unmodified apart from `Cargo.toml`, which
depends on `rusqlite` 0.41.1 instead of 0.40.1 and drops the optional feature
passthrough and examples. Upstream's rusqlite 0.41 bump (PR #58) is unreleased;
rusqlite 0.41 is required for `libsqlite3-sys` >= 0.39 (RUSTSEC-2026-0459).
Replace this directory with the crates.io release once it ships.
