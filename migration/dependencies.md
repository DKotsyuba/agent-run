# Migration dependency record

> Historical pointer. Dependency truth is `Cargo.toml`, the workspace crate
> manifests, `Cargo.lock`, and the pinned `rust-toolchain.toml` on `main`.

The shipped service is a native Rust binary and does not require Python, a
virtual environment, pip, uv, or project JavaScript to run. External engine
CLIs may have their own runtimes; those are engine dependencies, not an
agent-run implementation dependency.

Builds use the committed lockfile. Current release commands and host
requirements are documented in [docs/releasing.md](../docs/releasing.md).
