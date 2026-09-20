# A15 — Supported platforms and build targets

Status: accepted for 0.12.x.

## Decision

The 0.12.0 release publishes one native target. Linux remains in CI only as a
non-blocking validation lane after the owner deferred its release and
qualification:

| Target | Build/CI intent | Support claim |
|---|---|---|
| `aarch64-apple-darwin` | full checks, qualification, sealed artifact | supported |
| `x86_64-unknown-linux-gnu` | non-blocking compile/test/sealed-build validation | release and qualification deferred |

Linux is not supported merely because validation compiles or produces an
internal sealed build. Version 0.12.0 publishes no Linux archive. A future
decision to release Linux still requires a hosted full-suite run and
sealed-release smoke as evidence before T82 and the support claim can close.
Desktop delivery remains a separate T71 gate and is not implied by either
build target.

Platform-neutral CLI, MCP, socket API, store, adapters, and supervisor behavior
must fail explicitly when a platform-only integration such as launchd or
Keychain is unavailable.

## Evidence boundary

Current macOS evidence is summarized in [migration status](../status.md) and the
[sealed release smoke](../evidence/release-smoke-2026-09-20.md). The living
[qualification map](../evidence/qualification-scope.md) keeps T82 partial while
Linux release and qualification remain deferred.

The recorded full suite ran on macOS/arm64; the dated verification evidence
records it as passed with 0 failed. No corresponding hosted Linux run is
recorded.
