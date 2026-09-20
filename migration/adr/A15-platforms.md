# A15 — Supported platforms and build targets

Status: accepted for 0.12.x.

## Decision

The release system builds two native target families:

| Target | Build/CI intent | Support claim |
|---|---|---|
| `aarch64-apple-darwin` | full checks, qualification, sealed artifact | supported |
| `x86_64-unknown-linux-gnu` | compile/test/release validation | pending qualification |

Linux is not supported merely because it compiles or produces an artifact. The
first hosted Linux full-suite run and sealed-release smoke must be committed as
evidence before T82 and the support claim can close. Desktop delivery remains a
separate T71 gate and is not implied by either build target.

Platform-neutral CLI, MCP, socket API, store, adapters, and supervisor behavior
must fail explicitly when a platform-only integration such as launchd or
Keychain is unavailable.

## Evidence boundary

Current macOS evidence is summarized in [migration status](../status.md) and the
[sealed release smoke](../evidence/release-smoke-2026-09-20.md). The living
[qualification map](../evidence/qualification-scope.md) keeps T82 partial until
the hosted Linux evidence exists.

The recorded full suite ran on macOS/arm64; the dated verification evidence
records it as passed with 0 failed. No corresponding hosted Linux run is
recorded.
