# A15 — Supported operating systems, architectures and minimum versions

Status: **Proposed — requires an owner decision.** This ADR records the evidence
and a recommendation; it does not by itself declare a support commitment.

## What the plan requires

The migration plan (`migration/rust-migration-plan.md:1181`) states that Mac and
Linux are both mandatory, and that **each architecture and minimum version must
be confirmed by runner or live evidence**. That evidentiary bar, not the
existence of code, is what this ADR has to satisfy.

## Evidence in the code

Platform-conditional code is concentrated in six files and is heavily skewed:

| Gate | Occurrences |
| --- | ---: |
| `cfg(target_os = "macos")` | 31 |
| `cfg(target_os = "linux")` | 7 |
| `cfg(unix)` | 1 |
| `cfg(target_os = "windows")` | 0 |

By file: `crates/agent-run-platform/src/process.rs` (11),
`crates/agent-run/tests/launchd.rs` (6),
`crates/agent-run-platform/src/launch.rs` (6),
`crates/agent-run-platform/src/keychain.rs` (5),
`crates/agent-run/tests/cli_parity.rs` (3),
`crates/agent-run-platform/src/fs.rs` (2),
`crates/agent-run/src/launchd.rs` (1),
`crates/agent-run-platform/tests/process_identity.rs` (1).

Windows is not supported and is not a candidate: the supervisor depends on Unix
process groups, signals, Unix-domain sockets and Unix file modes throughout.

## Evidence from this migration's test runs

Every suite run, binary smoke and concurrency probe performed during the port ran
on **macOS (Darwin 27.0.0, arm64)**. No run on any Linux host or any other
architecture was performed at any point.

Two behavioral differences between the platforms were observed and are recorded:

- `Identity::zombie` (`crates/agent-run-platform/src/process.rs`) can only ever be
  set on Linux: on macOS `proc_pidinfo` answers `ESRCH` for an unreaped child, so
  the zombie state is unreachable there. Both paths reach the same `Dead` verdict,
  so the observable outcome matches, but the code path differs by platform.
- The process-liveness probe uses a macOS `sysctl` route (`KERN_PROC_ALL` /
  `KERN_PROC_PID`, `proc_listallpids`) rather than reading `/bin/ps`; the Linux
  path is separate and has never been executed in this migration.

## Recommendation

Declare **macOS on arm64 as the only supported target for the initial cutover**,
and describe Linux as *present in the source but unqualified*.

The reasoning is the plan's own bar. Linux code exists, but "exists" is not
"supported": seven conditional branches — including the process-identity and
liveness paths that the whole supervision model rests on — have never been
executed on a Linux host. Declaring support without a single run on the platform
would be exactly the kind of unevidenced claim this migration has otherwise
refused throughout.

If Linux support is required for the cutover, the work is concrete and should be
scheduled rather than assumed: run the full suite plus the binary smoke on a
Linux runner, confirm the process-identity and liveness paths against real
processes there, and record the run in `migration/evidence/`. Only then should
this ADR be changed to accept Linux.

## Consequence

Until an owner decision is recorded here:

- `xtask qualify` (board task M55) should qualify macOS/arm64 only, and should
  fail rather than silently pass if asked to qualify a platform with no evidence.
- The release and handoff artifacts (M56) must name the qualified platform
  explicitly, so that an operator cannot infer support that was never tested.

## Open question for the owner

Which of these is the intended target for the first cutover?

1. **macOS/arm64 only** — recommended; matches the evidence available today.
2. **macOS/arm64 and macOS/x86_64** — requires one qualification run on Intel.
3. **macOS and Linux** — requires a Linux runner and qualification of the
   process-identity and liveness paths before it can honestly be claimed.
