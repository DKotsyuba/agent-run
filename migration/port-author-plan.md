# Migration plan and acceptance gates

> **Historical execution plan.** Rust became the primary implementation in
> 0.12.0. Statements such as “still required,” old paths, and interim counts
> describe the port while it was being authored. Use [status.md](status.md) and
> [qualification-scope.md](evidence/qualification-scope.md) for current claims.

Baseline: v0.11.15, immutable commit `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`.

The delivery goal is complete replacement of the Python application, not a Python launcher with a Rust executable name. External engine CLIs and the signed Desktop host remain external integrations. This document distinguishes the work already represented by source code from work still blocking that goal.

## 1. Freeze contracts and build an executable reference corpus

**Partially done.** Pin the baseline, map modules, copy schema 16, document the public tool table, lifecycle, proof format and important provider wire contracts. The source map and eleven-tool inventory are included.

**Still required:** fetch the entire baseline source and original test suite. Inventory every public CLI command/flag, tool input/output schema, diagnostic field, lifecycle event, configuration error, native launch flag and generated file. Commit representative fixtures with secrets removed. A selective source review is not a complete inventory.

Exit gate: every original behavior and test is linked to a Rust implementation/test or an explicitly owner-approved removal. No feature is marked complete solely because a module with its name exists.

## 2. Stand up the native foundation

**Source present:** Cargo package, typed IDs/status transitions, strict config, bounded errors, profiles and policy evidence, descriptor-anchored file operations.

**Still required:** resolve and review Cargo.lock, validate the minimum toolchain, run formatting/compiler/Clippy checks, address diagnostics without weakening validations, audit dependency advisories/licenses, and test Linux/macOS builds. The archive does not claim this gate has passed.

## 3. Preserve durable state and answer compatibility

**Source present:** schema-16 SQLite state with WAL/FULL synchronization, replay checks, transactional limits/admission, messages, commands, attempts, lineage, delivery outbox and online backups. Answer v2 payload/marker/proof and legacy terminal frame reads are implemented.

**Still required:** port numbered migrations 1–15 and their pre-migration backup/refusal behavior; test every historical database snapshot and request JSON shape; ensure immutable identity/replay comparisons survive configuration changes; test old sidecars, invalid UTF-8, missing proofs, symlink swaps, concurrent writes, kill-at-fsync and recovery. Port Python-native resume identity conversion or keep release blocked.

Exit gate: copied historical data produces equivalent read results and continuations; upgrade/rollback tests prove original rows and answer hashes are preserved. Matching `user_version` alone is insufficient.

## 4. Reproduce process ownership and detached lifecycle

**Source present:** broker-backed launch, separate executable supervisor, READY before materialization, ownership tokens, command consumption, native interruption, process-group/known-descendant cleanup and evidence-based terminal classification. No service-owned execution-age deadline is introduced.

**Still required:** test slow/broken bootstrap pipes, client EOF, process exit before group discovery, permissions-denied observations, reused PIDs, orphan descendants, signal failures, broker SIGKILL, supervisor SIGKILL and database failures during each transition. Review the difference between standard Rust process spawning and upstream's `posix_spawn`-first path. Confirm cancellation cannot signal an unrelated/reused process.

## 5. Complete native adapter parity

**Source present:** Codex app-server model discovery, thread/turn operations, grant echo checks, streaming, steer and interrupt; Claude/GLM stream sessions; Qwen sandboxed CLI invocation; generated runtime homes and selected assets.

**Still required:** compare native wire transcripts and generated config against baseline fixtures; implement any missing auth/keychain, raw stream, plugin asset/recovery and diagnostics paths; validate managed Projects grants on macOS/Linux; decide the read-only Claude shell restriction; confirm exact Qwen credential and sandbox behavior. Test native continuations across restarts and configuration edits.

Exit gate: each engine passes offline differential fixtures and authorized live smoke tests. Unknown native protocol changes fail closed; no implicit downgrade to unsandboxed execution or a new context on failed resume.

## 6. Complete transport and completion-delivery parity

**Source present:** one dispatcher, CLI, bounded same-user socket API, official Rust MCP SDK, per-call broker connections, leased completion outbox, fixed notices and local Desktop/Claude transport implementations. Ten local Node-bridge contract tests actually ran successfully; these do not validate the Rust relay listener or a real Desktop host.

**Still required:** compile/test SDK protocol negotiation, cancellation and EOF; compare all CLI outputs/exit codes; port full legacy delivery evidence projection, exact original notice template and unbound-delivery expiration behavior; test lease loss/duplicates/retries/ambiguous acknowledgement and real host reconnects. Retain the signed-Node shim only to the extent the host requires it; do not represent it as pure Rust.

## 7. Complete quota and routing parity

**Source present:** exact nullable identities, freshness/expiry, reset-cycle burn, route validation, worst governing window, absolute account/lane/runtime weights, shared-pool alias collapse, reset credits, Codex app-server collection, explicit Claude OAuth and single-account Codexbar.

**Still required:** OmniRoute current-cache reader, multi-account Codexbar mapping, original native source fallbacks, source-specific model-compatible topology and exact public projections. Test failed sibling scopes, stale members, corrupt present windows, backend-account aliases, future clocks, already-reset windows, sample retention and query-cap fairness.

Exit gate: source failures cannot become successful empty capacity; an unknown governing window cannot yield a healthy route; aliases cannot invent capacity by being counted twice.

## 8. Package, migrate and release

**Source present:** documentation, strict check script, Linux/macOS CI definition, source archive and manifest. CI intentionally refuses release validation without a committed reviewed Cargo.lock.

**Still required:** all earlier gates, reproducible release artifacts for supported architectures, checksum/signing/attestation policy, installer/current-symlink switching, active-session-safe retention and rollback. Do not replace a working Python release before those gates pass.

## Suggested order for the next implementation session

First obtain a real Rust build result and fix compiler/test failures, not more speculative features. Then run and extend the offline fixture suite, import the entire upstream regression corpus, close historical DB/resume/auth omissions, finish capacity/delivery parity, and only then perform authorized real-engine smoke tests. Update `MIGRATION_STATUS.md` with command logs and fixture references at each gate.

## Definition of done

One supported native Rust application with no Python runtime fallback; a reviewed locked dependency graph; Linux/macOS CI green; complete original feature/test mapping; historical data and continuation compatibility; verified safe process ownership and answer proof behavior; all four engine integrations and both completion hosts exercised; and a source ZIP plus reproducible release procedure. At the time this archive was authored, this definition of done was **not met**.
