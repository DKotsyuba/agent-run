# Rust release acceptance checklist

Use this checklist for the exact candidate being released. Record command
output and artifact digests in a new dated evidence file; do not edit old
evidence to describe a new candidate.

## Repository gates

- [ ] `cargo xtask check`
- [ ] `cargo xtask qualify --release`
- [ ] `cargo xtask evidence verify`
- [ ] `cargo xtask archive --verify`
- [ ] `cargo build --locked --release --package agent-run --bin agent-run`
- [ ] `node --test scripts/check-desktop-transport.cjs`
- [ ] sealed release passes `cargo xtask release verify`
- [ ] documentation links resolve and no current page points into removed
      Python source

## Platform claim

- [ ] macOS arm64 artifact is built and smoked on macOS arm64
- [ ] the release contains no Linux archive; Linux validation is non-blocking
      and explicitly unqualified
- [ ] no unsupported architecture is published under a supported target name

## Runtime evidence

- [ ] private-home API and MCP smoke passes
- [ ] fixture-backed start reaches a durable terminal result with matching
      answer proof and confirmed process cleanup
- [ ] supported real-engine start and resume canaries pass for Codex, Claude,
      and GLM, or the release records why prior evidence remains applicable
- [ ] real Desktop delivery smoke is attached before claiming T71
- [ ] `agent-run doctor` has no unexpected warning or error

## Production cutover

- [ ] installed Python release and database backup are retained for rollback
- [ ] active agents and workflow writers are quiescent
- [ ] candidate manifest and `COMPLETE` marker verify before pointer switching
- [ ] deployment journal records old/new release and backup
- [ ] service restart, API ping, MCP discovery, one broker run, and answer proof
      pass after switching
- [ ] rollback command and schema compatibility were checked before cutover
- [ ] [post-cutover report](post-cutover-report.md) is completed

Passing repository checks does not itself authorize or prove a production
cutover.
