# Family standard adoption

agent-run is a member of the Agent MCP product family and aligns with the
family standard **1.0.0-rc.2** (a proposed baseline, not a certificate of
compliance). This page records what the repository declares, what is
verifiably adopted, and which requirements remain open. The machine-readable
declaration lives in [`family.toml`](../family.toml); the managed adoption
files and their pinned digests live in `.family/manifest.json`, verified by
`cargo xtask family verify` as part of `cargo xtask check`.

The adoption was performed by manual review against a pinned revision of the
family template (see `.family/baseline/NOTICE` for the license and provenance
of the exported baseline bytes). Nothing in the build or CI requires access
to that template.

## Product profile

| Axis | Value | Meaning for agent-run |
|---|---|---|
| Process | `resident` | The broker (`agent-run api serve`) owns execution, supervision, and delivery; CLI and MCP front ends submit through it |
| State | `local` | SQLite store under the owned product home; no second source of truth |
| Transports | `stdio` MCP plus the separately declared Unix-socket JSON-RPC API and shared CLI | One dispatcher, one public tool table |
| External programs | `tools` | Codex, Claude Code, and GLM engine CLIs are configured, version-reported external executables |
| Host adapter | yes | Embedded Desktop frontend (`assets/desktop-transport.cjs`) executed by host-supplied Node, plus host hook binding |

The release payload is one main binary plus the `agent-run-tui` observer and
the `agent-run-deploy` installation helper, all sealed from the same
workspace version. The Desktop bridge is embedded in the binary; it is not
`scripts/services/codegraph-probe.cjs`, the separately shipped CodeGraph
integration probe. Node is used for these declared product integrations, not
general repository automation. CI invokes their fixture checks through
`scripts/check-desktop-transport.cjs` and `scripts/check-codegraph-probe.cjs`;
this records configured checks, not a green run of the adoption revision.
Explicit Desktop bridge/version reflection in `doctor` remains open.

## Machine-contract authority

The authoritative public tool contract is **schema-first**:
[`assets/tools.json`](../assets/tools.json) is the single authority, parsed
once into the typed registry in `crates/agent-run-domain/src/tools.rs`, which
owns lookup, argument metadata, and public error declarations. No transport
keeps an independent schema. `tests/fixtures/baseline/tools.json` is the
frozen Python-port oracle. Current discovery snapshots are
`schemas/tools.json` and `schemas/worker-tools.json`, generated from the same
domain registries used by live MCP through `cargo xtask contract export`.
`cargo xtask contract check` compares complete descriptions, schemas and
annotations and is included in the unified gate. The private worker authority
is `assets/worker_tools.json`; it never imports the operator registry.

## Compatibility: observed versus not verified

| Item | Status |
|---|---|
| `aarch64-apple-darwin` | Historically qualified through the published 0.19.x native releases; each release re-runs the native gate |
| Linux x86-64 | Unqualified, non-blocking CI validation lane only |
| MCP protocol revision | Legacy initialize flow (`2025-11-25`) via rmcp 3.4.0; the `2026-07-28` discovery revision is **not verified** |
| Hosts (Codex CLI, Claude Code, GLM, Desktop relay v1–v4) | Integration-tested through fixtures and transport tests; **no formal family host-matrix qualification** — `qualified_hosts` stays empty until one is run |
| Compiler/SDK baseline | `rust-macos-2026-09-candidate1`: Rust 1.98.1 pinned, rmcp `=3.4.0`, MiniJinja `=2.24.0` |

## Security boundary summary

The full policy lives in [SECURITY.md](../SECURITY.md). In brief: the trusted
subject is the same-uid operator reaching the owned mode-0700 product home
through the CLI, MCP stdio, or the peer-checked Unix socket; engine CLIs are
configured explicitly and never auto-downloaded; effects are spawning
configured engines, writing inside declared per-agent roots, and delivering
bound completion notices — nothing else. `actor_role`/orchestrator fields are
attribution, not authentication. Filesystem permissions do not defend against
a hostile process with the same UID; that limit is documented, not denied.

## Adopted in this repository

- Rust-only automation: repository gates and release/deployment tooling run
  through `cargo xtask` (`check`, `archive`, `release`, `install`, `family`).
- Workspace baseline: edition 2024, resolver 3, `publish = false` inherited
  by every member, committed `Cargo.lock`, pinned toolchain, workspace lints
  with `undocumented_unsafe_blocks = deny`.
- One quality gate, `cargo xtask check`: fmt, strict offline/locked clippy
  and tests over all features, a supported default-feature compile check,
  rustdoc with `-D warnings`, and adoption-metadata verification.
- Environmental test hygiene: no test mutates its own process environment.
  Environment-dependent cases (`HOME`, `CODEX_HOME`, `CODEX_QUEUE_BIN`,
  `CLAUDE_CODE_OAUTH_TOKEN`, the Claude session registry override, GC tuning
  knobs) rerun in isolated child test processes that receive their values
  through the child's command environment, with bounded windows and
  kill-and-reap on timeout or parent panic; logger-level cases use the same
  isolation. Assertions are preserved and independent parent tests remain
  concurrent. Process-group cleanup signals only a verified owned,
  unreaped leader; it does not promise to signal groups after leader exit.
- Adoption verification checks manifest structure, pinned managed-file
  digests, and agreement on the declared standard version. It does not
  certify normative compliance, host qualification, or release provenance.

## Open requirements (truthful, not waived)

- **MCP compatibility:** both server identities use the Cargo product version
  and short English instructions. Tools carry reviewed effect annotations;
  admission replay is conditional on an unchanged request key and arguments.
  Unknown tools and malformed protocol parameters use MCP protocol errors.
  The presentation contract and intentional pre-1.0 minor delta are described
  in [MCP presentation](mcp-presentation.md). These local changes require the
  next release to be 0.20; they do not qualify hosts or publish a release.
- **Release/delivery stage (planned):** no external `release-manifest.json`
  binding commit/run/attempt, no draft-verify-publish sequence, no post-
  publication verification, no release waiter; the archive checks prove
  path/link safety but not duplicate-member, entry-count, unpacked-size, or
  mode constraints.
- **Release provenance:** the existing release workflow emits GitHub
  attestations. Independent attestation verification is a separate check;
  the current bootstrap installer checks the archive checksum before executing
  the downloaded installation helper; that helper verifies the sealed internal
  manifest during installation. Neither automatically verifies attestations
  before the helper executes. Emission alone does not
  establish the declared `github-attestation` trust profile end to end.
- **Bounds evidence:** MCP has separate per-tool text, row and exact-content
  budgets and a bounded private writer. Oversized pages fail as a whole.
  Broader upstream and child-process limits remain separate requirements.
- **Result validation:** MCP validates critical dynamic broker fields and
  projects explicit typed response views before rendering. This does not
  claim a separate JSON Schema validator for all CLI/socket result variants.
- **Observability:** component logs are UTC-daily and expire after 30 idle
  days (see [history retention](history-retention.md)); a per-file size cap
  is not yet evidenced.
- **Versioning:** the 0.x history already mixes feature and patch bumps, so
  no blanket SemVer pass is claimed; future releases follow the recorded
  policy.
- **Governance (host-side facts, unmodified):** the `main` branch requires
  the `supported-macos-arm64` and `Rust release contract` status checks
  (strict, `enforce_admins=false`); required reviews and repository rulesets
  are not configured. These remain open governance gaps, not enforcement
  claims.

Product contracts that predate the standard — the installer's sealed
directory layout, config/state schema, stable agent IDs, account boundaries,
process-identity checks, retention, and notification delivery — are preserved
by decision; the standard permits existing richer layouts during migration.
