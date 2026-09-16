# Migration status — release gate NOT satisfied

Baseline: `DKotsyuba/agent-run` v0.11.15, commit `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`.

## What this artifact is

A substantial **native Rust development implementation**, with source, configuration, SQL, an optional signed-Node delivery bridge, tests and migration documentation. It is not a Python wrapper. It is also **not a completed, compiled or regression-verified migration**. The full repository was not cloned into the authoring environment; selected source modules and contracts were retrieved through the GitHub connection. Claims of complete source inspection or behavioral equivalence would be unsupported.

`implemented` below means code exists, not that it has passed Rust compilation or a real-provider integration test. `test authored` means a Rust test is included, not executed. The pre-workspace snapshot is retained at `evidence/port-snapshot/REPORT.md`; current checks are recorded with the workspace change.

## Area-by-area status

| Area | Source implementation | Outstanding release requirement |
|---|---|---|
| Domain and identifiers | Typed IDs, exact state transitions, strict requests | Run Rust tests and compare original edge-case fixtures |
| Configuration | Strict TOML v1, packaged adapter aliases, account/auth declarations, native reserved roots | Full original invalid-config corpus and frozen identity compatibility |
| Roles and policy | Legacy/canonical roles, grants, explicit enforcement evidence | Original role/snapshot canonical hash compatibility |
| Managed files | Descriptor-anchored no-follow reads, atomic synced writes, explicit auth links | Race/fault injection review on Linux and macOS |
| SQLite | Schema 16, transactions, outboxes, transcripts, lineage, backup | Numbered migrations 1–15 not ported; original migration snapshots not tested |
| Durable launch | Separate supervisor, bounded READY, persistent ownership, cancellation | Process/crash/permission-denied/PID-reuse tests on both OSes |
| Codex | App-server initialization, roster, grants/echo checks, turn events, steer/interrupt/resume | Current real CLI, managed Projects policy and protocol differential tests |
| Claude/GLM | Stream-JSON process/session, result classification, generated settings, explicit auth | Real CLI tests, prompt-only output-schema compatibility and detailed usage parity |
| Qwen | Sandboxed headless stream, error-only result detection, native resume selector | Real sandbox and macOS Git bootstrap tests |
| Generated skills/plugins/hooks | Selected asset copying, hook trust/rendering, private homes, own snapshot format | Full upstream asset/plugin compatibility and extra-file tamper cases |
| CLI / Unix socket | Shared dispatcher, private socket, bounded frames, broker-backed starts | Exact CLI flags/output/exit-code parity and all original protocol tests |
| MCP | Official `rmcp` SDK stdio server and independent broker calls | Compile against pinned SDK, negotiation/cancellation integration tests |
| Completion delivery | Atomic leases/evidence, Codex relay v1–v3, existing Claude UDS session | Complete legacy evidence shape, exact original template and live host tests |
| Capacity | Freshness, reset-cycle burn, weights, physical-pool collapse, reset credits | Full source-specific topology and projection output parity |
| Capacity sources | Codex app-server; explicit Claude OAuth usage; single-account Codexbar | OmniRoute, labelled Codexbar and original native fallback collectors missing |
| Authentication | Declared environment, explicit file bridge, native login, scoped account homes | macOS Keychain fallbacks and all original credential-home conventions |
| Continuation | New Rust runs retain verified native context and one-child lineage | Python-created runs cannot be resumed by this implementation |
| Packaging | Cargo manifest, build/test scripts, CI definition and source ZIP | Generate and audit Cargo.lock, compile, tests, signed release and update/rollback workflow |

## Known differences and unsupported paths

1. Historical schema upgrades are refused rather than attempted without the upstream migration chain. A schema-16 layout is included; sharing an actively used production database across implementations is not supported or tested.
2. Native resume of Python-created runs is explicitly rejected. Matching schema version is not evidence that Python identity snapshots, launch homes or request replay semantics are fully compatible.
3. OmniRoute collection and labelled-account Codexbar identity mapping are not implemented. Unsupported source paths report a fixed error, not successful zero-quota collection.
4. macOS Keychain credential fallback is not implemented. Use configured environment/file/native login authentication in an isolated evaluation home.
5. One optional JavaScript file remains for the signed Codex Desktop host transport. It is an external host requirement, not a Python runtime dependency; the package is not literally all-Rust source.
6. Read-only Claude/GLM runs omit Bash. Runtime/tool preferences are not promoted to unverified OS isolation guarantees. This changes some upstream capabilities intentionally.
7. Normalized messages are persisted, but the full upstream raw-stream spool and external-payload reference contract are not reproduced. Usage/TTFT/API timing fields and detailed diagnostic views are not all equivalent.
8. Inline answers are capped at 128 KiB, lower than upstream, to stay within the bounded JSON-RPC frame even under escaping. Verification remains separately bounded to 16 MiB.
9. Delivery evidence uses a smaller safe Rust shape. Historical evidence of the Python shape is not fully projected. Notice wording and source-specific capacity output are not byte-for-byte equivalent.
10. Process creation uses the Rust standard process API with a minimal `setsid` pre-exec step; it does not reproduce upstream's explicit `posix_spawn(setsid=True)`-first implementation. Process identity and signals require further OS-specific race review.
12. The workspace now commits the reviewed `Cargo.lock`, pins Rust 1.98.1, and provides `cargo xtask check`; migration parity claims above remain independently gated.

## Blocking acceptance gates

No migration-complete claim until Cargo resolves a reviewed lockfile; formatting, compilation, Clippy and tests pass on Linux and macOS; the original test corpus is mapped and ported; omitted features above are implemented or explicitly approved as removals; old database/answer/continuation fixtures pass; and authorized live smoke tests validate each installed engine and notification host.

The code must be reviewed as a development change, not installed over a working release on the strength of a ZIP file alone.

## Structured output clarification

The pinned Python Claude adapter implements `output_schema` as a prompt instruction, not a native JSON-schema validator (`adapter.py`, lines 252–259). The Rust stream adapter follows that limited behavior. Neither a schema-shaped prompt nor a successful answer proof establishes JSON-schema conformance. This port does not claim a stronger validation guarantee.
