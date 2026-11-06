# Agent Run backlog

## Claude CLI native background notifications

- [x] Separate owned native task notifications from runner input acknowledgements; preserve coalesced task/steering correlation and contextual-only results.
- [x] Reject unknown or duplicate replays, runner-ID collisions, malformed origins and foreign sessions; retain content-free refusal categories.
Release gate: background-command completion and native continuation through Sonnet and GLM must be verified on the exact candidate before publication.

## P1 — Explicit worker finish and event-driven idle

- [ ] Implement the owner's explicit completion contract: only an authenticated internal worker `finish(summary)` completes a run; a native turn end becomes idle.
- [ ] Preserve the same run/native session across pool, steer and approved native/tool completion wakes; eliminate model polling used only to hold a turn open.
- [ ] Keep the finish summary immutable and schema-validated, with idempotent receipts, verified cleanup and restart-safe persistence. Never treat unknown native input, a late result or an unconfirmed cleanup as success.
- [ ] Probe Codex exec-cell survival, qualify Sonnet/GLM background wake and finish, then stage opt-in evaluation and default rollout for new runs. Preserve frozen legacy runs and unlimited execution.

Plan: [explicit-finish-plan.md](explicit-finish-plan.md). This is planned follow-up work, separate from the current Claude CLI interoperability fix. No execution or idle-age timeout is reintroduced.

## Claude background-task notifications — fixed and installed in 0.24.2

- [x] Fix the Claude-family stream interoperability issue observed with Agent Run 0.24.1 and Claude Code 2.1.294. Both Sonnet IDE-experiment arms ended with `runtime_contract_rejected` / `ValidationError` at `engine_execution`, about 1.4 seconds after an internal background-command completion notification.
- [x] Separate validated native internal `task-notification` messages from replay acknowledgements of runner-written user inputs. Native context is tracked separately, including the bounded optional emitter `runId`; unknown runner replay IDs remain refused.
- [x] Preserve task/result/session correlation. Do not accept arbitrary unknown replay IDs, trust notification-like text, or treat an internal notification as completion of the owner's task. Keep unknown/duplicate external replays and spoofed or unowned notifications fail-closed.
- [x] Add fixture regressions for foreground and background commands, completion arriving mid-turn, multiple notifications, genuine steering acknowledgements, malformed/spoofed origins, duplicate IDs and resumed sessions. Verify that a valid background completion lets the original task continue through a correlated final answer and verified cleanup.
- [x] Retain bounded content-free replay-origin refusal and terminal-correlation evidence, without messages, native IDs, arguments, environment values or credentials.
- [x] Qualify real native Sonnet and GLM foreground/background commands through sequence 3 with unchanged native sessions, exact answer hashes and confirmed cleanup. Publish and install 0.24.2 from accepted source `bee1a4e961ce6b372a4ed87e1412103712572ccd`.
- [ ] Rerun the affected benchmark pair as a separately labelled experiment. Preserve the original failed runs and candidates; do not patch or restart active benchmark participants.

Installed qualification on 2026-10-10 verified the exact attested published payload, CLI/TUI and resident 0.24.2, schema 28 integrity, unchanged configuration, fresh API/MCP lifecycle and a provider-free admission-refusal fixture. Real Sonnet/GLM qualification used an isolated local binary from the exact accepted source before publication. Existing doctor findings remain unchanged: two compaction-key false positives and three historical ownership warnings. The new explicit-finish lifecycle above is not part of this repair.

Evidence: an isolated model-free Rust reproduction of the unchanged replay predicate accepts a known runner input and rejects the native background-notification shape. Native histories show the same notification/failure timing in both arms. The original rejected stdout frame was not retained: the interoperability mechanism is reproduced, while attribution of these exact failures remains a high-confidence inference. Detailed private receipts are in the experiment's `run/sonnet-contract-diagnosis.json` and `run/sonnet-replay-reproduction.json`, outside release source; do not publish raw histories or process/session details with this backlog item.

## Synchronous start admission preflight

- [x] Validate that the requested launch is supported before `start` accepts asynchronous work and returns an agent ID. Unsupported harness/profile/model/account/options or permission combinations must return a typed error in that same tool call.
- [x] Reuse the actual adapter launch checks and native grant compiler for preflight and actual execution. Recompile through the same contract after checking frozen inputs; do not maintain separate lists of supported combinations.
- [x] Resolve effective role grants, provider/harness/model options and configuration revision consistently. Changes to launch capabilities must automatically change preflight behavior.
- [x] Preserve authority, immutable snapshot, process ownership and secret boundaries. Preflight must not start a harness/model turn or disclose authorization data.
- [x] Revalidate mutable external state at actual execution; distinguish unsupported input from later races such as disappearing binaries, credentials, capacity or files. Do not promise that preflight eliminates every runtime failure.
- [x] Cover CLI, MCP and socket transports through the shared domain dispatcher. Regression: a writable Codex launch with unsupported external read roots is rejected synchronously before durable asynchronous admission.
- [x] Exercise shared grant/reference checks, supported fake-engine starts and mutable-state races; ensure CLI/MCP/socket use the common admission path. Arbitrary live credential/model/service availability is not a static guarantee.

## Investigate Codex transport failure: ag-20261007-200321-d534c55a58

- [x] Revisit this incident with content-free execution-error diagnostics before attributing it to a network failure. The recorded outcome is `runtime_transport_failed`; the original execution error and exit code were not retained.
- [x] Preserve the existing run and source work. Do not cancel active leads, launch duplicate work, or replace the recoverable session merely to reproduce the incident.
- [x] Initial evidence: the run ended after about 940 seconds with a 25,920-second deadline. Six network warnings concern Codex analytics delivery, and native model/tool activity continued afterward; these warnings do not establish the terminal cause.
- [x] Inspect the shared supervisor error mapping and retain a bounded, content-free cause/stage for future incidents. Separate adapter I/O, malformed native events, persistence failures, and model-service network failures; never persist prompts, answers, credentials or environment values in diagnostics.

## Resolution and limits

Static admission now reuses the actual native grant, adapter executable/reference checks and resolved role policy before rows or capacity reservations are admitted. CLI/MCP/socket regression coverage verifies typed synchronous refusals; pool starts share the same preparation. Real execution repeats checks on frozen inputs and mutable state. Preflight cannot promise future authentication, native model availability, filesystem/service readiness or successful execution.

The recorded Codex incident was investigated against retained native events, supervisor metadata and cleanup evidence. Its original execution error was discarded by the old broker and cannot be reconstructed; analytics-only warnings are not evidence of terminal model-service network failure. No session was replaced or cancelled for this investigation. Both supervisor routes now retain content-free error categories/stages, separating SQLite and JSON failures, with best-effort log/event sinks and unchanged failure/cleanup semantics.

The follow-up [load stability plan](stability-plan.md) covers cache finalization delay, ownership-observer error attribution, pool reconciliation, incident retention and bounded concurrent qualification. These planned changes remain future work; this checklist does not claim they are implemented or deployed.
