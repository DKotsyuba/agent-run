# Agent Run backlog

## Claude CLI native background notifications

- [x] Separate owned native task notifications from runner input acknowledgements; preserve coalesced task/steering correlation and contextual-only results.
- [x] Reject unknown or duplicate replays, runner-ID collisions, malformed origins and foreign sessions; retain content-free refusal categories.
Release gate: background-command completion and native continuation through Sonnet and GLM must be verified on the exact candidate before publication.

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
