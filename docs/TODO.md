# Agent Run backlog

## Synchronous start admission preflight

- [ ] Validate that the requested launch is supported before `start` accepts asynchronous work and returns an agent ID. Unsupported harness/profile/model/account/options or permission combinations must return a typed error in that same tool call.
- [ ] Use one shared adapter-owned launch-plan compiler for preflight and actual execution. The real launch must consume the validated plan (or recompile it through the same contract after checking its frozen inputs); do not maintain separate lists of supported combinations.
- [ ] Resolve effective role grants, provider/harness/model options and configuration revision consistently. Changes to launch capabilities must automatically change preflight behavior.
- [ ] Preserve authority, immutable snapshot, process ownership and secret boundaries. Preflight must not start a harness/model turn or disclose authorization data.
- [ ] Revalidate mutable external state at actual execution; distinguish unsupported input from later races such as disappearing binaries, credentials, capacity or files. Do not promise that preflight eliminates every runtime failure.
- [ ] Cover CLI, MCP and socket transports through the shared domain dispatcher. Regression: a writable Codex launch with unsupported external read roots is rejected synchronously before durable asynchronous admission.
- [ ] Test that every preflight-accepted supported combination reaches the same launch compiler, and that capability changes cannot make preflight and real launch disagree.

## Investigate Codex transport failure: ag-20261007-200321-d534c55a58

- [ ] Revisit this incident with content-free execution-error diagnostics before attributing it to a network failure. The recorded outcome is `runtime_transport_failed`; the original execution error and exit code were not retained.
- [ ] Preserve the existing run and source work. Do not cancel active leads, launch duplicate work, or replace the recoverable session merely to reproduce the incident.
- [ ] Initial evidence: the run ended after about 940 seconds with a 25,920-second deadline. Six network warnings concern Codex analytics delivery, and native model/tool activity continued afterward; these warnings do not establish the terminal cause.
- [ ] Inspect the shared supervisor error mapping and retain a bounded, content-free cause/stage for future incidents. Separate adapter I/O, malformed native events, persistence failures, and model-service network failures; never persist prompts, answers, credentials or environment values in diagnostics.
