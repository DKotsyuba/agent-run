# Supported-engine live canary — 20 September 2026

Candidate: `e8d5b7d6011680f4ff79913d0c25c8d6f92a81c4`.

Binary SHA-256:
`7f959c3c85370d899acbf3b8e3e55bd81a1fdd684f39895e8bf2367dacb77151`.

The candidate ran as an isolated Rust broker with a private home and socket.
Production configuration, service pointer, and broker were not changed. The
operator started the isolated broker from an ordinary Terminal so the already
authorized native provider credentials remained available.

## Start results

| Runtime | Model | Agent | Result | Exact answer | SHA-256 |
|---|---|---|---|---|---|
| Codex | `gpt-5.6-luna` | `ag-20260920-185726-297246d513` | `succeeded` | `CODEX_CANARY_OK` | `61a9308cf700b3ce80c6fead4b5fcec6462e8e56526988d68b3c743669e1975c` |
| Claude | `sonnet` | `ag-20260920-185706-d83261c725` | `succeeded` | `CLAUDE_CANARY_OK` | `4b647199756d38e9ed53c60f8edb5dc63a12062f3dfb16a20fdeef895dad56b6` |
| GLM | `glm-5.3-flash` | `ag-20260920-185702-265438c7d2` | `succeeded` | `GLM_CANARY_OK` | `97f04f1256dd5827f67f09a393e3b135749d26d1b10fd470c3144a5394639951` |

Each answer was read back through the Rust CLI with proof version 2, exact byte
size, and a matching digest. Provider transcripts contained the submitted task
and provider-produced answer. Every child process group and descendant was
confirmed gone after completion.

## Resume results

Each continuation preserved its parent runtime, model, native session identity,
and inherited policy, then reached durable success.

| Runtime | Parent agent | Child agent | Exact answer | SHA-256 |
|---|---|---|---|---|
| Codex | `ag-20260920-185726-297246d513` | `ag-20260920-190102-a73f7b820f` | `CODEX_RESUME_OK` | `94dc2b37cc65dda5430cccf3eab7e131ddabc413d1d79a8ea88d69281fa9f8b1` |
| Claude | `ag-20260920-185706-d83261c725` | `ag-20260920-190108-88dd8a933b` | `CLAUDE_RESUME_OK` | `c63cc25d7cf7c1b4e52dd8704a768c0c5ba75930ac00e75e5712b2297ea32c14` |
| GLM | `ag-20260920-185702-265438c7d2` | `ag-20260920-190109-3870a6eb50` | `GLM_RESUME_OK` | `f8b61a1fa45f32f06bbd1e4db787b8f64fca5daa6114edf8996d2bd649c88036` |

All continuation answers were independently read through the Rust CLI with
proof version 2 and matching digests. Cleanup recorded confirmed process-group
and descendant absence for every continuation.

This evidence closes the supported-engine live portions of T58 and T59 for
Codex, Claude, and GLM. It does not establish Desktop-host delivery (T71), Linux
qualification (T82), or a production cutover.

The original operational checkpoint, including failed sandbox/launchd attempts,
is [live-canary-checkpoint-2026-09-20.md](../live-canary-checkpoint-2026-09-20.md).
