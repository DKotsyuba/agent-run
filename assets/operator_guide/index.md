# agent-run Operator Guide

This is the map. Run `agent-run doc <topic>` (CLI) or call the MCP tool
`doc` with `{"topic": "<topic>"}` to read one topic in full. Omit the
topic for this index.

| Topic | Covers |
|---|---|
| completion | MCP start response, automatic bound-chat delivery, compact notice format, result retrieval, legacy pool archive |
| idle-ping | free supervisor checks, optional bounded recovery, real waiting and risk notices |
| config | `<home>/config.toml`: source of truth, fail-closed validation, hot reload, safe-edit discipline |
| skills | revisioned-profile `skills = [...]`, plugin ownership, snapshots |
| mcp-servers | `[mcp.<name>]` declarations and revisioned-profile selection |
| plugins | `harnesses.<id>.plugins`, harness load mechanics, canonical role skill declarations |
| models | provider catalog and delegation guide; quota lives in Agent Quota |
| releases | sealed release build/switch/retention under standalone/releases |
| migrations | PRAGMA user_version, numbered SQL deltas, pre-backup, refusal cases |
| storage | shared managed assets, `storage status`/`compact`/`recover`, collection rules |
| troubleshoot | doctor first, failure_kind vocabulary, quota honesty, orphan check |

Read `config` first. Valid changes are loaded at request boundaries and by the
broker's 60-second hash check; invalid revisions leave the previous config
active.

MCP pages have hard text/row budgets. Request at most 20 agents per list page;
an oversized page is refused whole, with no skipped rows or fabricated cursor.
Formatting failure after accepted work preserves its receipt: keep the original
agent/notification and request IDs, and do not repeat a write to repair the text.
The CLI and socket keep their structured results and full document retrieval.

Read-only observers can query `capabilities` over the operator socket (CLI
`agent-run capabilities`; it is not an orchestrator MCP tool) for
schema-version-1 product facts, transport limits and actual read-method schemas.
Source revision is null without verified build provenance. Retain `ping`/`tools`
fallback for older brokers; discovery starts no provider collection or work.

When an answer is too large for the MCP text budget, `answer` returns the
existing verified artifact's path, size, SHA-256 and execution sequence. A file
link follows the host's normal permissions. No extra answer copy is created;
the existing history retention also governs this file. If the path is not
accessible, retrieve the answer with the operator CLI `agent-run answer-chunk`
(socket `answer_chunk`; not an orchestrator MCP tool since 0.32.0).

For complete large answers, `answer_chunk` (CLI `answer-chunk`) requires the
current `agent_id`, positive `sequence`, verified `expected_sha256` and UTF-8
`offset_bytes`. Continue with returned `next_offset` and the same fences; the
default budget is 64 KiB, maximum 128 KiB. Reconstruct on demand and verify the
final hash. Resume, hash changes, invalid offsets and corrupt proofs reject;
EOF returns empty content and `complete:true`. Exact legacy artifact chunks
include their terminal frame for hash fidelity. Paths come only from broker
records. Socket answers exceeding the complete 1 MiB JSON frame return metadata
with `content:null`, `inline_complete:false`; the CLI inline cutoff stays 1 MiB.
MCP exposes these two new reads in full `structuredContent`. See `docs/api.md`
in the source distribution for the complete argument/result/error contract.
