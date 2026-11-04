# agent-run Operator Guide

This is the map. Run `agent-run doc <topic>` (CLI) or call the MCP tool
`doc` with `{"topic": "<topic>"}` to read one topic in full. Omit the
topic for this index.

| Topic | Covers |
|---|---|
| completion | MCP start response, automatic bound-chat delivery, compact notice format, result retrieval, cooperative pools |
| idle-ping | free supervisor checks, optional bounded recovery, real waiting and risk notices |
| config | `<home>/config.toml`: source of truth, fail-closed validation, hot reload, safe-edit discipline |
| skills | revisioned-profile `skills = [...]`, plugin ownership, snapshots |
| mcp-servers | `[mcp.<name>]` declarations and revisioned-profile selection |
| plugins | `harnesses.<id>.plugins`, harness load mechanics, canonical role skill declarations |
| models | routing guide and quota diagnostics, compatibility catalogs, historical schema-1 rosters |
| releases | sealed release build/switch/retention under standalone/releases |
| migrations | PRAGMA user_version, numbered SQL deltas, pre-backup, refusal cases |
| storage | shared managed assets, `storage status`/`compact`/`recover`, collection rules |
| troubleshoot | doctor first, failure_kind vocabulary, limits honesty, orphan check |

Read `config` first. Valid changes are loaded at request boundaries and by the
broker's 60-second hash check; invalid revisions leave the previous config
active.

MCP pages have hard text/row budgets. Request at most 20 agents per list page;
an oversized page is refused whole, with no skipped rows or fabricated cursor.
Formatting failure after accepted work preserves its receipt: keep the original
agent/notification and request IDs, and do not repeat a write to repair the text.
The CLI and socket keep their structured results and full document retrieval.

Read-only observers can query `capabilities` (CLI `agent-run capabilities`) for
schema-version-1 product facts, transport limits and actual read-method schemas.
Source revision is null without verified build provenance. Retain `ping`/`tools`
fallback for older brokers; discovery starts no provider collection or work.

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
