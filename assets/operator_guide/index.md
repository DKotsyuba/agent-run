# agent-run Operator Guide

This is the map. Run `agent-run doc <topic>` (CLI) or call the MCP tool
`doc` with `{"topic": "<topic>"}` to read one topic in full. Omit the
topic for this index.

| Topic | Covers |
|---|---|
| completion | MCP start response, automatic bound-chat delivery, compact notice format, result retrieval |
| config | `<home>/config.toml`: source of truth, fail-closed validation, hot reload, safe-edit discipline |
| skills | revisioned-profile `skills = [...]`, plugin ownership, snapshots |
| mcp-servers | `[mcp.<name>]` declarations and revisioned-profile selection |
| plugins | `harnesses.<id>.plugins`, harness load mechanics, canonical role skill declarations |
| models | provider/model catalog, cached quota standing, delegation guide, historical schema-1 rosters |
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
