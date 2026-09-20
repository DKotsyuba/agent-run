# agent-run Operator Guide

This is the map. Run `agent-run doc <topic>` (CLI) or call the MCP tool
`doc` with `{"topic": "<topic>"}` to read one topic in full. Omit the
topic for this index.

| Topic | Covers |
|---|---|
| completion | MCP start response, automatic bound-chat delivery, compact notice format, result retrieval |
| config | `<home>/config.toml`: source of truth, fail-closed validation, hot reload, safe-edit discipline |
| skills | `skills = [...]` per runtime, plugin ownership, symlinks, rematerialize |
| mcp-servers | `[mcp.<name>]` declarations and per-runtime attachment |
| plugins | `runtimes.<rt>.plugins`, per-runtime load mechanics, fail-closed refusal |
| models | static model rosters and live availability |
| releases | sealed release build/switch/retention under standalone/releases |
| migrations | PRAGMA user_version, numbered SQL deltas, pre-backup, refusal cases |
| troubleshoot | doctor first, failure_kind vocabulary, limits honesty, orphan check |

Read `config` first. Valid changes are loaded at request boundaries and by the
broker's 60-second hash check; invalid revisions leave the previous config
active.
