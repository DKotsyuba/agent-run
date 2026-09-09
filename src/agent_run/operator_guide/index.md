# agent-run Operator Guide

This is the map. Run `agent-run doc <topic>` (CLI) or call the MCP tool
`doc` with `{"topic": "<topic>"}` to read one topic in full. Omit the
topic for this index.

| Topic | Covers |
|---|---|
| config | ~/.agent-run/config.toml: source of truth, fail-closed, priority multiplier/order, safe-edit discipline |
| skills | `skills = [...]` per runtime, plugin ownership, symlinks, rematerialize |
| mcp-servers | `[mcp.<name>]` declarations and per-runtime attachment |
| plugins | `runtimes.<rt>.plugins`, per-runtime load mechanics, fail-closed refusal |
| models | static rosters and Qwen OmniRoute route aliases |
| releases | sealed release build/switch/retention under standalone/releases |
| migrations | PRAGMA user_version, numbered SQL deltas, pre-backup, refusal cases |
| troubleshoot | doctor first, failure_kind vocabulary, limits honesty, orphan check |

Read `config` and `service` first: almost every maintenance task reduces to
"edit config.toml safely, then get the affected service to pick it up."
