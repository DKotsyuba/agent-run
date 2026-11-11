# Managed native control fixtures

Captured on dobi on 2026-11-11 as `agent-mcp` with control-only probes: no
model input reached a provider. Claude Code 2.1.311 ran with Run's managed
argv (`--tools ""`, the exact `--allowedTools`, and the discovered
`--disallowedTools` complement), a throwaway `CLAUDE_CONFIG_DIR`,
`ANTHROPIC_BASE_URL` set to a refused loopback port and only the `agent-pool`
MCP server from the stack's Pool configuration. `initialize` and `mcp_status`
were bare control requests; the one `system/init` frame was read after a user
frame whose API request could not leave the host. codex-cli 0.167.0 ran
`app-server` against a copy of the failed attempt's Run-generated
`config.toml` without credentials: `initialize`, `config/read`, `thread/start`
(no turn) and `mcpServerStatus/list`.

Normalized: temporary homes and work directories are replaced with the failed
attempts' runtime paths, and request, session, thread and message ids are
fixed. No credential appears in any capture.

`claude-2.1.311-mcp-status.json`, `claude-2.1.311-system-init.json`,
`claude-2.1.311-input-lifecycle.json`, `codex-0.167.0-thread-start.json` and
`codex-0.167.0-mcp-server-status.json` were recaptured on 2026-11-11 by the
ignored live probes `stream::tests::live_managed_claude_qualifies_before_model_input`
and `codex::tests::live_managed_codex_qualifies_before_any_turn` (agent-run-core),
run as `agent-mcp` against the failed attempts ag-20261111-033955-98bc33fa67
(Claude) and ag-20261111-033955-077019ad6d (Codex). They run Run's own
selection, discovery, preflight and verification code against the installed
harnesses. The `agent_run_worker` entries are real: the worker MCP of this
build ran in managed mode with a dummy capability whose broker endpoint does
not exist, so its catalog registration failed closed while `tools/list` served
exactly `health_reply` and `finish`. Claude's user frame went to a refused
loopback port; Codex never received a turn.

- `claude-2.1.311-input-lifecycle.json`: the `command_lifecycle` frames
  (`queued`, `started`) Claude emits for an input frame carrying a `uuid`,
  before `system/init`.
- `codex-0.167.0-thread-start.json`: Run's managed thread/start parameters
  (developer instructions elided) and codex's reply (thread reduced to id,
  environments and status): `environments: []` yields
  `runtimeWorkspaceRoots: []`.

- `codex-0.167.0-run-config.toml`: the failed attempt's Run-generated
  `config.toml` (environment variable names only), the source of the pinned
  hook declarations.
- `codex-0.167.0-config-read.json`: `config/read` of that configuration.
- `codex-0.167.0-config-read-managed.json`: the same configuration with the
  managed default-on features (`api_key_cyber_access_programs`,
  `api_key_model_discovery`, `auth_elicitation`, `mentions_v2`, `view_image`)
  written `false`, as Run now generates it.
