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
attempts' runtime paths, and request, session and message ids are fixed. No
credential appears in any capture. The supervisor-owned `agent_run_worker` MCP
needs per-attempt credentials and could not start offline; its entries
(`finish`, `health_reply`) are synthetic in `claude-2.1.311-mcp-status.json`,
`claude-2.1.311-system-init.json` and `codex-0.167.0-mcp-server-status.json`.

- `codex-0.167.0-run-config.toml`: the failed attempt's Run-generated
  `config.toml` (environment variable names only), the source of the pinned
  hook declarations.
- `codex-0.167.0-config-read.json`: `config/read` of that configuration.
- `codex-0.167.0-config-read-managed.json`: the same configuration with the
  managed default-on features (`api_key_cyber_access_programs`,
  `api_key_model_discovery`, `auth_elicitation`, `mentions_v2`, `view_image`)
  written `false`, as Run now generates it.
