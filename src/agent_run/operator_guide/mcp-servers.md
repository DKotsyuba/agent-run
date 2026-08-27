# mcp-servers

Declare an MCP server once, under `[mcp.<name>]`:

```toml
[mcp.agent_lsp]
transport = "stdio"
command = "/abs/path/to/agent-lsp"
args = ["--foo", "bar"]
env_from = ["SOME_ENV_VAR_NAME"]
```

`env_from` names environment variables to pass through by name only — never
inline secret values in config.toml itself.

## Attaching to runtimes

A declared `[mcp.<name>]` server does nothing on its own. Attach it to a
runtime by adding its name to that runtime's `mcp = [...]` list. A server
declared but attached to no runtime is inert; a name listed in `mcp =
[...]` but not declared under `[mcp.<name>]` fails closed at config load.

## Current state

Children (agents started via claude and codex) see `agent_lsp` and
`codegraph` today, when those runtimes' config lists them.

The opencode engine currently cannot expose MCP tools to the model at all —
this is a known upstream engine gap, not a config mistake. See
`docs/upstream-opencode-v2-prompt-mcp.md` for the filed report. Do not spend
time debugging an opencode child that "doesn't see" an attached MCP server;
confirm the gap in that doc first.
