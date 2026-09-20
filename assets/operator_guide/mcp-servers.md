# mcp-servers

Declare an MCP server once, under `[mcp.<name>]`:

```toml
[mcp.agent_lsp]
transport = "stdio"
command = "/abs/path/to/agent-lsp"
args = ["--foo", "bar"]
env_from = ["SOME_ENV_VAR_NAME"]
approval_mode = "approve"
```

`env_from` names environment variables to pass through by name only — never
inline secret values in config.toml itself. `approval_mode` is optional and one
of `auto`, `prompt`, `writes`, or `approve`; it defaults to `auto`. Codex renders
the value as `default_tools_approval_mode`. For every attached server set to
`approve`, agent-run also renders a narrow trusted PermissionRequest hook that
can approve only that server's MCP namespace. It never approves Bash or file
operations. Other adapters ignore the Codex-specific approval hint.

## Attaching to runtimes

A declared `[mcp.<name>]` server does nothing on its own. Attach it to a
runtime by adding its name to that runtime's `mcp = [...]` list. A server
declared but attached to no runtime is inert; a name listed in `mcp =
[...]` but not declared under `[mcp.<name>]` fails closed at config load.

## Codex workspace and destructive guard

`runtimes.codex.workspace_root` optionally gives write-capable Codex roles one
operator-approved project tree instead of only their assigned workdir. The
workdir must resolve below that root, external read roots still fail closed, and
read-only roles remain read-only with their exact roots. Write roles without a
network grant use the generated native `Projects` permission profile as their
app-server default; agent-run omits the conflicting legacy `sandbox` request and
verifies `activePermissionProfile.id` before starting a model turn. The
`runtimeWorkspaceRoots` echo stays scoped to the assigned workdir, while the
effective `sandbox.writableRoots` must contain the configured project tree.
When `workspace_network = true`, the Projects profile permits shell network and
the generated command policy routes `curl` through the normal approval reviewer.
Read-only and network roles keep their explicit legacy sandbox until Codex
exposes an equivalent scoped named profile for those grants.

DCG needs no agent-run-specific adapter. Declare its absolute installed binary
as a normal Codex runtime hook; install and verify that binary before enabling
the declaration:

```toml
[[runtimes.codex.hooks]]
event = "PreToolUse"
matcher = "^Bash$"
command = ["/absolute/path/to/dcg"]
```

DCG can only deny a pending shell call. It does not approve commands and does
not replace Codex sandboxing or residual auto-review.

## Current state

Codex and Claude children receive only the MCP servers named by their runtime
or revisioned profile. GLM does not expose the same native child-MCP surface;
use Codex or Claude when a task requires MCP tools.
