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

## Selecting servers

A declared `[mcp.<name>]` server does nothing on its own. Canonical revisioned
profiles select servers with `mcp = ["name"]` in their front matter. Runtime
`mcp = [...]` lists remain readable only for compatibility profiles; canonical
and compatibility selection cannot be mixed. A server selected nowhere is
inert, and an unknown selected name fails closed at config load.

## Shared background backends

Schema 2 also accepts `[services.<id>]` with an absolute foreground `command`,
literal `args`, absolute `cwd`, explicit `env_from` and a bounded `readiness`
command. These are broker-owned backends, separate from per-agent MCP clients.
The broker warms all configured services before a harness starts, retains their
leases while agents are active, and stops them after `idle_timeout_seconds`
(default 1800) counted from the last agent's completion. Native MCP clients
still need their normal configuration to connect to the shared backend; a
service declaration does not add tools to a role or multiplex stdio sessions.

Probe commands receive `AGENT_RUN_SERVICE_PID`, `AGENT_RUN_SERVICE_ID` and
`AGENT_RUN_SERVICE_GENERATION`. Their zero exit must mean the intended backend
is ready; output is discarded. Process ownership is separately checked by
native PID/start-token evidence. `doctor` reports service health without starting
backends. The native archive includes an external `services/codegraph-probe.cjs`
example qualified for CodeGraph 1.6.0, using that version's private daemon entry.
Keep CodeGraph's ordinary `serve --mcp --path <project>` client in the agent role.

## Codex workspace and destructive guard

`runtimes.codex.workspace_roots` optionally gives write-capable Codex roles
operator-approved project trees instead of only their assigned workdir. The
workdir must resolve below at least one of those roots, a write role admitted
under one root receives the full configured Projects root set, external read roots
still fail closed, and
read-only roles remain read-only with their exact roots. Write roles without a
network grant use the generated native `Projects` permission profile as their
app-server default; agent-run omits the conflicting legacy `sandbox` request and
verifies `activePermissionProfile.id` before starting a model turn. The
`runtimeWorkspaceRoots` echo stays scoped to the assigned workdir, while the
effective `sandbox.writableRoots` must contain every configured root.
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

Codex, Claude, and GLM children receive only the MCP servers selected by the
effective revisioned profile or compatibility runtime list.
