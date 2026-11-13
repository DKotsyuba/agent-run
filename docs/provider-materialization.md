# Provider-aware harness materialization

The new provider path has exactly two harness routes: Codex app-server and
Claude Code stream mode. A provider id is configuration data, not an adapter
kind. `glm` or any other named Claude-compatible provider uses the Claude
Messages route with its configured endpoint, header style, and `native_model`
alias. No new model-name registry is consulted.

At admission, resolve v2 TOML with `ProviderConfig::resolve_catalog(accounts)`
and a canonical role plan, then call
`provider::materialize_selected(config, catalog, provider, model, account,
role, workdir, runtime_home, app_home)`. It validates the account binding and
model restrictions, uses the existing skill/MCP/hook/permission materializer,
and returns a runtime digest. Persist that digest in
`ResolvedLaunchAuthority.assets_sha256`. The sealed
`provider-launch.json` contains the selected native alias, custom connection,
binary, plugin paths, restrictions, and full validated v2 config digest. It
contains no credential bytes.

New canonical roles prepend the shared worker instructions from
`assets/worker_instructions.md` to the profile body. They identify the agent as
an orchestrator's executor, ask for brief self-directed working notes instead of
narration, set shared work rules (scope, checkpoint commits on the worker's own
branch, no invented business rules, verification, cleanup), and request a
compact final report. These are model
instructions, not output filtering or a guarantee of silence. The composed text
is frozen and hashed in `ResolvedRolePlan.prompt`; resume restores it unchanged,
and historical sessions retain their original prompt. Claude/GLM receive it via
`--append-system-prompt`; Codex receives it before the task in `turn/start` input.
Changing the shared asset affects new roles after rebuilding the binary.

For each attempt, call `provider::plan_selected(config, catalog, authority,
account, runtime_home, app_home, host_environment, credential_reader, task,
resume_session)` only after session-owned process cleanup. It verifies the
runtime index, sealed role/assets proof, unchanged v2 settings, original
provider/model/connection/grants, and the newly selected account scope.
Changed live settings cannot replace frozen permissions. The resulting
`ProviderLaunchPlan` contains a child-only `LaunchPlan`, frozen
`native_model`, and validated role; do not serialize or log its environment.
The existing effective-policy evaluator runs at materialization and retry,
so a required model constraint must have actual harness/profile enforcement.

MCP selections and effective tool caps come from the sealed role, including
global catalog selections. Codex writes the cap as native `enabled_tools`.
After managed-service readiness and before each Claude/GLM harness spawn, the
supervisor calls `mcp_catalog::apply_claude_tool_filters` with an ownership
observer. Bounded stdio discovery computes native `--disallowedTools`; it does
not relay subsequent tool calls. Before executing the discovery backend, a
temporary stdin gate waits for its root identity to be journaled. Descendant
snapshots are refreshed, and dead-supervisor recovery cleans recorded discovery
processes before releasing the prepared attempt. No credential or catalog body
is journaled. See [MCP selection and limits](provider-config-v2.md#mcp-selection-and-tool-caps).

Native Codex uses the existing login and app-server invocation. Its selected
`auth.json` link is bound per attempt outside the immutable asset manifest,
so the same lineage home can retain history while the session consumer
switches an account after verified cleanup. Native Claude keeps its native
login/config directory. Fixtures prove the link and launch boundaries; a
disposable real two-login Codex A→B run also kept its native thread and
recalled the parent turn. An opt-in ordered Claude provider can select a named
native login after a custom API binding fails, and reopens the original sealed
transcript by path; the login remains private to that account. Other Claude
cross-account paths remain refused. A direct Claude Code API-to-subscription
probe retained a test session and context; Agent Run's handoff has synthetic
integration coverage, not a full real-provider failover qualification.

Custom Codex writes only the supported Responses
`model_provider = "agent_run_gateway"` settings and an `env_key` name into
the sealed config. Its token enters the child environment at attempt time,
and the generated shell policy excludes that key from tool shells. Custom
Claude sets `ANTHROPIC_BASE_URL` and the selected credential. Bearer gateways
receive `ANTHROPIC_AUTH_TOKEN` plus an explicitly empty `ANTHROPIC_API_KEY`
to prevent Claude from falling back to a saved API key; `x_api_key` gateways
receive only `ANTHROPIC_API_KEY`. It uses an isolated per-lineage config
directory rather than saved native login. No custom credential is written to
generated settings, argv, the provider metadata,
or the durable role document. Both routes keep configured roots, MCP, hooks,
native-setting guards, and model restrictions under the sealed role/assets
checks.

For OpenRouter, use an ordinary inference API key, not a management/provisioning
key. A successful `GET /api/v1/key` only confirms that the key is recognized:
`is_management_key = true` or `is_provisioning_key = true` identifies an
administrative key that cannot call completion endpoints. Check these metadata
flags before interpreting a successful credential probe as inference readiness.
See [OpenRouter's Claude setup](https://openrouter.ai/docs/cookbook/coding-agents/claude-code-integration)
and [management-key scope](https://openrouter.ai/docs/guides/overview/auth/management-api-keys).

Public `start` and `resume` use the provider path. Historical v1 rows retain
their separate compatibility reader; they do not create a public runtime
alias. Its compatibility-only removal points are
`config::Adapter::Glm`, `adapters::glm::cli_model`,
`materialize`'s old `Adapter::Glm` branch, the `fable`/GLM model mapping in
`core::stream::plan_with_environment`, GLM-specific environment handling in
`adapters::auth`, and the matching `core::supervisor` dispatch. No
public permanent GLM launch alias is promised by the v2 path.

## Private launch variables and tool processes

Agent Run's private launch variables must not reach a shell, test or child CLI
that the agent starts, where a `cargo test` or a nested CLI could act on the live
Run as the worker, or confuse Run and Bridge peer and ancestry tests.

| Variable | Codex | Claude Code (and Claude-compatible providers) |
| --- | --- | --- |
| `AGENT_RUN_WORKER_*` (endpoint, run, attempt, token, catalog selectors) | Harness environment; worker MCP via `env_vars`; excluded from tool shells | Only the worker MCP entry's `env` in a private per-attempt `--mcp-config`; absent from the harness and its tools |
| `AGENT_RUN_SYSTEM_*` (system writer and operator sockets) | Never forwarded | Never forwarded |
| `AGENT_RUN_LAUNCH_ID` | Harness environment; excluded from tool shells | Harness environment; visible to tools |
| `AGENT_RUN_PROVIDER_TOKEN` (custom gateway) | Harness environment; excluded from tool shells | Not used |
| `BRIDGE_NATIVE_OWNER_*` | Unchanged | Unchanged |

Before this change both harnesses inherited the worker capability in their own
environment, and Claude Code passed it on to every Bash command. In system mode
the supervisor's `AGENT_RUN_SYSTEM_WRITER_SOCKET` and
`AGENT_RUN_SYSTEM_OPERATOR_SOCKET` also reached every harness. Codex excluded only
the custom gateway token from tool shells.

Every generated Codex `config.toml` now writes
`shell_environment_policy.exclude = ["AGENT_RUN_*"]`; a custom gateway also keeps
`inherit = "core"`. Codex starts stdio MCP children with a separately filtered
environment plus the declared `env_vars`, so the worker MCP still authenticates.
This was checked against codex-cli 0.167.1: a `codex sandbox` command saw none of
the excluded names, and an MCP child still received its `env_vars`. The managed
Code Mode host is not a shell. Model code there can call only the enabled MCP
tools and has no process environment API. The policy applies to newly
materialized runtime homes; a lineage resumed from an earlier frozen home keeps
its sealed config.

Claude Code gives its whole environment to Bash and every other child process.
For each attempt the supervisor therefore removes `AGENT_RUN_WORKER_*` from the
launch environment. It copies the sealed `mcp/mcp-config.json` to
`<runtime_home>/worker-mcp/mcp-config.json` (directory 0700, file 0600, outside
the sealed index), writes the values into the worker entry's `env`, and points
`--mcp-config` at the copy. The sealed file and its digest are unchanged. The copy
is removed after the harness is reaped. If a supervisor dies, the next attempt
replaces the stale copy, and the broker refuses the old capability once its
attempt ends. User MCP catalog discovery for Claude starts after this split, so
discovery children also no longer receive the capability. The broker's finish
guard still recognizes the scoped values.

`AGENT_RUN_LAUNCH_ID` is deliberately kept in both harness environments. It is a
non-secret correlation marker, not a credential. Bridge reads it directly from
the matched harness process's `/proc/<pid>/environ` to bind a hook to its
journaled launch (`os::attempt::capture_launch`). Bridge's `detached_consumer`
and Run's Pool-resident peer check treat it in any same-UID ancestor as "inside
a managed launch". Removing it from the harness would break that detection. It
grants no authority: Run and Bridge accept it only from a kernel-verified harness
incarnation. Codex tool shells do not inherit it; ancestry checks still find it
on the harness itself.

OpenCode is not changed beyond the `AGENT_RUN_SYSTEM_*` removal. Its stdio MCP
servers inherit the harness environment, and moving the capability into a native
per-server environment needs a qualified OpenCode configuration field first.

This is protection against accidental inheritance. It is not a boundary against
a same-UID agent that deliberately reads another process's `/proc/<pid>/environ`
or the private per-attempt file.

## Native process ownership for Bridge

Managed native wake requires Agent Run to own the actual harness process as its
direct child. A launcher that spawns another process, including the npm Codex
JavaScript shim, does not meet this boundary. Select the matching native Codex
executable supplied by the installed package, or a launcher that replaces itself
with that executable. Keep the same model, account, catalog and workspace policy;
only the process launch path changes. Existing frozen runtime homes retain their
admitted launch identity.

## Managed OpenCode Bridge lifecycle

An operator may configure `[delivery.bridge.opencode_plugin]` with absolute
`package` and `hook_command` paths to the deployed first-party Bridge lifecycle
package and credential-free hook executable. The selected harness environment
must explicitly configure `BRIDGE_NATIVE_OWNER_CHANNEL` and
`BRIDGE_NATIVE_OWNER_DIRECTORY`. These settings identify components and IPC;
native recipient identity still comes from actual kernel and lifecycle facts.

Eligible explicit-finish roles receive exactly the package manifest, `index.js`
and hook executable in the existing immutable runtime snapshot. Source files
must be canonical regular owner/root-owned files, bounded in size and not
group/other-writable. Snapshot paths replace mutable deployment paths in native
configuration. Install scripts/dependencies, arbitrary harness plugins and hooks
remain unsupported. Research and IPC-isolated roles receive no projection.
Provider materialization derives explicit-finish eligibility from the admitted
worker-MCP role, preserving the same rule as the compatibility path.

The pinned OpenCode 2.0.28 plugin reports lifecycle only. Incoming Bridge messages
use the supervisor's existing owner route and native queue admission, with one
original deadline and no second input scheduler. Context injection and standalone
OpenCode root return delivery are not qualified by this managed-worker path.
