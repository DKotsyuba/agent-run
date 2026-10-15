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
an orchestrator's executor, suppress routine narration, preserve approval and
verification obligations, and request a compact final report. These are model
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
recalled the parent turn. Claude-family cross-account continuation remains
unverified and is refused. A real three-account rewrite and bound completion
delivery were not qualified by that two-login run.

Custom Codex writes only the supported Responses
`model_provider = "agent_run_gateway"` settings and an `env_key` name into
the sealed config. Its token enters the child environment at attempt time,
and the generated shell policy excludes that key from tool shells. Custom
Claude sets `ANTHROPIC_BASE_URL` plus exactly one selected
`ANTHROPIC_AUTH_TOKEN` or `ANTHROPIC_API_KEY`; it uses an isolated
per-lineage config directory rather than saved native login. No custom
credential is written to generated settings, argv, the provider metadata,
or the durable role document. Both routes keep configured roots, MCP, hooks,
native-setting guards, and model restrictions under the sealed role/assets
checks.

Public `start` and `resume` use the provider path. Historical v1 rows retain
their separate compatibility reader; they do not create a public runtime
alias. Its compatibility-only removal points are
`config::Adapter::Glm`, `adapters::glm::cli_model`,
`materialize`'s old `Adapter::Glm` branch, the `fable`/GLM model mapping in
`core::stream::plan_with_environment`, GLM-specific environment handling in
`adapters::auth`, and the matching `core::supervisor` dispatch. No
public permanent GLM launch alias is promised by the v2 path.

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
