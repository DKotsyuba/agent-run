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

Native Codex uses the existing login and app-server invocation. Its selected
`auth.json` link is bound per attempt outside the immutable asset manifest,
so the same lineage home can retain history while the session consumer
switches an account after verified cleanup. Native Claude keeps its native
login/config directory. A fixture can prove the link and launch boundaries;
real A→B native continuity remains unproven.

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

The current v1 consumer path remains intact until session dispatch switches
to these APIs. Its compatibility-only removal points are
`config::Adapter::Glm`, `adapters::glm::cli_model`,
`materialize`'s old `Adapter::Glm` branch, the `fable`/GLM model mapping in
`core::stream::plan_with_environment`, GLM-specific environment handling in
`adapters::auth`, and the matching `core::supervisor` dispatch. The
capacity source's legacy GLM mapping remains quota-side work. No public
permanent GLM launch alias is promised by the v2 path.
