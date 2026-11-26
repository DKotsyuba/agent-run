# models

## Historical schema 1: claude, codex, and glm

Static rosters come from each runtime's `models = [...]` in config.toml.
The broker reloads valid changes automatically; new starts use the new roster
and existing sessions retain their admitted model identity.

### Verifying

Schema 1 has no provider catalog read; migrate to schema 2 and use the
delegation guide below.

## Schema 2: the provider catalog

MCP discovery exposes one routing read: `delegation_guide`. It reads valid
config, canonical role files and one committed local snapshot. For native
Codex it can read one Quota snapshot to flag eligible paid-credit fallback.
Agent Quota presents windows, reset times and capacity standing
(`agent-quota limits` or its MCP). Admission reads a fresh Quota snapshot per
decision. Native Codex accounts with reported paid or unlimited credits remain
eligible after window exhaustion and rank last. The caller picks provider,
model, effort and profile using the configured guidance.

For each provider (in configured order) the guide shows its harness and
recommendations and every explicit offering with its default `params`,
`allowed_params` (for example efforts; a chosen `effort` outside the list is
refused at start and resume), hard `restrictions`, model `recommendations`
and the canonical `profiles` admission would accept for it. With a `profile`
filter, only offerings that role can use are listed. No account id, label or
credential reference appears; per-account facts stay in `accounts list`.

Edit provider/model `recommendations` in `config.toml`; valid reload changes
the next guide/catalog result and its `config_revision`. Skills need no model
or account constants. Configuration excerpt:

```toml
[providers.codex]
recommendations = ["native subscription; long refactors"]
[[providers.codex.models]]
id = "gpt-main"
allowed_params = { effort = ["medium", "high"] }
params = { effort = "medium" }
recommendations = ["broad coding"]
```

Omitted effort uses `params.effort`. Other parameter keys are rejected until a
launch adapter can execute them. See `agent-run doc config` for full config.

A schema-1 file has no provider catalog; the guide is `Unsupported` there.

### GLM effort through Claude Code

For `glm-5.3` and `glm-5.3-flash`, a wire check with Claude Code 2.1.280
confirmed that `--effort low|high|max` reaches the Messages request unchanged
as `output_config.effort`. Omitting the flag in that check sent `high`;
do not assume that omission requests the provider's `max` default. Configure
`params.effort` explicitly when a stable default matters.

[Z.AI's effort contract](https://docs.z.ai/devpack/latest-model#switch-effort-thinking-intensity)
maps `minimal|light|low` to `low`, `medium|high` to `high`, and
`xhigh|max|ultra` to `max`. Explicit effort takes priority over the thinking
toggle and provider default. This describes wire behavior, not a model-ability
ranking; only use values admitted by the configured `allowed_params`.

## Delegation guide

`agent-run delegation-guide` and the `delegation_guide` MCP/socket read accept
the same exact optional `provider`, `model`, and `profile` filters, with typed
`ValidationError` for unknown names. For example:

```sh
agent-run delegation-guide --provider codex --model gpt-main --profile review
```

Omitted filters retain the default guide. One local read snapshot renders a
compact operating header, providers in configured order with
harness/guidance, and each exact selectable model: admissible canonical
profiles (`profiles: none` means no role may use it), nonempty default/allowed
params, restrictions, configured guidance and any eligible Codex paid-credit
fallback. Read current capacity from Agent Quota before choosing. Schema 1 is
`Unsupported`; an empty catalog is explicit. Text omits skills, MCP arrays,
hashes, accounts, credentials and endpoints, normalizes recommendation
whitespace/control characters, and adds no model-ability ranking or facts
beyond its inputs.

Before delegating a task, the orchestrator must call this tool and read its
output before choosing provider, model, effort or profile. Keep task-suitability
and cost guidance in each provider/model's `recommendations`; changes appear
after configuration reload without editing an orchestration skill. Empty guidance
does not establish a model's suitability. Model choice remains the orchestrator's
decision; omit the account unless the request explicitly pins one.

Use the advertised start/resume schemas and completion contract. A brief names
the bounded deliverable, role, working directory, allowed reads/writes and checks.
Codex grants the working directory automatically. `read_roots` adds other paths
only when the selected role permits external reads; leave it empty when
`allow_external_read_roots = false`. Writable tasks must fit their granted
workspace and cannot gain access from paths in the prompt. The role owns write
permission; the request's compatibility `write` flag cannot narrow or expand it.
Respect requests to work personally, available capacity and permission refusals.
Inspect the returned evidence rather than treating a terminal status as acceptance.
