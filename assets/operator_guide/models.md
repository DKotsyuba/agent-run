# models

## Historical schema 1: claude, codex, and glm

Static rosters come from each runtime's `models = [...]` in config.toml.
The broker reloads valid changes automatically; new starts use the new roster
and existing sessions retain their admitted model identity.

### Verifying

`agent-run models` reports each configured runtime's declared models together
with the availability evidence its adapter can obtain. A missing model is a
configuration or provider-roster error; fix the runtime declaration before
admitting work with that model.

## Schema 2: the provider catalog

MCP discovery exposes two routing reads: `delegation_guide` and `limits`.
Schema 2 keeps `models` and `capacity_order` as call-only compatibility views.
They read valid config, canonical role files and committed quota observations;
they never collect quota, probe a harness, reserve capacity or start work.
The caller picks provider, model, effort and profile; configured guidance is
prose, not an automatic model-suitability decision.

Filters are exact identifiers; an unknown value is a `ValidationError`:

```sh
agent-run models --provider codex --profile review --model gpt-main
agent-run capacity order --model gpt-main
```

The same filters remain accepted by direct compatibility calls to `models` /
`capacity_order` over MCP and the broker socket (`{"provider":..,"profile":..,"model":..}`,
`{"model":..}`). Each result carries `config_revision` (SHA-256 of the exact
`config.toml` bytes), `capacity_revision` (the committed quota/registry
snapshot it was read from) and `ranked_at` (the clock the standing was
evaluated at — not a sample age); `models` also carries `roles_sha256` over
the listed role grants. Registry status, samples and exhaustion facts all
come from that one committed read. Reuse a result only while
`config_revision`, `capacity_revision` and `roles_sha256` all match: role
files can change without a config edit.

`models` lists, per provider (in capacity order): `harness`, connection
`kind`/`protocol`, `auth_family`, `limits_source`, `priority_multiplier`,
`score` (best available priority times the multiplier, saturating at the
largest finite double, or `null` when nothing is available; equal scores
order by status, then provider id), provider `recommendations`, and every
explicit offering with its
`native_model`, default `params`, `allowed_params` (for example efforts; a
chosen `effort` outside the list is refused at start and resume),
hard `restrictions`, model `recommendations`, the canonical `profiles`
admission would accept for it, and cached `quota`: `status` (`available`,
`unknown` = no usable known standing, `priority_overflow`, `exhausted`,
`no_eligible_account`), `best_priority`, `evidence` (`fresh` = a current
sample or an active exhaustion fact decided it, `stale` = samples exist but
yield no usable known standing, `missing` = never observed), `newest_observed_at`
(the newest sample timestamp on that lane) and `exhausted_until` (the
earliest known reset when exhausted). These are cached facts, not health
claims; `unknown`/`stale` alone do not prove an expired validity timestamp.
With a `profile` filter, providers are ranked over only the
offerings that role can use. No account id, label or credential reference
appears; per-account facts stay in `limits` (each account-bound row names
its `account` and physical `pool`) and `accounts list`.

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

A schema-1 file keeps its historical runtime roster and route order; the
filters are `Unsupported` there.

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

## MCP compact presentation

Use `delegation_guide` for compact provider/model guidance and `limits` for
diagnosis. Limits retain quota windows, percentages, reset times and freshness
and add schema-2 provider/model standing, scores and provider multipliers in
`ranking`. Windows and ranking share one committed snapshot, config revision
and advice clock (`observed_at` equals `ranking.ranked_at`); the existing ranker
considers every governing window and exhaustion fact. Unknown/stale capacity
is never presented as healthy. MCP text omits account/pool identities and
retains whole-response size/row bounds; CLI/socket diagnostic JSON keeps those
original physical-window identities. Call-only `models` and `capacity_order`
retain their original compact MCP layouts and structured CLI/socket shapes.

## Delegation guide

`agent-run delegation-guide` and the `delegation_guide` MCP/socket read accept
the same exact optional `provider`, `model`, and `profile` filters, with typed
`ValidationError` for unknown names. For example:

```sh
agent-run delegation-guide --provider codex --model gpt-main --profile review
```

Omitted filters retain the default guide. One committed snapshot renders a
compact operating header, providers in capacity order with harness/guidance,
and each exact selectable model: cached quota status/evidence, sample age and
reset horizon derived from `ranked_at`, admissible canonical profiles
(`profiles: none` means no role may use it), nonempty default/allowed params,
restrictions and configured guidance. Schema 1 is `Unsupported`; an empty
catalog is explicit. Text omits skills, MCP arrays, hashes, accounts,
credentials and endpoints, normalizes recommendation whitespace/control
characters, and adds no model-ability ranking or facts beyond its snapshot.

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
