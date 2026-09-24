# models

## claude, codex, and glm

Static rosters: whatever model ids are listed in each runtime's `models =
[...]` in config.toml. There is nothing else to sync: the broker loads a valid
changed config automatically, and new starts materialize the updated roster.
Existing sessions retain their admitted model identity.

### Verifying

`agent-run models` reports each configured runtime's declared models together
with the availability evidence its adapter can obtain. A missing model is a
configuration or provider-roster error; fix the runtime declaration before
admitting work with that model.

## Schema 2: the provider catalog

With `schema_version = 2`, `models` is the public provider catalog and
`capacity_order` the provider-only capacity order. Both are read-only views of
the current valid config, the canonical role files, and committed quota
observations: they never collect quota, probe a harness, reserve capacity or
start work. The caller alone picks provider, model, effort and profile; the
catalog reports facts and never recommends by itself.

Filters are exact identifiers; an unknown value is a `ValidationError`:

```sh
agent-run models --provider codex --profile review --model gpt-main
agent-run capacity order --model gpt-main
```

The same filters are the `models` / `capacity_order` tool arguments over MCP
and the broker socket (`{"provider":..,"profile":..,"model":..}`,
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
`unknown` = no current sample, `priority_overflow`, `exhausted`,
`no_eligible_account`), `best_priority`, `evidence` (`fresh` = a current
sample or an active exhaustion fact decided it, `stale` = samples exist but
none is current, `missing` = never observed), `newest_observed_at` (the
newest sample time on that lane, i.e. its age) and `exhausted_until` (the
earliest known reset when exhausted). These are cached facts, not health
claims. With a `profile` filter, providers are ranked over only the
offerings that role can use. No account id, label or credential reference
appears; per-account facts stay in `limits` (each account-bound row names
its `account` and physical `pool`) and `accounts list`.

Recommendations are plain configured prose. Editing them in `config.toml`
changes the next `models` result (and its `config_revision`); no skill needs
model or account constants:

```toml
[providers.codex]
harness = "codex"
connection = { kind = "native" }
auth_family = "openai"
limits_source = "exec"
collector = { command = "/bin/bash", args = ["/opt/agent-run/collectors/codex.sh"], source = "codex-appserver" }
recommendations = ["native subscription; strongest for long refactors"]
[[providers.codex.models]]
id = "gpt-main"
native_model = "gpt-native"
allowed_params = { effort = ["medium", "high"] }
params = { effort = "medium" }
recommendations = ["broad coding"]
[[providers.codex.models]]
id = "gpt-review"
restrictions = ["web_tools_disabled"]
[[providers.codex.bindings]]
label = "personal"
account = "acct-codex"
```

`params.effort` is the effective default when a start omits effort. Other
parameter keys are rejected until a launch adapter can execute them.

A schema-1 file keeps its historical runtime roster and route order; the
filters are `Unsupported` there.
