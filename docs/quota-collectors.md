# Quota collectors

Public semantics of the physical quota observation layer and the embedded
Lua collector engine (`agent_run_core::capacity::{quota,lua,collectors}`,
`agent_run_store::quota`).

## Observation layer (`capacity::quota`, `agent_run_store::quota`)

* Collectors report **version-1 output**: `{ "version": 1, "windows": [ {
  pool, window, models, remaining_percent?, reset_at?, observed_at,
  valid_until? } ] }`. The `version` envelope is mandatory and no other
  top-level key is accepted. `pool` is the provider-independent physical
  lane; `models` is the explicit membership (repeated names are malformed);
  absent `remaining_percent`/`reset_at` mean unknown and are preserved,
  never invented; `observed_at` may not lie in the future of the host clock.
* Rust binds the global `AccountId`, the stable collector source identity,
  the observing runtime scope, and the configured model set
  (`CollectorScope`); scripts and raw output never choose identity.
  `normalize_collector_output` validates through `NormalizedQuotaSnapshot::
  validate`: foreign models, duplicate or contradictory pool windows,
  nonfinite or out-of-range numbers, inverted or future times, and oversized
  outputs are rejected before anything becomes an account fact.
* One global account owns one physical pool per lane across every provider
  alias (`PhysicalQuotaKey` = `account::lane`); two aliases never fork or sum
  a pool. A shared pool's window facts must be identical for every model
  that declares it.
* `agent_run_store::quota::record_quota_snapshot` persists **one row per
  physical window** — never one per model — with explicit
  `capacity_samples.account_id`/`quota_key` identity (the legacy `target`
  column is kept only as a view mirror), the lane in `lane`, and full model
  membership in `payload_json`, in one immediate transaction that advances
  the single `quota_capacity_revision` exactly once when it mutates rows and
  not at all for a no-op round. The account must be registered
  (`provider_accounts`), enforced by foreign key and an explicit
  pre-transaction check. The store only persists raw facts; scoring,
  ranking, and reservation stay in core.
* **Exhaustion latch** is durable in `quota_exhaustion`, keyed by account,
  physical key, stable collector source, and window — independent of rolling
  sample retention and of script revisions: a latched zero-remaining window
  survives a round that only produced unknown data (timeout/malformed
  rounds persist nothing) until its known reset passes or — with no reset —
  actually fresh positive evidence arrives (`remaining > 0`, observed at least
  as late as the exhausted fact and not later than now, validity unexpired,
  reset not passed). Missing pools and unrelated pools cannot release a latch;
  an older exhausted report cannot roll it back. Matching is by lane and
  window across collector source identities, so a script revision neither
  forks the pool nor strands the latch. Carried facts keep their original
  observation times; only the survival horizon extends (to the reset, or one
  900 s TTL past the round).
* Freshness, unknown data, and collection failure are distinct states; a
  failed collector never replaces good evidence, and read-only advice never
  fetches network or reserves.

## Lua collector engine (`capacity::lua`)

* Lua 5.4 embedded via mlua 0.12.1 (vendored). Each invocation runs a fresh
  base-only VM. The optional coroutine, table, string (including Lua pattern
  matching), math, and utf8 libraries are absent, as are `io`, `os`, `debug`,
  `package`, `load`, `loadfile`, `loadstring`, `dofile`, `require`, `print`, and
  `collectgarbage`; chunks load in text-only mode so bytecode is rejected.
* Default bounds (validated, overridable only positively and under hard
  ceilings): 16 MiB VM memory, 10,000,000 instructions, 30 s invocation,
  8 HTTP requests, 10 s and 2 MiB per request, 256 output windows.
  Instruction and wall limits are checked by the interpreter hook. Lua 5.4
  hook errors are ordinary catchable errors, so the base `pcall`/`xpcall` are
  **guarded**: once either budget is exhausted they refuse to protect
  anything, and every later hook tick errors, so a script catching aborts
  inside `pcall` still collapses within bounded ticks and the invocation
  returns a typed `Timeout`/`InstructionLimit` on its own. Native host work
  (such as bounded JSON conversion) cannot be preempted by a Lua hook; the
  wall limit is cooperative, not a hard real-time deadline. No collector may
  use Lua pattern matching or coroutines. Registry compilation validates the
  same resource limits and rejects source larger than the VM memory bound.
* `collect(ctx)` receives the nonsecret host-bound account identity, explicit
  model configurations, host time, an opaque auth marker, bounded host JSON
  (`ctx.json.decode/encode`, static `quota_json_*` errors, body-bounded), a
  bounded RFC 3339 helper (`ctx.time.rfc3339`, nil for unparseable input),
  and `ctx.http.request`.
* The `AuthCapability` is constructed bound to exactly one global account,
  an explicit placement (`RawAuthorization` for GLM quota, `BearerAuthorization`
  for Claude-style OAuth, `Header(name)` for gateway keys), and its validated
  origins; the engine re-checks both bindings per invocation. `ctx.http.
  request` validates the exact scheme/host/port allowlist (HTTPS, or
  explicit loopback HTTP; no userinfo, fragments, or wildcards) under the
  same canonical `reqwest::Url` interpretation the transport uses, **before**
  Rust injects the credential header; redirects are resolved by the
  transport's URL semantics, followed only within the same origin, and every
  hop re-authorizes against origin and the checked shared budget (no
  underflow, exhaustion is permanent across caught errors). Scripts cannot
  set the configured credential header, `authorization`, `host`, `cookie`,
  `connection`, or `proxy-authorization`.
* Response bodies and header values are scrubbed of the credential value
  (plus any extra markers), and response header names containing credential
  material are omitted before anything is exposed to Lua, so an
  endpoint echoing the injected secret cannot make it script-observable.
  Requests carrying injected credentials derive to a redacted `Debug`, as
  does the capability itself. Errors crossing the boundary are static typed
  categories (`CollectorError`, `QuotaHttpError`); Lua error text and HTTP
  bodies never appear externally.
* Script identity is the exact-byte SHA-256 frozen per invocation;
  `ScriptRegistry` retains the last valid revision, and a replacement must
  both verify and compile in text mode inside a bounded throwaway VM before
  it can displace it. The presence and contract of `collect` are enforced
  per invocation.
* `ctx.origin` is the single Rust-chosen request origin, validated against
  the capability's origin allowlist before the VM starts and normalized of
  its trailing slash (Lua has no string library to do that itself). Scripts
  concatenate endpoint paths onto it; every request still re-authorizes
  against the full allowlist and budget.
* Bounded host helpers replace the absent libraries: `ctx.json.decode` /
  `ctx.json.encode` (decoded JSON `null` is the `ctx.json.null` sentinel,
  distinct from an absent field), `ctx.time.rfc3339` (RFC 3339 → Unix
  seconds or `nil`), and `ctx.text.tokens` (≤128-byte text → at most 16
  ASCII-lowercased alphanumeric tokens, for case-insensitive model-name
  membership). There is still no string, pattern, or coroutine library.
* `retry-after` on a throttled response accepts delay-seconds or an
  HTTP-date (measured against the host clock); positive horizons clamp to
  900 s, and a throttle without a usable horizon is still the typed
  rate-limit outcome.

## Account-scoped driver (`capacity::collectors`)

* A provider with `limits_source = "lua"` must bind an explicit
  `CollectorBinding` — a first-party script identity (`glm_quota`,
  `anthropic_usage`) plus one to eight exact origins (HTTPS, or plain HTTP
  only for loopback fixtures; never userinfo, fragments, queries, or
  paths), and optionally an absolute `script_file` plus an explicit `auth`
  placement for a provider-specific custom Lua script. Validation is part
  of the provider contract: a Lua source without a binding, a binding
  without the Lua source, unknown origin spellings, and a custom script
  without an explicit placement are all rejected. A script or credential
  identity is never inferred from a provider's name; an unknown script
  identity surfaces as the typed `collector_unknown` round failure, and a
  first-party identity cannot be overridden by a script file. Custom
  scripts install through the retained-script registry under their
  complete source identity — script id plus canonical script file — so two
  configurations reusing one id never share code. The file is read through
  a 256 KiB bound (never allocating past it); a replacement is installed
  only when it verifies and compiles, and each accepted revision is also
  kept at `capacity/scripts/<sha256(identity)>.lua`, so a missing,
  oversized, or invalid edit falls back to the last valid revision in this
  process and in later polling rounds. Script files carry no credentials.
* Every alias of one `(global account, script)` must declare the exact
  same binding; a contradiction is rejected during planning, before any
  credential access or network request, in either declaration order. One
  round collects once per `(global account, collector)` regardless of how
  many provider labels bind the account: aliases denote one physical
  account, so they never duplicate remote requests or fork pools. The
  model set is the union of each enabled eligible binding's effective
  model subset (its own subset, else the provider's full set), keyed by
  each model's `native_model.unwrap_or(id)` spelling, and is the
  exhaustive membership a script may report.
* Credentials come only from the account registry's protected reference
  through the quota-side `QuotaCredentialReader`: environment, private
  file, and Keychain stores resolve at request time exactly as the shared
  system reader does. Native and named **Claude** logins resolve only
  inside Rust through the account-home convention the provider adapter
  launches with: `native:claude-code` is the host login (`CLAUDE_CONFIG_DIR`
  when set, else `~/.claude`); `named:claude-code:<label>` is only the
  directory `auth login` and runs also use (`claude_account_config`):
  `<home>/accounts/claude/<label>/claude-config`, or an earlier release's
  `<claude harness home>@<label>/claude-config` when only that exists; both
  existing is an ambiguity error. Each directory is read
  as Claude Code reads it — `.credentials.json`, else the Keychain item
  `Claude Code-credentials`, suffixed `-<first 8 hex of sha256(dir)>` for
  any explicit directory. A missing named store is an error and never
  falls back to the default login. Codex logins are refused here because
  Codex quota travels through app-server metadata. The generic
  custom-gateway reader keeps its own native refusal unchanged. Every
  resolution failure is reported as the one fixed `credential_unavailable`
  code — reader text is never forwarded, whatever it starts with — and
  store and ledger failures are the fixed `store_failed` /
  `backoff_persist_failed` codes. Resolved bytes go straight into the
  redacted `AuthCapability` — never configuration, reports, or the store. GLM quota
  uses `RawAuthorization` (the quota endpoint is not the inference
  gateway's Bearer form); Anthropic OAuth uses `BearerAuthorization` plus
  the script-supplied `anthropic-beta` header.
* Failures apply a bounded backoff keyed by `(account, collector source)`
  — 60 s doubling to a 900 s ceiling, an endpoint-declared `retry-after`
  horizon overriding the exponential default within the same cap — shared
  across every alias. Throttled statuses (429/503) never reach Lua: the
  engine classifies them typed with the parsed bounded horizon carried on
  the error, never raw headers. The ledger is durable in
  `capacity/backoff.json` under the agent-run home, so suppression
  survives the process boundary between launchd polling rounds.
  Suppressed rounds issue no remote request and leave previous samples
  and the durable exhaustion latch untouched. All network and credential
  work happens outside every database transaction; persistence runs
  through `record_quota_snapshot` only after a round fully succeeds.
* The polling path dispatches on schema: `capacity collect` runs the
  account-scoped provider sources (Lua units plus Codex app-server units)
  for a schema-v2 home, and the legacy per-runtime path otherwise; there a
  retired `codexbar` source is never invoked and reports the fixed
  `codexbar_retired_migration_required` failure, keeping earlier samples.
  A home
  that declares `schema_version = 2` but fails to load reports that error
  instead of falling back to the legacy sources. The round's `ok` is true
  only when no Lua or Codex row failed and the backoff ledger persisted.
* CodexAppserver providers are probed once per registered account, however
  many provider aliases bind it, through `account/rateLimits/read` with the
  verified isolated probe and cleanup contract. The probe login comes from
  the account's protected reference — `native:codex` is the host login
  (`CODEX_HOME`, else `~/.codex`), `named:codex:<label>` is
  `<home>/accounts/codex/<label>` — never from a provider display label.
  Buckets (`rateLimitsByLimitId`, or the legacy single `rateLimits`) map to
  models only by the provider's own naming: a bucket whose `limitName`
  token sequence equals a bound model's native spelling governs that model
  alone; the general `codex` bucket governs the remaining bound models;
  any other bucket (live example: `base_model_inference` named
  `gpt-reserve`) has no known membership, is not applied to any model, and
  is counted in the row's `unmapped_lanes`. Observations are normalized by
  the same closed version-1 validation and keyed by the registered
  `AccountId`, which is what provider ranking consumes.

## First-party collectors

* `glm_quota` (source identity `glm-quota`) calls
  `GET {origin}/api/monitor/usage/quota/limit`; the payload root is `data`
  when present, else the top-level object. Current Coding Plan accounts
  return one `CREDIT_LIMIT` entry per window (the five-hour and weekly
  pools documented at <https://zcode.z.ai/en/docs/usage-stats>). Their
  `unit`/`number` encoding — `3`/`5` five hours, `6`/`1` one week — and
  `nextResetTime` in epoch milliseconds, `usage` total and `currentValue`
  used, come from an observed payload
  (<https://github.com/robinebers/openusage/issues/1104>) and were
  cross-checked against the live account; they are not published schema.
  The older official `TOKENS_LIMIT` (bare `percentage` = five-hour usage,
  as in the official `query-usage.mjs`) is still accepted. Both land in the
  `primary` pool as distinct `five_hour` / `seven_day` windows.
  `percentage` must agree with the counts within one point (the provider
  rounds it); `remaining` is ignored because live values are not
  `usage - currentValue`. `TIME_LIMIT` (monthly MCP) is never converted
  into inference capacity. Unknown types, window encodings, out-of-range
  numbers, or resets outside years 1–9999 fail the round typed.
* `anthropic_usage` (source identity `anthropic-usage`) calls
  `GET {origin}/api/oauth/usage` with the Bearer capability and the
  `anthropic-beta: oauth-2025-04-20` header, and reads only `limits[]`; the
  parallel top-level `five_hour` / `seven_day*` objects describe the same
  pools and are never added in. `session` → `primary`/`five_hour` and
  `weekly_all` → `secondary`/`seven_day` over every bound model;
  `weekly_scoped` → its own `model:<key>`/`seven_day` pool over exactly
  the bound models its `scope.model` names (exact `id`, or every
  `display_name` token present in the model name — live payloads carry
  `{"display_name": "Fable", "id": null}`). A scoped limit naming no bound
  model does not apply to the account. `is_active` only marks the
  currently binding limit (live: `true` on the critical scoped entry), so
  it never filters. Unknown kinds, a non-null `scope.surface`, unusable
  model scopes, scopes on general kinds, and invalid percents or resets
  fail the round typed instead of inventing or dropping a governing pool.
* The credential-safe canary `cargo run -p agent-run-core --bin
  quota_canary -- <glm|anthropic> [origin]` performs one real read-only
  request against an approved account and prints only normalized facts:
  status, typed failure, bounded retry-after, top-level keys, `limits[]`
  field names and kinds, and an allowlist of scalar window/scope facts
  (`unit`, `number`, `percentage`/`percent`, count consistency, reset
  horizon, `group`, `is_active`, `severity`, reduced `scope`).
  `quota_canary codex [label]` (with `AGENT_RUN_HOME`, optional
  `CODEX_BINARY`) prints only bucket ids, `limitName`, window minutes and
  used percent. Tokens, bodies, and headers are never printed.

## Pool membership consumed by selection

A physical pool id (`primary`, `secondary`, `credits`, `codex`,
`model:<key>`) is never a model name. `record_quota_snapshot` stores, on
every persisted window row, the exact model lanes (native alias, else model
id — the names each collector unit is built with) that window governs, in
`payload_json.models`, per window rather than per pool, so a narrower window
never inherits every model of its pool. Account selection, the `models` and
`capacity_order` views and admission reservations consume that membership:
a window governs a model only when its newest row names that model's lane.
While an exhaustion latch remains active, a partial unknown observation cannot
shrink that window's recorded membership; the prior governed lanes remain
until reset or fresh evidence covers them all. Other physical windows keep
their own membership.
Only rows with no recorded membership (written before membership was
recorded) fall back to a pool id equal to the lane. All governing windows of
all governing pools enter one model's score and exhaustion gates, and one
pool is reserved once however many models or provider aliases it governs.
Ranking merges sample history by account, physical key, source, and window;
renaming a provider alias cannot turn one physical window into two competing
forecasts. A different physical key remains a separate quota constraint.

Membership is per window end to end: the normalizer gives each model only
the windows whose output names it, and a normalized snapshot may therefore
show different subsets of one pool's windows under different models (one
physical window repeated under several models must still carry the same
fact). An exhaustion latch is carried through an incomplete round only into
the models its window governed; it is never copied into another model that
merely shares the pool. When a partial round carries the window into only a
subset of those models, persistence retains the complete previous membership.

Retention never deletes, for any latched physical window, that window's
newest row in the ranker's `(observed_at, id)` order, so the latch keeps
restricting exactly the models that row names regardless of other windows,
unrelated newer samples or later-inserted stale rows. A row whose payload
has no valid `models` array is legacy: it governs only the lane equal to its
pool id, in both the store and the ranker; malformed metadata is never read
as a model mapping.

Test layouts with pools such as `credits` and windows such as `monthly`
exercise the per-window contract; they are not evidence about any native
provider's quota semantics (the first-party GLM collector's own windows are
unchanged). A
latch written from an authoritative native signal (for example a Claude
`rate_limit_event` rejection of a mapped `five_hour` window) records the
observation itself as a zero-remaining row carrying the membership the
collector mapping defines. No schema change is needed: the membership was
already persisted in `payload_json`.
