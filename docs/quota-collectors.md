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
  (plus any extra markers) before anything is exposed to Lua, so an
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

## Account-scoped driver (`capacity::collectors`)

* A provider with `limits_source = "lua"` must bind an explicit
  `CollectorBinding` — a first-party script identity (`glm_quota`,
  `anthropic_usage`) plus one to eight exact origins (HTTPS, or plain HTTP
  only for loopback fixtures; never userinfo, fragments, queries, or paths).
  Validation is part of the provider contract: a Lua source without a
  binding, a binding without the Lua source, and unknown origin spellings
  are all rejected. A script or credential identity is never inferred from
  a provider's name, and an unknown script identity surfaces as the typed
  `collector_unknown` round failure.
* One round collects once per `(global account, collector)` regardless of
  how many provider labels bind the account: aliases denote one physical
  account, so they never duplicate remote requests or fork pools. The
  model set is the union of each bound provider's
  `native_model.unwrap_or(id)` spellings and is the exhaustive membership
  a script may report.
* Credentials come only from the account registry's protected reference
  through the native `CredentialReader`: environment, private file, and
  Keychain stores resolve at request time; native and named harness logins
  are refused (`native login remains owned by the harness`) and never
  become exportable quota tokens. Resolved bytes go straight into the
  redacted `AuthCapability` — never configuration, reports, or the store.
  GLM quota uses `RawAuthorization` (the quota endpoint is not the
  inference gateway's Bearer form); Anthropic OAuth uses
  `BearerAuthorization` plus the script-supplied `anthropic-beta` header.
* Failures apply a bounded exponential backoff keyed by
  `(account, collector source)` — 60 s doubling to a 900 s ceiling, shared
  across every alias — and a success clears it. Suppressed rounds issue no
  remote request and leave previous samples and the durable exhaustion
  latch untouched. All network and credential work happens outside every
  database transaction; persistence runs through
  `record_quota_snapshot` only after a round fully succeeds.

## First-party collectors

* `glm_quota` (source identity `glm-quota`) follows the official
  `glm-plan-usage` contract: `GET {origin}/api/monitor/usage/quota/limit`,
  payload root `data` when present else the top-level object, `limits[]`
  entries with `type` and `percentage`. `TOKENS_LIMIT.percentage` is the
  five-hour token usage and is reported as the remaining share of one
  `primary` pool's `five_hour` window. `TIME_LIMIT` is monthly MCP usage
  and does not govern inference quota, so it is not reported; the official
  contract establishes no weekly or reset fields and none are invented.
* `anthropic_usage` (source identity `anthropic-usage`) calls
  `GET {origin}/api/oauth/usage` with the Bearer capability and the
  `anthropic-beta: oauth-2025-04-20` header. It ports the **recorded**
  envelope (`limits[]` of `kind`/`percent`, optional RFC 3339 `resets_at`;
  `session` → `primary`/`five_hour`, weekly kinds →
  `secondary`/`seven_day`). That envelope has not been re-verified against
  a live endpoint; a contract drift surfaces as the engine's typed failure
  categories, never as invented windows.
* Live-contract evidence is tracked separately from these fixture-verified
  paths: the GLM field names come from the official plugin source; the
  Anthropic live schema remains an open verification gate.
