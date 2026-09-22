# Quota collectors

Public semantics of the physical quota observation layer and the embedded
Lua collector engine (store schema v17 facts, `agent_run_core::capacity`).

## Observation layer (`capacity::quota`, `agent_run_store::quota`)

* Collectors report **version-1 output**: `{ "windows": [ { pool, window,
  models, remaining_percent?, reset_at?, observed_at, valid_until? } ] }`.
  `pool` is the provider-independent physical lane; `models` is the explicit
  membership; absent `remaining_percent`/`reset_at` mean unknown and are
  preserved, never invented.
* Rust binds the global `AccountId`, the collector source identity, the
  observing runtime scope, and the configured model set
  (`CollectorScope`); scripts and raw output never choose identity.
  `normalize_collector_output` validates through `NormalizedQuotaSnapshot::
  validate`: foreign models, duplicate/conflicting windows, nonfinite or
  out-of-range numbers, inverted times, and oversized outputs are rejected
  before anything becomes an account fact.
* One global account owns one physical pool per lane across every provider
  alias (`PhysicalQuotaKey` = `account::lane`); two aliases never fork or sum
  a pool.
* `agent_run_store::quota::record_quota_snapshot` persists windows into
  `capacity_samples` (global account in `target`, lane in `lane`, model
  membership in `payload_json`) in one immediate transaction that advances
  the single `quota_capacity_revision` exactly once when it mutates rows, and
  not at all for a no-op round. The store only persists raw facts; scoring,
  ranking, and reservation stay in core.
* **Exhaustion latch**: a persisted zero-remaining window survives a round
  that only produced unknown data (timeout/malformed rounds persist nothing)
  until its known reset passes or — with no reset — positive fresh evidence
  arrives. Carried facts keep their original observation times; only the
  survival horizon extends (to the reset, or one 900 s TTL past the round).
* Freshness, unknown data, and collection failure are distinct states; a
  failed collector never replaces good evidence, and read-only advice never
  fetches network or reserves.

## Lua collector engine (`capacity::lua`)

* Lua 5.4 embedded via mlua 0.12.1 (vendored). Each invocation runs a fresh
  restricted VM: coroutine/table/string/math/utf8 libraries only; `io`, `os`,
  `debug`, `package`, `load`, `loadstring`, `dofile`, `require`, and `print`
  are absent; chunks load in text-only mode so bytecode is rejected.
* Default bounds (validated, overridable only positively and under hard
  ceilings): 16 MiB VM memory, 10,000,000 instructions, 30 s invocation,
  8 HTTP requests, 10 s and 2 MiB per request, 256 output windows.
  Instruction and wall limits are enforced by the interpreter hook, so tight
  CPU loops abort; the whole invocation is a cancellable future with no
  detached workers.
* `collect(ctx)` receives the nonsecret host-bound account identity, explicit
  model configurations, host time, and an opaque auth marker. `ctx.http.
  request` validates the exact configured scheme/host/port allowlist (no
  userinfo, fragments, or wildcards) **before** Rust injects `Authorization`
  from the host-held capability; credentials never enter Lua, and redirects
  are followed only within the same origin. Production transport is reqwest
  with redirects disabled and bounded streamed bodies; tests inject fakes.
* Errors crossing the boundary are static typed categories
  (`CollectorError`, `QuotaHttpError`); Lua error text and HTTP bodies never
  appear externally, and known secret markers are redacted from any
  diagnostic.
* Script identity is the exact-byte SHA-256 frozen per invocation; a
  `ScriptRegistry` retains the last valid revision and refuses to let a
  rejected replacement silently displace it.
