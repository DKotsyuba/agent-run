# External quota collectors

Every schema-2 provider selects an external executable or disables collection.
There are no built-in provider collectors and no embedded scripting interpreter.
The executable may be Bash, Python, another interpreter with a script argument,
or a native program. The operator owns its dependencies and permissions.

```toml
[providers.glm]
# Existing harness, models and account bindings remain required.
limits_source = "exec"

[providers.glm.collector]
command = "/bin/bash"
args = ["/opt/agent-run/collectors/glm.sh"]
source = "glm-quota"
timeout_seconds = 30
# env_from = ["HTTPS_PROXY"]
```

The command must be absolute. Arguments are passed literally, without shell
expansion, interpolation or an implicit shell. Use an interpreter as command
and its script path as the first argument when desired. The optional source
is a stable physical observation name; omission derives it from command/args.
Keep an existing source (glm-quota, anthropic-usage, codex-appserver) during an
upgrade to preserve forecast history. The command is read anew on every round;
a missing or broken replacement fails, with no hidden fallback to older code.

Collector configuration is mandatory for limits_source=exec and forbidden for
limits_source=none. Lua and codex_appserver remain readable only inside frozen
historical runs; current config rejects them with migration guidance. Configure
all providers explicitly before switching the installed binary. The database
schema and version-1 observation envelope are unchanged.

## Invocation protocol

Rust writes one UTF-8 JSON document to stdin and closes stdin. The document is
private: never log it or copy it to argv. For a token-backed account:

```json
{
  "version": 1,
  "account": {"id": "acct-work", "auth_family": "anthropic"},
  "models": {"native-model": {"id": "configured-model", "native_model": "native-model"}},
  "now": 1790000000,
  "auth": {"kind": "token", "token": "<protected value>"},
  "harness": {"id": "claude-code", "command": "/usr/local/bin/claude"}
}
```

The models object contains the union of enabled bindings for that account and
collector, keyed by native model spelling. It is the complete allowed output
membership. The account comes from the registry, never a provider-local label.
Native Claude OAuth and explicit env/file/Keychain tokens use the token form.
Native Codex uses auth.kind=native_login and auth.directory pointing to that
account's native login directory instead; its executable uses the supplied
harness command to query metadata without running a model. These are credential
bridges, not provider quota protocol implementations in Rust.

The child environment retains HOME, PATH, USER, LOGNAME, LANG, LC_ALL and TMPDIR,
plus explicitly configured env_from names. Scripts run with normal user rights:
this is trusted operator code, not a sandbox. Secrets must stay out of script
arguments, stdout, stderr and intermediate files. The supplied HTTP scripts
pass authorization headers to curl over stdin.

On success, exit zero and write exactly one JSON object to stdout:

```json
{"version":1,"windows":[{"pool":"primary","window":"five_hour","models":["native-model"],"remaining_percent":75,"reset_at":1790010000,"observed_at":1790000000}]}
```

Missing percentages and reset times are unknown, not zero. Rust validates the
closed schema, model membership, ranges and clocks before storing anything.
Account/source identities are bound by Rust, never accepted from script output.
A token echoed into an output string or key rejects the entire result.

The whole execution deadline is 30 seconds by default (configurable 1–300).
Input/stdout are bounded to 2 MiB; stderr to 64 KiB. Rust observes the actual
script PID independently of its pipes, cleans the verified process group and
captured descendants, and drains output with a short bound. Timeout, cancellation,
nonzero exit, missing executable, oversized/malformed output, and unconfirmed
cleanup are failures. Reports contain fixed classifications, never raw output
or script error text. The last good samples keep their original timestamps.

## Polling and supplied scripts

Collection runs once per global account/source per round. Aliases with conflicting
command settings are rejected before any execution. Failures apply the shared
durable 60-second exponential backoff, capped at 900 seconds. Scripts control
HTTP retries within their execution deadline; Rust does not interpret HTTP status
codes or provider-specific retry headers. Reads such as models, limits and
capacity_order use persisted observations and never execute collectors.

The native release includes the external files under collectors/. The source
repository keeps them under scripts/collectors/:

* glm.sh: GLM Coding Plan HTTP usage; requires Bash, curl and jq.
* claude.sh: Anthropic OAuth HTTP usage; requires Bash, curl and jq.
* codex.sh: Codex app-server account/rateLimits/read; requires Bash, jq and the
  configured Codex executable. The temporary home is MCP-free and links only the
  selected native login. No model turn is started.

Each script uses its neighboring .jq files to normalize provider formats. The
HTTP scripts accept an optional quota URL as their first argument. Copy the whole
collectors directory when customizing it; Rust neither embeds nor chooses these
files based on a provider name. Their HTTP endpoints and response rules live
only in those external files. Unknown Codex buckets are not assigned to models.

A provider may explicitly map authoritative native exhaustion window names to
physical pools through collector.exhaustion_windows. For the supplied Claude
collector: exhaustion_windows = { five_hour = "primary", seven_day = "secondary" }.
These mappings apply to all eligible model lanes of that account. Omit a mapping
when the native signal cannot safely establish a physical pool; the current run
still handles quota exhaustion, but no guessed shared latch is written.

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
  900 s TTL past the round). If disjoint models report fresh zero for the same
  physical window, its one shared latch keeps the later known reset; an
  unknown reset remains unknown. This can conservatively block a newly
  exhausted model until the older restriction expires, never release an older
  model early.
* Freshness, unknown data, and collection failure are distinct states; a
  failed collector never replaces good evidence, and read-only advice never
  fetches network or reserves.

## Pool membership consumed by selection

A physical pool id (`primary`, `secondary`, `credits`, `codex`,
`model:<key>`) is never a model name. `record_quota_snapshot` stores, on
every persisted window row, the exact model lanes (native alias, else model
id — the names each collector unit is built with) that window governs, in
`payload_json.models`, per window rather than per pool, so a narrower window
never inherits every model of its pool. Account selection, the `models` and
`capacity_order` views and admission reservations consume that membership:
a window governs a model only when its newest row names that model's lane.
While an exhaustion latch remains active, a partial unknown, positive, or
disjoint zero observation cannot shrink that window's recorded membership;
the prior governed lanes remain until reset or fresh evidence covers them
all. Other physical windows keep
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
