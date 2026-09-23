# Provider contract

Standalone public semantics for provider orchestration as of store schema
v17. The canonical type definitions live in
`crates/agent-run-domain/src/catalog.rs`; quota and session consumers import
them from `agent_run_domain::catalog` rather than redefining business types.

## C1 Catalog

* Harnesses are exactly `codex` and `claude-code` (`HarnessId`).
* A provider (`ProviderDefinition`) binds one harness, one native or custom
  connection, one auth family, an explicit model list, and account bindings.
  Provider ids are arbitrary within the identifier grammar. Custom endpoints
  require HTTPS; an explicit custom-connection flag allows HTTP only for
  loopback fixtures. URL userinfo,
  fragments, unsupported schemes, and harness/auth-family mismatches fail
  validation.
* Provider models are explicit: model id, optional native model, fixed textual
  settings, allowed parameter values, plain-text recommendations, and typed
  preserved restrictions. There is no model intelligence, classification, or
  scoring in the catalog.
* V2 capacity source kinds are `codex_appserver`, `lua`, and `none`. A
  `lua` source must additionally bind an explicit `CollectorBinding`: a
  first-party collector script identity plus one to eight exact HTTPS (or
  fixture loopback HTTP) origins. Neither a script identity nor a
  credential identity is ever inferred from a provider's name; an unknown
  script identity is a typed collection failure, not a guess.
* A registered account (`AccountRecord`) has an immutable global opaque
  `AccountId`, an auth family, a secret *reference*, and a status. A provider
  binding (`ProviderBinding`) carries a validated `AccountLabel`, an optional
  model subset that must be contained in the provider's models, and a positive
  finite multiplier defaulting to 1. A binding is valid only when its account
  is registered and the account auth family equals the provider auth family.
* The same global account id may be bound under several providers or labels.
  Those aliases denote one physical account: one quota pool and one set of
  reservations, never summed. Separate account ids cannot register the same
  canonical auth-family/secret-reference storage identity.
* Wire deserialization runs the same whole-catalog checks as construction,
  including duplicate ids, model subsets, registration, and auth families.
* No secret bytes exist in any catalog type. Credentials are named by
  `SecretRef`; registration checks its typed native/named/environment/file/
  Keychain location. The Rust-only [account registry](account-registry.md)
  resolves custom request headers at use time.

Fixtures: `tests/contracts.rs` builds a minimal two-alias catalog
(`codex-plus`, `codex-pro` over one `gpt-5.1` model and one global account).

## C2 Authority versus attempt lease

* `ResolvedLaunchAuthority` is the immutable authority frozen at admission:
  provider, harness, native/custom connection, explicit model and effort, profile,
  workdir, canonical resolved role payload, required tool-assets digest, and
  frozen eligible account scope. The payload carries the operative prompt,
  write/network/read grants, skill and MCP permissions, and required
  constraints. Its initial auth reference records admission context, while
  each attempt's validated credential lease supplies the active account.
  Required constraints are obligations, not grants.
  Admission must produce it from the resolved role plan and sealed assets.
  Every launch/retry must reconstruct it with
  `role_plan::role_from_authority` with a digest computed from the sealed
  asset bytes. This validates the role payload, authority revision, and asset
  proof. Consumers use those frozen role/tool semantics and bind the current
  attempt lease for account auth without changing the frozen grants.
  Domain validation checks the canonical role revision without depending on
  config or core. Raw harness-native settings cannot override these fields.
* `AttemptCredentials` is the mutable per-attempt lease: exactly one selected
  enabled global account plus a coupled `SecretHandle` minted from that
  account's catalog record and provider/model binding. Lease fields are
  private; callers cannot pair account A with account B's reference. The
  handle implements no serde trait. References name nonsecret storage
  locations; credential bytes never enter the catalog or lease.

## C3 Quota admission contracts

* `NormalizedQuotaSnapshot` carries collector facts per host-bound account
  and explicit model: each physical key owns its source-labelled,
  provider-reported windows with remaining percent, optional reset,
  observation time, and validity. Unknown values remain absent. Validation
  bounds membership and rejects foreign keys, duplicate windows, invalid
  percentages, and nonfinite times. Repeated physical keys across models
  must have identical window sets and facts, regardless of window order.
  Scoring stays outside this DTO and the store.
* `QuotaCandidateSet` is immutable and read-only: provider, explicit model,
  auto or pinned intent, deterministically ordered rank groups, and the
  committed capacity revision the ordering was computed against. Each
  candidate carries the exact account-bound physical key set that admission
  must reserve with the attempt. Rank is a producer-assigned ordinal: lower
  groups win, and only equal ranks use active-count then stable global account
  id as admission tie-breaks. Producing and ordering the set (scoring)
  belongs to the quota side. Unknown capacity
  candidates follow known usable capacity; missing data is never invented.
* Transactional admission (session side) re-validates the committed revision
  in `BEGIN IMMEDIATE`, filters by current account status and reservations,
  and persists the chosen account plus the attempt. The store exposes only the
  raw facts it owns: `Store::quota_capacity_revision` and
  `Store::active_attempt_counts` and `Store::active_reservation_counts`;
  matching `*_in` methods read through the admission transaction;
  it performs no scoring. One global monotonic `quota_capacity_revision`
  covers the entire scored snapshot. A quota producer advances it in the same
  transaction as each relevant quota mutation, then reads that value for its
  candidate set; admission compares that same singleton in its transaction.
  A change to either of two pools invalidates a set scored before it.
* Typed verdicts (`QuotaAdmissionError`) with stable machine names:
  `selection_stale` (revision moved; the quota consumer recomputes
  outside the transaction, at most three retries), `selection_busy` (retry budget spent,
  no admission attempted), `no_eligible_account` (no candidate currently
  registered, enabled, and in scope), and `quota_exhausted` (repeats an
  authoritative structured provider exhaustion fact only). None may
  masquerade as another.
* Pinned requests select exactly the requested account and disable failover;
  auto requests select among the ordered candidates. The intent persists with
  the request for replay: the original auto/pinned intent is stored separately
  from the resolved account.

### Initial provider start handoff

`ProviderStartRequest` is the strict new input: configured provider id,
explicit model, canonical profile, task and workdir, optional provider-local
account label, and the established start options. It rejects a `runtime` field
or caller-supplied quota candidates. `Service::start_provider_trusted` accepts
this request plus a **trusted Rust** `QuotaCandidateSet`, admits one logical
agent and first attempt atomically, then launches the normal supervisor only
for a newly created admission. `Service::admit_provider_trusted` exposes the
same admission without spawning for offline supervisor fixtures. Neither is a
public CLI/MCP/JSON-RPC quota-candidate endpoint.

### Mechanical account choice

`Service::admit_provider` (and `Service::start_provider`, which then hands a
newly created attempt to the provider-aware supervisor) is the ordinary
consumer. The orchestrator still names provider, model, effort and profile
explicitly; only the account is chosen here, mechanically:

1. **Replay first.** A repeated `request_id` with the identical request
   returns the original admission (`created=false`) before config, account
   registry or quota is read, so later config, provider, account or quota
   changes cannot alter it; a different request under that id is
   `RequestConflict`. Replay never reserves again or spawns a supervisor.
2. The current valid v2 config (exact-byte revision; an invalid edit keeps
   the last valid one) and the registry resolve the launch authority once;
   that revision is frozen into the admitted row and used for execution.
3. `provider_candidates` ranks accounts from persisted evidence outside
   every write transaction: known usable before unknown, exhausted,
   disabled and model-ineligible accounts excluded whatever their
   multiplier; a request account label is a pin with no failover. No token
   budget is derived and no reset credit is consumed.
4. `Store::admit_provider` revalidates under `BEGIN IMMEDIATE` (revision,
   current registry status, caps) and breaks ties only among equal ranks by
   active load (shared by every alias of a global account), then id.
5. On `selection_stale` the consumer recomputes from persisted facts and
   resubmits: **one initial selection plus at most three recalculations**
   (`PROVIDER_STALE_RETRIES = 3`, four submissions in total). A fourth stale
   submission returns `selection_busy` with no agent, attempt or
   reservation. There is no sleep or polling, and every other error returns
   unchanged — never relabelled as exhaustion.

`agents.identity_json.provider_identity_version=2` and its request hash prove
new provider identity independently of the historical `runtime` read-model
projection. The row freezes the original request, exact config byte digest,
validated credential-free config, its normalized snapshot digest, resolved
role grants, model and eligible account scope. Later TOML edits do not change
that admitted launch.
The supervisor seals the generated asset digest before spawn and reads the
selected account only from its owned attempt. It plans credentials through the
provider adapter for that account, binds events/messages to the real attempt,
and releases physical keys only after verified cleanup or a certified
never-spawned result. Replays return the first admission before consulting
changed configuration or quota state. This first path does not switch accounts
automatically; historical version-one rows retain their existing read path.
Claude Messages launches request partial stream events so live assistant text
can be journaled while the engine is running.

## C4 Attempts and ownership (schema v17)

* One logical agent id keeps its existing attempts/events/messages tables.
  Schema v17 adds:
  * `agents.selection_intent` (`auto`/`pinned`) and
    `agents.requested_account_id` — the original request intent, kept separate
    from any resolved account.
  * `attempts.selected_account_id`, `phase`, `process_identity`,
    `process_birth_time`, `cleanup_proof_json`, `session_facts_json` —
    per-attempt account selection, phase (e.g. `account_switch`), verified
    process identity, and cleanup/session proof facts.
  * `provider_accounts` — the account registry (global id, auth family,
    secret reference, status).
  * `capacity_samples.account_id` and `quota_key` — nullable together.
    Historical samples retain both NULL and their original `runtime`/
    `target` meanings. New provider samples reference a registered global
    account and a physical key beginning with `<account_id>::` plus a
    nonempty lane. Consumers read these explicit fields and never infer an
    account from legacy target/runtime text.
  * `quota_exhaustion` — durable physical-window facts independent of sample
    retention. The primary key is `(account_id, quota_key, source, window_id)`;
    rows also carry `observed_at`, optional `reset_at`, and optional
    `collector_revision`. The account is registered, the key belongs to it,
    and source/window ids are nonblank. `source` is a stable collector identity;
    code revision is non-key metadata and cannot fork a latch. Deleting old
    capacity samples cannot delete exhaustion rows.
  * `quota_capacity_revision` — one committed monotonic revision for the
    whole scored snapshot.
  * `attempt_quota_keys` — the exact physical key set consumed by each
    selected model/attempt. The insert guard ties keys to the selected
    `AccountId`; active reservation counts join this set to owned attempts.
    Alias labels share the same keys, while distinct lanes remain distinct.
    A legacy NULL account may be bound once; a non-NULL selection and
    persisted quota keys cannot be changed to another identity.
* The quota writer owns atomic exhaustion set/clear decisions and global
  revision advancement. A positive clear requires fresh evidence no older
  than the latched observation; this schema does not score or clear facts.
* `attempts.ownership_active` (default 0) marks the attempt that owns the
  agent's execution slot. Ownership spans the entire orchestrated lifecycle —
  claim (prepared/starting), running, account switch, and cleanup-pending —
  and must be released (set to 0) by the session consumer only after verified
  process teardown and cleanup proof. The schema does not yet enforce that
  proof. The partial unique index `idx_attempts_one_active`
  (`ON attempts(agent_id) WHERE ownership_active = 1`) enforces at most one
  owned attempt per agent across all those states. Legacy rows and current
  release inserts default to 0 and are therefore unaffected.
* Historical messages with a NULL attempt id remain readable, and current
  inserts of `running` attempt 1 remain valid, until consumers transition.
* Schema v17 is unpublished preparation. The committed `current-v17.sqlite`
  fixture is rebuilt from `historical-v16.sqlite` plus final migration 017.
  Intermediate local dev17 homes from earlier schema drafts are disposable;
  no live repair or migration 018 is implied.

## C5 Historical decoding

* Stored `request_json` blobs and sealed artifacts are never rewritten. The
  decoder (`decode_legacy_request`) retains the raw runtime spelling,
  including arbitrary names such as `glm` and `main`. Recorded adapter
  evidence or a verified migration-map entry supplies provider and harness.
  Fixed Codex/Claude spellings reject conflicting adapter evidence; neither
  spelling invents a provider identity. Without evidence, history remains
  readable with unresolved provider/harness and resume must refuse.
* Resume of an old run
  whose authority fields did not stay unchanged must block explicitly; unsafe
  permissive resumes are out of the question. Migration `017` is additive
  only: it adds nullable columns and new tables, requires no credential
  inspection, and leaves every historical row byte-identical.

## Boundary tests

Executable boundary coverage lives beside the contracts:

* alias identity, auth-family rejection, pinned vs auto, typed verdicts,
  authority/lease separation, legacy decoding:
  `crates/agent-run-domain/tests/contracts.rs`
* migration equivalence for every historical version, legacy reads, ownership
  exclusivity across states, cross-account key rejection, physical reservation
  counts, and global revision facts:
  `crates/agent-run-store/tests/state_migrations.rs`

## C6 Public catalog reads

`models` and `capacity_order` (shared registry `assets/tools.json`, one
dispatch for CLI, broker socket and MCP) are read-only over one validated
config revision from the service's exact-byte last-valid cache, the canonical
role files, and one committed quota read. They never collect quota, call a
harness, reserve, consume reset credits, write samples or start agents.

* `capacity_order` returns providers, never provider/account pairs, ranked
  by `capacity::provider_ranking` (known before unknown; exhausted, disabled
  and ineligible accounts excluded). Each offered model keeps its own
  `status` and `best_priority`; the optional exact `model` filter ranks
  providers by that model alone.
* `models` adds the explicit offerings (`native_model`, `params`,
  `allowed_params`, `restrictions`, `recommendations`), provider
  recommendations, harness and connection kind, canonical role grants, and
  for each offering the roles admission would accept: the role loads as a
  canonical provider role, its plan resolves, and the effective policy with
  the model's restrictions admits on that harness — the same checks a start
  applies. Exact `provider`/`profile`/`model` filters; unknown values are
  validation errors.
* Results carry `config_revision`, `capacity_revision`, `observed_at`, and
  (`models`) `roles_sha256`. No account id, label or secret reference is
  emitted. The orchestrator chooses provider, model, effort and profile;
  there is no automatic model choice or ability score.
