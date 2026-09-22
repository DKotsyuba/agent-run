# Provider contract

Standalone public semantics for provider orchestration as of store schema
v17. The canonical type definitions live in
`crates/agent-run-domain/src/catalog.rs`; quota and session consumers import
them from `agent_run_domain::catalog` rather than redefining business types.

## C1 Catalog

* Harnesses are exactly `codex` and `claude-code` (`HarnessId`).
* A provider (`ProviderDefinition`) binds one harness, one protocol endpoint,
  one auth family, an explicit model list, and account bindings. Provider ids
  are arbitrary within the identifier grammar.
* Provider models are explicit: model id, optional native model, textual
  parameters (e.g. effort choices), plain-text recommendations, and explicit
  preserved restrictions. There is no model intelligence, classification, or
  scoring in the catalog.
* A registered account (`AccountRecord`) has an immutable global opaque
  `AccountId`, an auth family, a secret *reference*, and a status. A provider
  binding (`ProviderBinding`) carries a provider-local label, an optional
  model subset that must be contained in the provider's models, and a positive
  finite multiplier defaulting to 1. A binding is valid only when its account
  is registered and the account auth family equals the provider auth family.
* The same global account id may be bound under several providers or labels.
  Those aliases denote one physical account: one quota pool and one set of
  reservations, never summed.
* No secret bytes exist in any catalog type. Credentials are named by
  `SecretRef` (a storage reference such as a keychain label); the real
  resolver is a separate deliverable.

Fixtures: `tests/contracts.rs` builds a minimal two-alias catalog
(`codex-plus`, `codex-pro` over one `gpt-5.1` model and one global account).

## C2 Authority versus attempt lease

* `ResolvedLaunchAuthority` is the immutable authority frozen at admission:
  provider, harness, protocol endpoint, explicit model and effort, profile,
  workdir, granted constraints, tool-assets digest, and the frozen eligible
  account scope. Raw harness-native settings cannot override these owned
  fields.
* `AttemptCredentials` is the mutable per-attempt lease: exactly one selected
  global account plus a `SecretHandle`. The handle implements no serde trait,
  so credential references cannot be serialized into configuration, the
  database, snapshots, or logs.

## C3 Quota admission contracts

* `QuotaCandidateSet` is immutable and read-only: provider, explicit model,
  auto or pinned intent, deterministically ordered candidates, and the
  committed capacity revision the ordering was computed against. Producing and
  ordering the set (scoring) belongs to the quota side.
* Transactional admission (session side) re-validates the committed revision
  in `BEGIN IMMEDIATE`, filters by current account status and reservations,
  and persists the chosen account plus the attempt. The store exposes only the
  raw facts it owns: `Store::quota_capacity_revision` and
  `Store::active_attempt_counts`; it performs no scoring.
* Typed verdicts (`QuotaAdmissionError`) with stable machine names:
  `selection_stale` (revision moved; quota recomputes outside the
  transaction, at most three retries), `selection_busy` (retry budget spent,
  no admission attempted), `no_eligible_account` (no candidate currently
  registered, enabled, and in scope), and `quota_exhausted` (repeats an
  authoritative structured provider exhaustion fact only). None may
  masquerade as another.
* Pinned requests select exactly the requested account and disable failover;
  auto requests select among the ordered candidates. The intent persists with
  the request for replay: the original auto/pinned intent is stored separately
  from the resolved account.

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
  * `quota_capacity_revisions` — the committed capacity revision per physical
    quota key (`PhysicalQuotaKey` = global account + physical lane).
* `attempts.ownership_active` (default 0) marks the attempt that owns the
  agent's execution slot. Ownership spans the entire orchestrated lifecycle —
  claim (prepared/starting), running, account switch, and cleanup-pending —
  and is released (set to 0) only after verified process teardown and cleanup
  proof. The partial unique index `idx_attempts_one_active`
  (`ON attempts(agent_id) WHERE ownership_active = 1`) enforces at most one
  owned attempt per agent across all those states. Legacy rows and current
  release inserts default to 0 and are therefore unaffected.
* Historical messages with a NULL attempt id remain readable, and current
  inserts of `running` attempt 1 remain valid, until consumers transition.

## C5 Historical decoding

* Stored `request_json` blobs and sealed artifacts are never rewritten. The
  decoder (`decode_legacy_request`) maps historical runtime spellings to
  catalog identities read-only:
  * `codex`, `codex_appserver` → provider `codex`, harness `codex`
  * `claude`, `claude_code`, `claude-code` → provider `claude-code`, harness
    `claude-code`
* Unknown runtimes are a typed refusal, never a guess. Resume of an old run
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
  exclusivity across states, and revision facts:
  `crates/agent-run-store/tests/state_migrations.rs`
