# Inventory: durable state and domain primitives (WP prefix ST)

> **Historical analysis — the addresses below are not current.** This inventory
> records the state of the port at the time it was written, when the Rust code
> was a single `rust/` crate. That tree no longer exists; the port is now the
> `crates/` workspace, and the parity figures quoted here were superseded long
> ago. The addresses are kept verbatim because rewriting them would attach an
> old measurement to code it never described. For what is covered today, see
> `migration/status.md`, `migration/baseline/test-map.csv` and
> `migration/evidence/qualification-scope.md`.

Scope: `src/agent_run/state/*`, `domain.py`, `errors.py`, `paths.py`,
`process_identity.py`, `verify.py`, `launch_evidence.py` vs
`rust/src/state/mod.rs` (+ `schema.sql`), `domain.rs`, `error.rs`, `fs.rs`,
`verify.rs`, `process.rs`. Excludes `state/capacity.py` and
`state/delivery.py` (separate agent); their `StateStore` wrapper methods are
listed for completeness only.

## 1. Summary

- **Schema is a genuine match.** `rust/src/state/schema.sql` is
  structurally identical to `src/agent_run/state/schema.sql` (same 16
  tables, columns, checks, indices, `PRAGMA user_version=16`); no diff found
  beyond formatting. This is the strongest ported piece of the area.
- **Overall behavioral parity estimate: ~35%.** The pieces that exist
  (admission, simple run/finish, transcript, commands, basic answer
  verification, process-identity primitives) are close to correct. Whole
  subsystems Python relies on for a working daily install —
  reconciliation, resume lineage, run-stats extraction, orchestrator
  binding/context receipts, startup-ownership/handoff safety, launch
  bootstrap diagnostics, and the full completion-decision policy — have
  **no Rust implementation in this area** or only a crude, non-equivalent
  stand-in elsewhere.
- **Biggest single blocker for "drop-in replacement": no forward schema
  migration.** `Store::connect` (`rust/src/state/mod.rs:170-176`) refuses
  to open any existing database whose `user_version` is not exactly 16.
  An owner's real `~/.agent-run/state.db` created by an older Python
  release (schema 1-15) **cannot be opened at all** by this Rust binary;
  it must be pre-migrated by the Python code first. Porting the migration
  chain is already assigned as WP ST-MIG; until it lands, the Rust binary
  is not a drop-in replacement for any existing installation.
- **Second blocker: the completion-decision policy is drastically
  simplified.** `verify::completion` (`rust/src/verify.rs:147-163`) covers
  only 3 of Python `verify_completion`'s ~7 branches (no `ENGINE_VANISHED`,
  no distinct `TIMED_OUT` handling, no silence-note text, no
  `NO_ANSWER`/`ANSWER_INCOMPLETE`/`ANSWER_PRESENT` evidence labels feeding
  `failure_text`). Every terminal agent's `failure_kind`/`failure_text`
  will diverge from Python in non-trivial stop scenarios.
- **Third blocker: reconciliation and resume lineage are unported** in
  the state module. `state::Store` has no `reconcile`/`reconcile_reaped`
  method; a much cruder, non-fair, non-paginated stand-in lives in
  `rust/src/service.rs:370` (another agent's file) and does not implement
  Python's verdict/proof contract (`checked_supervisor_proof`,
  `src/agent_run/state/db.py:223-292`). `resume_lineage`/`latest_child`
  (`src/agent_run/state/resume.py`) have no Rust counterpart at all beyond
  the DB-level parent/child uniqueness constraint.
- Startup-ownership safety (`claim_startup`, `begin_supervisor_handoff`,
  the immutable-rewrite rule in `record_supervisor`) is reduced in Rust to
  a single one-shot `set_owner` (`rust/src/state/mod.rs:352-364`) with no
  deadline, no handoff extension, and a different (weaker) immutability
  check.

## 2. Module map

| Python module | Responsibility | Rust counterpart | Status | Missing/divergent behavior |
|---|---|---|---|---|
| `state/schema.sql` | 16-table durable schema, v16 | `state/schema.sql` (whole file) | **ported** | No diff found. |
| `state/migrations.py` + `migrations/*.sql` | Numbered migration chain 1→16, backup-before-migrate, schema lock | none | **missing** (assigned as WP ST-MIG, not detailed here) | Only fresh v16 creation exists in Rust; no upgrade path for an existing installation. |
| `domain.py` | `AgentStatus`/transitions, `AgentId`, `OrchestratorRef`, `StartRequest`, `Message`, `Outcome` value types | `domain.rs` (whole file) | **ported** | Transition table matches exactly (`domain.py:52-84` vs `domain.rs:109-135`). `Message`/`MessageRole` (`domain.py:228-251`) has no distinct Rust type; role set is inlined as a string allow-list in `Store::message` (`state/mod.rs:420`) — same 5 values, different shape, functionally equivalent. |
| `errors.py` | `AgentRunError` taxonomy: `BrokerUnavailable`, `ValidationError`, `StateTransitionError`, `PathEscapeError`, `SchemaMigrationRequired`, `AuthError`, `CapacitySourceError` | `error.rs` (whole file) | **partial** | No `PathEscapeError` equivalent (folded into `Validation`/`Integrity`); no `SchemaMigrationRequired` signal at all — Rust just hard-refuses (see Summary); `AuthError`/`CapacitySourceError` are other agents' concern. Rust adds `NotFound`/`Conflict`/`Capacity`/`Transition` variants Python expresses differently (via `ValidationError`/`StateTransitionError` subclasses raised inline) — needs reconciliation against the (not-mine) `service.py` public error mapping to confirm wire compatibility. |
| `paths.py` | `agent_run_home`, `agent_dir`/`create_agent_dir` (escape-checked), `runtime_skills_dir`, `state_db_path` | `fs.rs::home`/`private_dir`/`Dir` | **different-by-design, partial** | No dedicated `agent_dir`-equivalent function exists; the join `home.join("agents").join(id)` is duplicated ad hoc in `rust/src/service.rs:265`, `rust/src/supervisor.rs:116`, `rust/src/state/mod.rs:443` with no `_require_beneath`-style resolve+contain check (mitigated only by `AgentId`'s strict format regex). No `runtime_skills_dir` counterpart in this area (`config.rs:322 skills_dir` is a different, unchecked mechanism, not mine to fix). |
| `process_identity.py` | `ProcessState` (5 values), `observe_process`, `capture_process_birth` — thin psutil wrapper | `process.rs` (`Identity`, `ProcessState`, `inspect`, `observe`, `capture_process_birth`≈none) | **ported+extended** | Rust's `ProcessState` has an extra `NotStarted` variant (`process.rs:16-23`) Python's doesn't have (used when no pid at all, `process.rs:141-144`); behavior for the 5 shared variants matches. `process.rs` additionally implements `OwnedProcess`/`Cleanup`/group-kill logic (`process.rs:194-324`) that has no Python source in this area — that logic's Python counterpart is `lifecycle.py`'s `terminate_process_group`/`verify_process_group` (core-lifecycle agent's file, referenced here only as a dependency/boundary note). |
| `verify.py` | Answer sealing/verification: `AnswerProof`, `inspect_answer`, `load_answer_proof`, `read_answer_payload`, `verify_completion` (full decision policy), `silence_seconds` | `verify.rs::Proof/Sidecar/seal/read/error_only/completion` | **partial** | `seal()` has no Python counterpart (the coding agent itself writes the sidecar per `docs/runtime-contract.md`, not agent-run — `adapters/home.py:329` documents the contract but doesn't write it either; likely fine as a Rust-only test/production helper, needs confirming it's never invoked from adapter code with wrong semantics). `read()` folds Python's `inspect_answer`+`_load_answer_proof`+`_proof_mismatch` into one function but drops the typed error hierarchy (`AnswerMissingError`/`AnswerTamperedError`/`AnswerOversizedError`/`AnswerEncodingError`/`AnswerProofError`, `verify.py:62-83`) in favor of one `Error::Integrity` string — changes the wire `kind` from `ValidationError` (Python, since `AnswerError` extends `ValidationError`) to `AnswerIntegrityError` (Rust `error.rs:48`) for every answer-verification failure: **a protocol-visible divergence**. `completion()` is a materially smaller policy than `verify_completion` (`verify.py:561-637`) — see Summary. `silence_seconds`/`_silence_note` (`verify.py:525-543`) have no Rust equivalent; silence text never appears in Rust failure_text. |
| `launch_evidence.py` | Bootstrap pipe protocol: `preflight_executable`, `write_exec_failure`/`write_bootstrap_record`, `read_bootstrap_record`, `diagnose_bootstrap_failure`, `bootstrap_event_data`/`bootstrap_error_fields` | none found | **missing** | No file in `rust/src` implements a bootstrap-failure pipe protocol; grep for `bootstrap`/`exec_failure` across `rust/src` found nothing. A failed spawn (bad executable, permission denied) has no diagnosed path in Rust today — it would surface as a generic error instead of the structured `bootstrap_failed` event Python produces. |
| `state/db.py` | ~30 SQL helper functions: row/JSON marshalling, `checked_supervisor_proof`, `idempotent_agent`, `insert_event`/`insert_agent_row`, schema validation/init, `_private_path`, context-receipt encode/parse | scattered inline SQL in `state/mod.rs` | **partial** | Inline literal SQL duplicates some of this (insert_event≈`tx_event` `state/mod.rs:110-130`, `insert_agent_row`≈inline INSERT in `admit` `state/mod.rs:330`). No Rust equivalent of `checked_supervisor_proof` (`db.py:223-292`, needed for reconciliation), `_upsert_context_receipt`/`record_context_component_receipt` (`db.py:392-503`, context_receipts table is schema-present but never written by Rust), `_validate_schema`/`_table_shape` defensive schema check (`db.py:773-819`), or `_private_path` (`db.py:846-871`, `fs::private_dir` in Rust covers only the directory case, not `db.py`'s file-specific checks). |
| `state/store.py::StateStore` | ~45-method facade: admission, transitions, transcript, orchestrator binding, context receipts, startup ownership, reconciliation dispatch, capacity/delivery wrappers | `state::Store` (24 methods, `state/mod.rs`) | **partial** | See table below for a method-by-method map. |
| `state/reconciliation.py` | `reconcile_reaped_agent`, `reconcile_reaped_supervisor`, `reconcile_unowned_starting`, `reconcile_active_agents`, `_fair_rows` (paginated fair scan), `process_owner_identity` | none in state module; crude non-fair stand-in in `rust/src/service.rs:370` (not this area) | **missing** | No fairness/pagination, no verdict taxonomy (`alive`/`dead`/`identity_mismatch`), no use of `checked_supervisor_proof`'s immutable-ownership contract. See Summary. |
| `state/run_stats.py` | `_runtime_result_stats`, `_token_usage_stats`, `_resumed_token_usage_stats`, `_usage_stats`/`_resumed_usage_stats` (multi-attempt aware), `record_run_stats`, `backfill_run_stats`, `record_run_stats_best_effort` | inline subset in `Store::finish` (`state/mod.rs:494-504`) | **partial** | Rust extracts `input/output/cache_read/cache_write/reasoning/total_tokens`, `num_turns`, `cost_usd` directly from a flat `usage` value with simple non-negative/finite filters — matches only the simplest of Python's two source shapes (`_runtime_result_stats` vs `_token_usage_stats`, `run_stats.py:57-100`). No `ttft_ms`/`api_duration_ms` (schema columns exist, schema.sql:239-240, never populated by Rust). No resumed-lineage-aware aggregation (`_resumed_token_usage_stats`/`_resumed_usage_stats`, `run_stats.py:117-204`, which sum across a resume chain). No `backfill_run_stats` (`run_stats.py:289-316`) for repairing rows written before this feature existed. |
| `state/resume.py` | `ResumeLineage`, `_quiescent` (all ancestors dead check via `ProcessOps`), `resume_lineage`, `latest_child` | none (only DB-level parent/child uniqueness via `agents_parent_agent_id_unique` index) | **missing** | `admit()` (`state/mod.rs:298-317`) checks the immediate parent is terminal and has no existing child, matching part of the DB contract, but the read-side lineage walk/quiescence check (`resume.py:41-116`) that decides whether a *chain* is safe to resume from does not exist in Rust. |
| `state/start.py` | `AgentCreation`, `create_agent` (limited-admission with parent claim, startup-owner/deadline persistence) | `Store::admit` (`state/mod.rs:235-344`) | **partial** | Covers concurrency-limit check, parent-terminal/no-existing-child check, and request-id replay. Does **not** persist `startup_owner_identity`/`startup_owner_birth_time`/`startup_deadline_seconds` atomically at admission the way `create_agent_limited` does (`start.py` via `db.py` fields `startup_owner_pid_identity`, `startup_deadline_at`) — Rust's `admit` sets `startup_owner_pid_identity`/`startup_owner_birth_time` from `process::inspect(getpid())` only (`state/mod.rs:331-333`), with no deadline column write at all, so `startup_deadline_at` is always NULL from Rust-admitted rows. |
| `state/activity.py` | `context_agents` — recent-activity lookup for a session | none | **missing** | Not referenced anywhere in `rust/src`. |
| `state/diagnostics.py` | `DiagnosticSnapshot`, `diagnostic_snapshot` — doctor-style summary | none | **missing** | Not referenced anywhere in `rust/src`. |
| `state/__init__.py` | Re-export surface only | n/a | n/a | Nothing to port. |

### `StateStore` method map (`store.py`)

| Python method (line) | Rust equivalent | Status |
|---|---|---|
| `initialize`/`open`/`close`/`path` (83-104) | `Store::initialize`/`open` (mod.rs:132-138); no `close`/`path` needed (RAII, `home` field public) | ported |
| `create_agent`/`create_agent_limited` (106-168) | `admit` (235-344) | partial — see start.py row above |
| `replace_config_revision` (172-202) | none distinct; `update_identity` (365-374) sets both identity+revision together, no compare-and-swap of revision alone | missing |
| `has_pending_cancel` (204-216) | `cancel_pending` (563-565) | ported |
| `bind_orchestrator` (218-245) | none | missing |
| `find_orchestrator_session` (247-255) | none | missing |
| `record_context_receipt*` (259-322) | none | missing |
| `get_agent` (324-325) | `get` (212-221) | ported |
| `list_agents` (327-360) | `list` (571-603) | ported (session filter added in Rust, not in Python signature shown) |
| `events_revision` (362-368) | `revision` (566-570) | ported |
| `agent_projection` (370-451) | none (batched read query with delivery/evidence/cleanup/phase joins) | missing |
| `active_count` (453-455) | inline count in `admit` only, no standalone method | partial |
| `create_attempt`/`finish_attempt` (457-505) | inline single hardcoded `number=1` insert in `running` (389); no standalone/multi-attempt API | missing (multi-attempt/resume-retry unsupported) |
| `append_event` (507-528) | `event` (345-351) | partial — Rust drops `attempt_id`/`require_attempt` linkage |
| `append_message` (530-563) | `message` (409-428) | partial — Rust has no `resolve_message_storage` (large-message spool-to-file, `db.py:86-131`) |
| `transcript` (565-577) | `transcript` (604-633) | ported (pagination semantics differ slightly: byte-cap `more` logic) |
| `record_supervisor` (579-638) | `set_owner` (352-364) | partial — weaker immutability rule, no `heartbeat_at` write |
| `claim_startup` (640-697) | none | missing |
| `begin_supervisor_handoff` (699-751) | none | missing |
| `transition`/`_transition` (753-912) | ad hoc logic inline in `running`/`finish` only | partial (no general-purpose transition entry point; terminal-cancel race handling in `transition` 771-817 not replicated) |
| `expire_unbound_deliveries` (828-842) | none (delivery agent's area) | dependency |
| `reconcile`/`reconcile_reaped` (914-end) | none in state module | missing |
| `enqueue_command`/`claim_command`/`complete_command` (1007-1074) | `enqueue`/`pending_commands`/`command_done` (508-562) | ported |
| capacity methods (1075-1183) | delegate to `state/capacity.py` | out of scope (capacity agent) |
| delivery methods (1183-end) | delegate to `state/delivery.py` | out of scope (delivery agent) |

## 3. Public contract surface

| Surface | Python source | Rust source | Parity |
|---|---|---|---|
| Schema tables/columns/indices, v16 | `state/schema.sql:1-275` | `state/schema.sql:1-109` | **ported**, exact match found by full-text comparison |
| Answer proof sidecar JSON shape (`kind`,`media_type`,`proof_version`,`answer`,`bytes`,`sha256`) | `verify.py:376-391` (`answer_proof_document`) | `verify.rs:21-29,50-57` (`Sidecar`) | ported (field set and values match) |
| `.answer-format` marker content `b"2\n"` | `verify.py:50` | `verify.rs:59` (`b"2\n"`) | ported |
| Legacy sentinel frame `\n<<<agent-run:complete>>>\n` | `verify.py:19,59` | `verify.rs:12` | ported |
| `MAX_ANSWER`=16MiB, inline limit=128KiB | `verify.py:53-56` | `verify.rs:10-11` | ported |
| Answer read error taxonomy → wire `kind` | `verify.py:62-83` all subclass `ValidationError` | `error.rs:48` maps all to `AnswerIntegrityError` | **divergent** (wire-visible) |
| Agent id format `ag-YYYYMMDD-HHMMSS-<10 lowercase hex>` | `domain.py:96-114` | `domain.rs:38-63` | ported |
| Status/lifecycle values (9) and transition table | `domain.py:23-84` | `domain.rs:67-145` | ported (exact match) |
| Process-identity fields (`pid`,`ppid`,`group`,`birth`,`token`,`zombie`) | `process_identity.py` uses psutil's `create_time()` only (no ppid/group/token/zombie captured) | `process.rs:5-13` (`Identity`) | **different-by-design, richer in Rust** — Python's stored `supervisor_birth_time` is a bare float; Rust's `token` field (`linux:<boot_id>:<ticks>` / `darwin:<start_sec>:<start_usec>`) is a Rust-only addition with no Python-side column consumer to compare against — confirm the DB's `supervisor_identity`/`identity_json` free-text fields are compatible free-form strings on both sides (**unverified** without reading the not-mine `lifecycle.py`/`supervisor.py` writers) |
| Reconciliation verdict values `alive`/`dead`/`identity_mismatch` and failure kinds `supervisor_dead`/`supervisor_identity_mismatch` | `db.py:223-292`, `store.py:914-971` | not present in state module | **missing** |
| Run-stats fields (18 columns) | `schema.sql` `run_stats` table + `run_stats.py:57-233` | `state/mod.rs:500-504` populates 11 of 18 (misses `ttft_ms`, `api_duration_ms`; ignores resumed-lineage aggregation) | **partial** |
| Error → RPC code mapping | not in this area (`service.py`, not mine) | `error.rs:60-66` (`rpc_code`) | unverified cross-area; flag for cross-checking with the transport agent |

Transcript/raw-stream spool layout: Python's `resolve_message_storage`
(`state/db.py:86-131`, referenced from `store.py:540-545`) spools oversized
message content to a file under the agent directory and stores a `raw_ref`
pointer instead of inline content when a message exceeds a size threshold.
Rust's `Store::message` (`state/mod.rs:409-428`) always stores `text`
inline and accepts an already-computed `raw_ref` from the caller — it does
not itself implement the oversized-message spool/threshold decision. This
is a **missing** behavior, not just unverified: no spool-writing code was
found anywhere under `rust/src` (grep for spool/oversize found nothing).

## 4. Test mapping

| Python test file (lines) | Behaviors covered | Rust test coverage |
|---|---|---|
| `test_domain.py` (151) | AgentId format, transitions, StartRequest/OrchestratorRef validation | `rust/tests/domain_config.rs` (295 lines, mixed with config tests) — likely overlapping, not line-verified |
| `test_paths.py` (48) | `agent_run_home`, `agent_dir` escape checks | none found |
| `test_process_identity.py` (41) | `observe_process` alive/dead/reused/denied/unknown | `rust/tests/process_identity.rs:3,24` (2 tests: reuse distinction, unproven-identity-not-death) — partial overlap |
| `test_verify.py` (251) | Answer proof roundtrip, tampering, legacy frame, `verify_completion` policy | `rust/tests/verification.rs` (182 lines, 15 tests) — covers proof/roundtrip/tamper/legacy/symlink well; only 1 test (`completion_requires_answer_and_process_cleanup`) touches the completion policy, none exercise `ENGINE_VANISHED`/timeout/silence-text branches |
| `test_reconciliation.py` (367) | Fair-scan reconciliation, verdicts, proof mismatches | none |
| `test_resume.py` (790) | Resume lineage, quiescence, adapter resume flows | `rust/tests/state.rs:179` (`resume_lineage_has_one_child_and_a_stable_root`, 1 test) — covers only the DB uniqueness constraint, not the 790-line Python behavior surface |
| `test_resume_adapters.py` (112) | Adapter-level resume wiring | none (adapter-area, reference only) |
| `test_run_stats.py` (276) | Token/cost extraction, backfill | none |
| `test_state_db.py` (139) | `db.py` helpers (row marshalling, schema validation) | none directly; partially exercised incidentally by `state.rs` |
| `test_state_migrations.py` (662) | Migration chain 1→16 | out of scope here (ST-MIG) |
| `test_state_outbox.py` (525) | Delivery/capacity `StateStore` wrapper methods | out of scope (capacity/delivery agent) |
| `test_state_store.py` (850) | Full `StateStore` surface | `rust/tests/state.rs` (278 lines, 12 tests) — covers admission/replay, concurrency cap, terminal immutability, commands, transcript cursor, delivery-on-bind, backup, version refusal; does **not** cover orchestrator binding, context receipts, startup ownership/handoff, `agent_projection`, config-revision CAS, or reconciliation |

## 5. Work packages

| ID | Title | Blocking/Later |
|---|---|---|
| ST-MIG | Migration chain 1→16 (already assigned elsewhere) | blocking |
| ST-1 | Reconciliation engine | blocking |
| ST-2 | Startup ownership, handoff, and general transition API | blocking |
| ST-3 | Completion decision policy (`verify_completion`) | blocking |
| ST-4 | Orchestrator binding and context receipts | blocking |
| ST-5 | Resume lineage read path | blocking |
| ST-6 | Multi-attempt support + oversized-message spool | blocking |
| ST-7 | Launch bootstrap diagnostics | blocking |
| ST-8 | Run-stats full extraction + backfill | later |
| ST-9 | Diagnostics/activity read helpers | later |
| ST-10 | Centralize `agent_dir`/path-escape helper | later |

---

### ST-1: Reconciliation engine
- **Implement**: Port `src/agent_run/state/reconciliation.py:23-332` (`_fair_rows`, `reconcile_reaped_agent`, `reconcile_reaped_supervisor`, `reconcile_unowned_starting`, `reconcile_active_agents`, `process_owner_identity`) and `state/db.py:223-292` (`checked_supervisor_proof`) and `state/store.py:914-1006` (`StateStore.reconcile`/`reconcile_reaped`) into `rust/src/state/mod.rs` as `Store::reconcile`/`Store::reconcile_reaped`, using `crate::process::observe`/`inspect` (already present, `rust/src/process.rs:141-170`) for liveness proof.
- **Depends on**: none within this area; the caller (currently a crude stand-in at `rust/src/service.rs:370`) belongs to the core/service agent — coordinate so `service.rs::reconcile` switches to calling the new `Store` methods instead of its inline logic.
- **Acceptance**: new `rust/tests/state.rs` (or a new `reconciliation.rs`) tests porting the shape of `tests/test_reconciliation.py`: dead-supervisor verdict transitions to `Lost` with `failure_kind="supervisor_dead"`; identity-mismatch verdict requires differing finite birth times and transitions with `failure_kind="supervisor_identity_mismatch"`; stale/incomplete proof raises without mutating; already-terminal agent returns `false`/no-op; fair pagination doesn't starve later rows across repeated calls.
- **Est. LOC**: ~450.

### ST-2: Startup ownership, handoff, and general transition API
- **Implement**: Port `state/store.py:172-202` (`replace_config_revision`), `640-751` (`claim_startup`, `begin_supervisor_handoff`), and generalize `579-638` (`record_supervisor`'s exact immutable-rewrite rule) plus `753-912` (`transition`/`_transition`, including the terminal-cancel race in `transition:771-817`) as new/rewritten `Store` methods, replacing/augmenting the current `set_owner` (`state/mod.rs:352-364`) and the transition logic embedded ad hoc in `running`/`finish`.
- **Depends on**: none.
- **Acceptance**: tests for: startup claim is immutable and deadline-bounded (past-deadline claim fails); handoff extends the deadline only when identity matches and no supervisor is bound yet; `record_supervisor`'s one-shot "own group becomes the verified engine group" rewrite is the *only* allowed post-set change; `transition` to `SUCCEEDED`/`TIMED_OUT` while a `pending` cancel command exists is redirected to `CANCELLING`→`CANCELLED` and completes the command with `reason="terminal_cancel"`, matching `store.py:771-817`.
- **Est. LOC**: ~500.

### ST-3: Completion decision policy
- **Implement**: Replace `rust/src/verify.rs:147-163` (`completion`) with a full port of `src/agent_run/verify.py:525-637` (`silence_seconds`, `_silence_note`, `_with_answer`, `verify_completion`), and port the read-side classification `inspect_answer`/`AnswerProof`/`_inspect_proof_sidecar` (`verify.py:134-330`) as a Rust `AnswerProof` type distinct from the sealing `Proof`, since `verify_completion` needs `evidence`/`complete` classification independent of a pre-known expected hash.
- **Depends on**: none.
- **Acceptance**: port the shape of `tests/test_verify.py`'s completion-policy tests: group-survived overrides everything; cancel/timeout stop reasons map to `Cancelled`/`TimedOut` with correct `failure_kind=evidence` and `failure_text` silence note; engine-vanished (`session_outcome is None`) maps to `Failed`/`engine_vanished`; success without a complete answer downgrades to `Failed`/`<evidence>`.
- **Est. LOC**: ~350.

### ST-4: Orchestrator binding and context receipts
- **Implement**: Port `state/store.py:218-322` (`bind_orchestrator`, `find_orchestrator_session`, `record_context_receipt`, `record_context_receipt_for_ref`, `record_context_components_for_ref`) and `state/db.py:392-503` (`_upsert_context_receipt`, `encode_context_components`, `parse_context_components`, `record_context_component_receipt`) as new `Store` methods. Note `admit` (`state/mod.rs:246-256`) already creates/upserts `orchestrator_sessions` rows inline for the *admission-time* case; this WP covers the separate *late-binding* path (an agent started without an orchestrator ref, bound afterward) which currently has no Rust entry point at all, including moving `deliveries` rows out of `waiting_binding` on bind.
- **Depends on**: delivery agent for the `waiting_binding`→`pending` transition semantics (read `state/delivery.py` for where `waiting_binding` rows originate — not found in the files read for this inventory).
- **Acceptance**: binding is idempotent for the same ref, rejected for a conflicting rebind; context receipt compare-and-store reports exactly the changed component names.
- **Est. LOC**: ~300.

### ST-5: Resume lineage read path
- **Implement**: Port `state/resume.py:28-134` (`ResumeLineage`, `_quiescent`, `resume_lineage`, `latest_child`) as a `Store` method using `crate::process::observe`.
- **Depends on**: none.
- **Acceptance**: port the shape of `tests/test_resume.py`'s lineage tests: a chain with any non-quiescent ancestor is reported unsafe to resume; `latest_child` picks the correct tip by `sequence`.
- **Est. LOC**: ~250.

### ST-6: Multi-attempt support + oversized-message spool
- **Implement**: Port `state/store.py:457-505` (`create_attempt`, `finish_attempt`) as real multi-attempt methods (Rust's `running`, `state/mod.rs:375-400`, currently hardcodes `number=1`, which will collide or misbehave on any resume/retry that creates a second attempt), and `state/db.py:86-131` (`resolve_message_storage`, `_spool_oversized_message`) for large-message spooling referenced from `append_message`.
- **Depends on**: ST-2 (shares the general transition/attempt bookkeeping).
- **Acceptance**: two attempts for one agent get numbers 1 and 2; a message beyond the size threshold is spooled to disk under the agent directory and stored with a `raw_ref` instead of inline content, matching `db.py:86-131`'s threshold.
- **Est. LOC**: ~300.

### ST-7: Launch bootstrap diagnostics
- **Implement**: Port `src/agent_run/launch_evidence.py` in full (280 lines: `preflight_executable`, `write_exec_failure`/`write_bootstrap_record`, `read_bootstrap_record`, `diagnose_bootstrap_failure`, `bootstrap_event_data`/`bootstrap_error_fields`) to a new `rust/src/launch_evidence.rs`, matching the pipe-based exec-failure protocol.
- **Depends on**: core-lifecycle/supervisor agent for the spawn call site that must write to/read from the pipe.
- **Acceptance**: spawning a nonexistent executable produces the same structured `bootstrap_failed` event fields as Python, not a generic I/O error.
- **Est. LOC**: ~350.

### ST-8: Run-stats full extraction + backfill
- **Implement**: Port `state/run_stats.py` in full (328 lines), replacing the inline subset in `Store::finish` (`state/mod.rs:494-504`) with `_runtime_result_stats`/`_token_usage_stats`/`_resumed_token_usage_stats`/`_resumed_usage_stats`, and add `backfill_run_stats`.
- **Depends on**: ST-5 (resumed-lineage aggregation needs the lineage walk).
- **Acceptance**: `ttft_ms`/`api_duration_ms` populate when present in the payload; a resumed chain's stats sum across attempts matching `run_stats.py:117-204`.
- **Est. LOC**: ~350.
- **Priority**: later — enriches reporting, doesn't block run lifecycle correctness.

### ST-9: Diagnostics/activity read helpers
- **Implement**: Port `state/activity.py:16-54` (`context_agents`) and `state/diagnostics.py:16-70` (`DiagnosticSnapshot`, `diagnostic_snapshot`).
- **Depends on**: none.
- **Acceptance**: matches Python's field set for a `doctor`-style snapshot.
- **Est. LOC**: ~200.
- **Priority**: later — CLI/doctor visibility only.

### ST-10: Centralize `agent_dir`/path-escape helper
- **Implement**: Port `src/agent_run/paths.py` (`agent_run_home` logic already exists as `fs::home`; add `agent_dir`, `create_agent_dir`, `runtime_skills_dir` with the same resolve+`_require_beneath` double-check) into `fs.rs`, and replace the three ad hoc `home.join("agents").join(id)` call sites (`rust/src/service.rs:265`, `rust/src/supervisor.rs:116`, `rust/src/state/mod.rs:443`) with calls to it.
- **Depends on**: none; touches other agents' files at the call sites, coordinate before editing them.
- **Acceptance**: a manually-constructed path escape attempt (not reachable through a valid `AgentId` today, but defense-in-depth) is rejected the same way Python rejects it.
- **Est. LOC**: ~150 (plus 3 call-site edits outside this area).
- **Priority**: later — no known exploitable gap today because `AgentId`'s format is already regex-constrained.

## 6. Risks and unknowns

- **Error taxonomy/wire mapping is unverified end-to-end.** `error.rs`'s `rpc_code`/`public()` (`error.rs:38-66`) was compared only against this area's Python exception classes, not against `service.py`'s actual JSON-RPC error serialization (out of this area's file list) — the transport/CLI agent should confirm `kind` strings match what clients expect, especially the `AnswerIntegrityError` vs `ValidationError` divergence noted in §3.
- **`process.rs`'s `token` field and `supervisor_identity` string format compatibility with existing databases is unverified** — an existing installation's `identity_json`/`supervisor_identity` values were written by Python's `psutil`-based code and never contain a `linux:`/`darwin:` token; Rust's `process::observe` (`process.rs:141-170`) falls back to birth-time comparison when the token doesn't match that prefix, which should keep old rows readable, but this was not tested against a real legacy row.
- **`waiting_binding` delivery state origin not located** in this area's files; ST-4's acceptance criteria depend on the delivery agent's findings for where that state is produced.
- **Schema-diff claim is a full-text read comparison, not a checksum/tool diff** — recommend an automated `diff` between the two `schema.sql` files as a cheap regression gate once both are edited independently.
- **Budget note**: this inventory did not read `docs/architecture.md`/`docs/runtime-contract.md`/`docs/continuations.md` line-by-line against the Rust port; citations above rely on source-code reading only, per the read-and-document scope.
