# A19 — Release and deploy divergence from the Python release script

Status: accepted for the migration; the publication half remains owner-decidable.

## Context

`tests/test_release_script.py` carries 30 behaviors. Seven describe deployment
quiescence and recovery and are genuine contracts for the Rust `xtask`. The
other 23 describe a release pipeline that the Rust workspace does not have and
is not intended to grow: `xtask release` is an offline build, verify, install,
update and rollback tool, while the Python script also published to GitHub,
installed a hash-pinned wheel closure into a sealed virtualenv, ran a release
time smoke client, drove `launchctl`, and migrated the store mid-deploy.

Porting those 23 would mean writing the missing subsystems in order to satisfy
their tests. That is the opposite of a migration: it would grow the Rust
surface to match an implementation detail of the Python packaging story.

## Decision

Port the seven deployment-recovery behaviors. Classify the remaining 23 as
divergences, leave them unported, and record them here so that the coverage gap
is explained rather than merely observed.

### 1. GitHub publication — 11 behaviors, not ported

`missing_or_wrong_head_workflow_never_passes`,
`failed_and_empty_job_workflows_fail_closed`,
`required_pending_and_missing_checks_wait`,
`required_failure_and_head_change`,
`tag_must_be_annotated_and_match_package`,
`public_resume_does_not_inspect_dirty_checkout`,
`dirty_new_source_refused_before_fetch`,
`version_pr_updates_and_checks_lock_with_package_version`,
`new_publication_gates_exact_heads_before_annotated_tag`,
`assets_require_hash_and_signed_commit_workflow_subject`,
`cli_uses_shared_exception_identity_and_restores_every_job`.

Python drives `gh` for workflow and check gates, opens a release pull request,
refreshes the lock, creates an annotated tag, and verifies attestation and
provenance of downloaded assets. Nothing in `crates/` or `xtask/` calls `gh`,
merges a pull request, or checks attestation. Publication stays on the
Python/CI side. The sealed-manifest portion of this area is already covered by
`xtask/tests/release.rs`.

### 2. pip and virtualenv installation — 2 behaviors, not ported

`prepare_rejects_python_before_creating_release`,
`locked_install_requires_complete_transitive_closure`.

Python requires a 3.14 interpreter and installs a hash-pinned wheel closure
into a sealed virtualenv. The Rust artifact is a single static binary;
`xtask/src/release.rs` has no interpreter, `pip` or `venv` concept.

### 3. Release-time smoke and live validation — 4 behaviors, not ported

`mcp_smoke_keeps_stdin_open_until_tools_reply`,
`mcp_smoke_bounds_partial_frames`,
`mcp_smoke_rejects_initialization_errors`,
`live_validation_waits_for_api_then_retries_transient_database_open`.

The Python release script performs an MCP stdio handshake and a post-switch API
probe. `xtask` contains no MCP client and no live probe. Porting these would
require building a release smoke harness that does not exist. MCP protocol
behavior itself is covered separately by the MCP parity tests.

### 4. launchd control — 2 behaviors, not ported

`wrong_default_home_plist_fails_before_stopping`,
`restart_failure_retries_compatible_services`.

Python reads installed property lists, resolves `AGENT_RUN_HOME` from them, and
calls `bootout`, `bootstrap` and `kickstart` with retry. No `launchctl`
invocation exists anywhere in the workspace: `crates/agent-run/src/launchd.rs`
only renders descriptors, and `--home` is always explicit.

### 5. Schema migration during deploy and roll-forward — 4 behaviors, not ported

`failed_migration_restores_old_only_when_schema_is_unchanged`,
`swap_failure_after_migration_recovers_new_binary`,
`recovery_retries_database_open_and_never_selects_old_binary`,
`database_readiness_timeout_preserves_journal_without_guessing_schema`.

Python migrates the store in the middle of a deploy and then selects which
binary to recover to by inspecting the observed schema. In Rust, migrations run
when the runtime opens the store (`agent-run-store`); deployment never migrates,
and recovery offers only an explicit operator `rollback`. The recovery
behaviors therefore describe a state machine Rust does not enter.

## Consequence for the coverage map

These 23 rows stay uncovered permanently. They are not a backlog item and
should not be counted as remaining migration work. Any future report that shows
`tests/test_release_script.py` as a large uncovered cluster should cite this
record rather than re-deriving the analysis.

## Defect found while porting the seven that do apply

`xtask/src/deploy.rs` `quiescent` counted only non-terminal `agents` rows, so a
deploy could swap the `current` binary while a `workflow_runs` owner was still
running and still holding the store. The module documentation asserted that no
Rust counterpart to the Python legacy-writer check existed; that was wrong.
`sql/schema.sql` — the schema the Rust store itself creates — carries
`owner_pid_identity` and `owner_birth_time`, so those rows live in the very
database `quiescent` opens.

The fix ports `scripts/release_local.py` legacy-writer logic: only a `Dead` or
`Reused` observation proves a recorded writer is gone, while `Denied` and
`Unknown` are uncertainty and block the switch; non-finite and negative birth
evidence is discarded rather than compared.

## Open observations, deliberately not addressed here

- On macOS `proc_pidinfo` answers `ESRCH` for an unreaped child, so
  `Identity::zombie` in `crates/agent-run-platform/src/process.rs` is only ever
  set on Linux. Both paths reach the same `Dead` verdict, so this is cosmetic
  today, but the platform asymmetry is real.
- `quiescent` treats a missing `state.db` as quiescent, which is correct for a
  first installation, whereas Python refuses it. This difference is unproven by
  any test and should be settled deliberately rather than by accident.
