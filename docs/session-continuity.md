# Native session continuity across an account change

Status: feasibility determined, fixture canary in place, real A→B account
switch pending. This document records the exact conditions under which a
native Codex or Claude-family session keeps its session ID, history, and
grants across an account change, the product/API seam a real switch needs,
and how the repeatable fixture boundary proves everything that can be proved
without a second real account.

## Conditions for a native session to survive continuation

These are the durable, checkable conditions enforced by the resume path
(`agent-run-core` `service::resume` and `stream::plan`):

1. **Terminal parent with a recorded native session.** Resume requires a
   run whose status is terminal and whose `runtime_session_id` is the native
   thread/session ID confirmed by the engine.
2. **Sealed Rust launch identity.** The parent must carry
   `rust_identity_version = 1` with a role profile that matches the stored
   request (name, write grant, read roots), an effective policy, a sealed
   runtime home, and a runtime snapshot digest that still verifies against
   the materialized snapshot. Python-era runs are refused.
3. **Model still enabled.** The parent's model must remain in the runtime's
   enabled model list.
4. **Runtime identity unchanged.** The runtime home and auth block must be
   byte-identical to what the parent launched with; otherwise resume refuses
   with "runtime identity changed since the parent ran".
5. **Account still declared.** A labelled account on the parent request must
   still be declared for the runtime (`selected_account`). This is a product
   guard, not a native harness limitation.
6. **Stream identity certification.** A resumed engine's result must report
   the exact requested session ID; a missing or foreign `session_id` fails
   the run instead of certifying work from another native context
   (`resume_stream.rs`).
7. **Turn gating.** A Codex app-server session accepts turn events only for
   the active thread/turn pair; a replayed turn completion without a new
   turn ID is ignored, so tool results cannot be journaled twice
   (`agent-run-adapters` `codex/session.rs`).
8. **Immutable grants and single lineage.** Exactly one continuation exists
   per stale ancestor (`Error::Conflict` otherwise), and grant tampering in
   the recorded identity is an integrity refusal.

## Why a real account switch cannot continue the same native session today

Native history is stored inside the account-scoped home:

- Codex: labelled accounts run with `CODEX_HOME = <app-home>/accounts/codex/<label>`
  and thread history lives under that home; `codex login --account <label>`
  writes credentials only there.
- Claude family: labelled accounts run with
  `CLAUDE_CONFIG_DIR = <runtime-home>@<label>/claude-config`, and sessions
  live under that config dir.

Selecting a different existing account therefore selects a different native
home, and the native harnesses resolve `--resume <id>` / thread resume only
inside the current home. No public API today can resume a native session
recorded under account A while authenticating as existing account B.

**Required product/API seam (not yet implemented):** either

1. a native harness capability that resumes a session by an explicit
   cross-home reference (session path or exported session), or
2. an agent-run account-handoff operation that re-seals the recorded native
   session (history plus grants) into the target account's home and records
   the transfer in the launch identity before resume runs.

Until one exists, a real A→B proof is pending; mocks and fixtures cannot
establish it. Native fallback to a new conversation or a summary is not an
acceptable substitute: absent history, a different session ID, changed
grants, an unproved live process, or an ambiguous submitted turn must be
reported as a typed unavailable/blocker.

## Repeatable fixture boundary and existing evidence

The fail-closed negatives an account handoff must satisfy are pinned by
`crates/agent-run-core/tests/session_continuity.rs`
(`cargo test --locked -p agent-run-core --test session_continuity`), which
drives the real `Service::resume` boundary with temporary homes and store
fixtures only — no provider, credential store, or real account:

- `grant_tampering_and_unsealed_home_are_typed_refusals`: contradictory
  grants refuse with an integrity error, and an identity without a sealed
  runtime home refuses with a validation error; neither falls back to a new
  conversation.

The remaining boundary conditions are already pinned by existing targets and
are not duplicated:

- `--test codex_resume`: account and model pinning at the config boundary,
  single-continuation lineage atomicity, runtime-home drift detection, and
  refusal of Python-created runs.
- `--test resume_stream`: a result whose `session_id` is missing or foreign
  cannot certify a resumed run.
- `agent-run-adapters`' `codex_session.rs`: a replayed turn completion
  without a new turn ID is ignored, so tool results cannot be journaled
  twice.

A provider quota rejection is the trigger scenario for an account switch,
but it is a distinct signal from agent-run's global active-capacity limit;
no fixture here claims to simulate provider quota exhaustion.

## Real native probe boundary

A real opt-in probe is admissible only when it uses the normal authorized
runtime authentication paths (for example `auth login`/`auth status` account
selection through the public CLI) without reading or extracting credential
material. Such a probe must be harmless and tool-free, and it requires the
seam above to select another existing account for the *same* native session;
until then, no real probe is shipped and no successful canary is claimed.
