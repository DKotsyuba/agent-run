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

## Account and history topology

- Codex labelled login stores credentials in
  `<app-home>/accounts/codex/<label>/auth.json`. The supervisor generates a
  lineage/per-run `runtime_home` under the configured runtime home, and
  materialization sets `CODEX_HOME` to that generated home. It links only
  `auth.json` from the account credential home into the generated home.
  Native thread history belongs to that generated home, not to the login
  credential home.
- Claude labelled login, execution, and quota collection share one
  `CLAUDE_CONFIG_DIR`, chosen by `materialize::claude_account_config`:
  `<app-home>/accounts/claude/<label>/claude-config`, unless only a login
  from an earlier release exists at
  `<configured-runtime-home>@<label>/claude-config`, which is then used in
  place. If both directories exist the label is ambiguous and login, runs,
  and quota collection fail instead of choosing one. Its account-scoped
  config state and native session discovery need a separate continuity proof.

The current product guard requires the parent's labelled account to remain
declared; it does not prove that native switching is impossible. A real A→B
continuation must demonstrate the same native session ID and history with
unchanged grants while authentication changes, through an explicit verified
handoff at the session boundary. For Codex, that may reuse the sealed lineage
home with a newly bound account credential, provided snapshot and process
evidence remain valid. For Claude, the account config location needs a
verified session-access path. Both remain unproven. A new conversation,
summary, foreign session ID, changed grants, unproved live process, or
ambiguous submitted turn cannot count as continuity.

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
runtime authentication paths (`agent-run login <runtime> --account <label>`
or `agent-run auth <label> <runtime>`; the provider CLI runs `codex login`
and `codex login status`, or `claude auth login` and
`claude auth status --json`) without reading or extracting credential
material. Such a probe must be harmless and tool-free, and it requires the
session seam above to select another existing account for the *same* native session.

That seam is explicit provider resume (see `docs/provider-contract.md`):
for Codex the native conversation lives in the run's own `CODEX_HOME`
(`sessions/**/rollout-*-<thread>.jsonl`), and a resumed attempt only rebinds
that home's `auth.json` link to the newly selected account after the parent
is cleaned up, then continues with `thread/resume` on the same thread. A
disposable-state probe with two real native Codex logins (the default login
and a named one, same provider and model, read-only role) switched accounts
after the first was disabled in the disposable registry and continued the
same thread, recalling a nonce from the parent turn. Claude Code keeps its
history per login directory, so its resume stays on the parent's account;
cross-account Claude continuation is unverified. Automatic, in-flight
quota-triggered failover is not implemented.
