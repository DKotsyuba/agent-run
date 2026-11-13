# Native session continuity across an account change

Status: schema-2 supports guarded Codex account switching and an opt-in ordered
Claude API-to-native-login queue. A disposable direct Claude Code probe resumed
an API-created transcript with a subscription login, retained its session ID and
recalled a marker; the Agent Run handoff has synthetic integration coverage.
This does not qualify every provider error or interrupted external action.
Historical schema-1 rows do not silently resume through a schema-2 provider.

## Historical schema-1 continuation conditions

These are the durable, checkable conditions of the legacy resume path
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
- Claude labelled login and execution share one
  `CLAUDE_CONFIG_DIR`, chosen by `materialize::claude_account_config`:
  `<app-home>/accounts/claude/<label>/claude-config`, unless only a login
  from an earlier release exists at
  `<configured-runtime-home>@<label>/claude-config`, which is then used in
  place. If both directories exist the label is ambiguous and login and runs
  fail instead of choosing one. For an ordered switch,
  authentication uses this account directory while `--resume` names the
  previously sealed transcript by absolute path.

The historical guard requires the parent's labelled account to remain
declared. In schema 2, Codex A→B rebinds the credential only after verified
cleanup and continues the same native thread in the sealed lineage home; this
was observed with two real logins. A real three-account chain has fixture
coverage but no live qualification. Ordered Claude retries the same transcript
only after its original input, cleanup and tool lifecycle are proven. A new
conversation, summary, foreign session ID, changed grants, unproved live process,
or ambiguous submitted turn cannot count as continuity.

## Repeatable fixture boundary and existing evidence

The fail-closed negatives an account handoff must satisfy are pinned by
`crates/agent-run-core/tests/session_continuity.rs`
(`cargo nextest run --locked -p agent-run-core -E 'test(/^session_continuity::/)'`), which
drives the real `Service::resume` boundary with temporary homes and store
fixtures only — no provider, credential store, or real account:

- `grant_tampering_and_unsealed_home_are_typed_refusals`: contradictory
  grants refuse with an integrity error, and an identity without a sealed
  runtime home refuses with a validation error; neither falls back to a new
  conversation.

The remaining boundary conditions are already pinned by existing targets and
are not duplicated:

- `codex_resume` tests: account and model pinning at the config boundary,
  single-continuation lineage atomicity, runtime-home drift detection, and
  refusal of Python-created runs.
- `resume_stream` tests: a result whose `session_id` is missing or foreign
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
same thread, recalling a nonce from the parent turn. A separate direct Claude
Code probe resumed a disposable API transcript under a subscription login by
absolute path and recalled its marker; the persisted continuation appended to
the same test transcript. Ordered Claude handoff in Agent Run is separately
covered by synthetic broker/supervisor tests. Other cross-account Claude paths
remain refused. See `provider-contract.md` for triggers and proof requirements.

## Harness history normalizations

The Claude seal's structural check compares every `api-request-blob` tool
call with the real transcript record it repeats. Claude Code 2.1.311 and
2.1.322 replay some calls normalized, which previously prevented any seal
(`native_history_unavailable`) and made the execution unresumable. Exactly
two rewrites are accepted: a `Bash` `command` prefixed with
`cd <cwd> && `, where `<cwd>` is the absolute `cwd` of the real record, and
an `Edit` input without its recorded default `replace_all: false`. Any other
directory, quoting, separator, key, value or tool still refuses.

Codex `thread/resume` sets `excludeTurns`; otherwise codex-cli returns every
earlier turn in one reply, which exceeds the engine frame on a long thread.

A system resume preflight returns only its closed code. The account worker
logs the content-free cause on stderr, including whether changed history grew
after an intact sealed prefix or was rewritten.

## Stable public agent identity

The public `agent_id` is the existing lineage root and remains the only agent
identifier exposed to orchestrators. Immutable execution rows, answers, timings,
native sessions and process ownership remain separate internally. Retries and
account handoffs retain their attempt identity inside each execution.

CLI, MCP and socket controls resolve the stable ID to the latest execution once.
Request-id replay recovers the original admission even after later resumes.
Lists page logical agents; transcripts use global message cursors across the
retained lineage. Existing cleanup and transactional admission gates still apply.

Machine admission receipts contain the stable ID plus the existing `sequence`
counter. Hooks and CLI wait use this to pin the exact admission without exposing
a second agent identifier. Legacy exact-ID inputs remain accepted privately.
Reconnect MCP clients together with the broker so both use the same receipt
contract. The database schema is unchanged. See the embedded
[completion guide](../assets/operator_guide/completion.md).

## OpenCode v2 continuation

A cleanup-bound `HistorySeal` additionally records the private SQLite main
file, exact WAL bytes and settled session/message semantics. An absent and an
empty WAL are the same evidence (no frames): OpenCode 2.3.0 deletes its WAL on a
graceful exit and the read-only proof connection recreates it empty. SHM is
coordination data and is not history. Anchored no-follow reads, WAL-aware
query-only SQL and byte/message bounds reject linked, changed, foreign or
unfinished history. Pending input, suspension, compaction and revert cannot mint
a seal. Historical Codex/Claude seals omit this optional field and retain their bytes.

The seal records the OpenCode version that wrote the session
(`sqlite.version`, from `session_v2.version`). Only qualified versions seal, and
each uses its own schema table:

| Version | Pending input (must be empty for the session) | `session_v2.time_suspended` |
| --- | --- | --- |
| 2.0.28 | `session_inbox` and `session_pending` (both required) | known absent; checked if present |
| 2.3.0 | `session_inbox` (required); `session_pending` known absent, checked if present | required, must be null |

A table or column may be missing only where the session's version is known to
lack it; anywhere else its absence is an incompatible schema, never an empty
queue. The semantic digest covers the same selected session columns and full
projected messages for every version. Columns outside that selection, such as
2.3.0's `resume_attempts`, do not enter it; a fresh 2.3.0 `serve` on the same
database left the digest unchanged in live resume checks. A seal written before versions were recorded means 2.0.28, the only
version that could seal then.

Resume verifies the physical manifest before spawn. After native startup may
checkpoint the database, the started server's `/api/info` version must equal the
sealed version, and the same session and full projected-message digest must
still match before another input is admitted. A session sealed under one
OpenCode version therefore never resumes under another; the refusal is
`continuation_unavailable`. Native startup may already have migrated the private
database by then, which is why the version check happens before any input. No new baseline, summary
substitution or latest-session lookup is allowed. OpenCode stays on the parent's
account; cross-account automatic continuation is not enabled.
