# A17 — Completion notice, delivery evidence, and failure-kind contract

> **Historical compatibility investigation.** Python paths and the local venv
> command below describe the frozen baseline used to correct the port. They are
> evidence provenance, not current operating instructions. Read each
> `src/agent_run/...` citation as `archive/python-legacy:src/agent_run/...`.

Status: accepted. Resolves three pre-existing `crates/agent-run/tests/protocol.rs`
failures and two `crates/agent-run/tests/fake_engine.rs` failures (the latter
surfaced by same-night merges landing the answer-proof work), all introduced by
the blind port before any test had been run against the real Python release, by
checking each assertion against the actual Python source and its own tests, and
correcting the invented ones.

## Method

For each of the three tests, Python's actual behavior was read from
`src/agent_run/delivery/base.py` and `completion_notice_contract.py`, cross-checked
against `tests/test_delivery_base.py` and the byte-identical contract JSON
(`src/agent_run/delivery/completion_notice_contract.json` ==
`assets/completion_notice.json`, confirmed by diffing the parsed JSON), and against
`tests/fixtures/baseline/notices/cases.json` (rendered notices captured from the
running Python release). Two of the three assertions were then re-verified by
running `CompletionNotice(...).render()` directly under
`/Users/pluto/projects/agent-run/.venv-py314/bin/python` (3.14.3) with
`PYTHONPATH=src`, rather than trusting the source reading alone.

## Decisions

### 1. `notices_escape_controls_and_never_accept_arbitrary_lifecycle` — test corrected

Old assertion: `notification_id = "ntf_"` must fail `validate()`.

Python rule (`base.py:186-191`, `_trusted_id`): a `notification_id` is valid
whenever it is a nonblank string of at most 512 characters — there is no `ntf_`
prefix or format requirement. `tests/test_delivery_base.py:208-210` enumerates the
actual invalid set: `("", "   ", "n" * 513, 7)`. A live run confirms it:
`CompletionNotice(notification_id="ntf_", ...)` constructs without error
(`ntf_ ACCEPTED (no format rule)`, verified in this task). The `/^ntf_[A-Za-z0-9_-]+$/`
regex the port's author likely had in mind lives in
`src/agent_run/delivery/codex_desktop_host.cjs:153`, a different trust boundary (the
Node relay's own inbound request parsing), not `CompletionNotice`.

Verdict: the test encoded an invented rule. Rewrote it to assert Python's real
blank/oversized cases (`""`, `"n".repeat(513)`) instead of the fictitious `"ntf_"`
case, keeping the genuine nonterminal-status rejection (`Status::Running`, which
does mirror `base.py:265-266`'s `status not in TERMINAL` check).

Rust: `crates/agent-run-core/src/delivery/mod.rs:249-267` (`Notice::validate`) was
already correct — it only needed the test's expectation fixed.

### 2. `evidence_does_not_accept_extra_private_fields` — test corrected

Old assertion: `safe_evidence` must accept
`{"classifier","duration_ms","accepted","ambiguous"}`.

Python rule (`base.py:121-156`, `DeliveryAttemptEvidence.from_payload`): the
accepted shape is *exactly* 14 declared fields — `classifier`, `executable`,
`argv_shape`, `duration_ms`, `returncode`, `spawn_errno`, `error_class`,
`stdout_tail`, `stderr_tail`, `stdout_bytes`, `stderr_bytes`, `stdout_truncated`,
`stderr_truncated`, `message_id_present` — and `base.py:136` rejects any payload
whose key set differs at all (`set(value) != expected`), not only ones with an
extra `secret` key. `accepted` and `ambiguous` are not stored fields in either
language: in Python they are never part of the dataclass; in Rust
(`crates/agent-run-core/src/delivery/mod.rs:105-119`) they are private derived
methods on `Evidence`, not `#[derive(Deserialize)]` fields. A payload built from
only `classifier`/`duration_ms`/`accepted`/`ambiguous` was never a valid evidence
record in either implementation — with `#[serde(deny_unknown_fields)]` and no
`#[serde(default)]` on `Evidence` (mod.rs:38-70), it fails on the *missing*
required fields before `secret` is ever added, so the test's first assertion
(`is_some()`) could never have passed.

Verdict: the test encoded a shape that doesn't correspond to the real contract on
either side. Rewrote it to use the full valid 14-key evidence object, confirm
`safe_evidence` accepts it, then confirm adding an extra `secret` key makes it
`None` — which is what `deny_unknown_fields` actually guards, matching
Python's `from_payload` exact-key-set check.

Rust: `crates/agent-run-core/src/delivery/mod.rs:137-140` (`safe_evidence`) and the
`Evidence` struct were already correct; only the test's fixture shape was wrong.

### 3. `unknown_failure_categories_do_not_leak_provider_prose_in_notices` — test corrected (renamed)

Old assertion: rendering a notice with an unrecognized `failure_kind` must NOT
contain that failure_kind text, and MUST contain the literal phrase
`"verified successful outcome"`.

Python rule (`completion_notice_contract.py:97-110`, `failure_notice_block`): for
an unrecognized kind, `reason`/`advice` fall back to `default_failure`, but the
kind label itself (`kind = failure_kind or "unknown"`) is always interpolated
into the `"- Failure: {kind} — {reason}"` line — it is never suppressed. This is
confirmed three ways:
- `base.py:309-321` (`CompletionNotice.render`) always escapes and passes
  `failure_kind` into `failure_notice_block`; it never substitutes a placeholder
  for an unrecognized value.
- Baseline fixture `tests/fixtures/baseline/notices/cases.json`, case
  `failed-codex_futureProviderCode` (lines 74-84): rendered output contains
  `"- Failure: codex_futureProviderCode — The agent ended without a recognized
  failure category."` verbatim.
- Live run in this task:
  `CompletionNotice(status=FAILED, failure_kind="secret-provider-error-text").render()`
  returns `"...- Failure: secret-provider-error-text — The agent ended without a
  recognized failure category.\n- Advice: Inspect list_agents, ..."` — the exact
  text the old test asserted must be absent. The phrase `"verified successful
  outcome"` does not exist anywhere in `completion_notice_contract.json` or its
  Rust mirror `assets/completion_notice.json` (confirmed byte-identical via
  parsed-JSON diff); it was invented.

The real safety property Python enforces is narrower than "no leak": the
**reason/advice guidance text is always package-owned** (from `default_failure` or
a known `failure_guidance`/`status_guidance` entry), never derived from the
caller's `failure_kind` string — only the short, length- and blank-bounded label
itself (`_metadata`, `base.py:194-216`, ≤128 chars) is passed through, and only
after control-character/line-separator escaping (`_escaped_metadata`,
`base.py:219-236`) so it can never forge a new list line.

Verdict: the test encoded an invented rule and a fabricated string. Renamed it to
`unknown_failure_categories_render_with_package_owned_default_guidance` and
rewrote the assertions to match the fixture: the label renders (using a
realistic unrecognized kind, `codex_futureProviderCode`, taken directly from the
baseline fixture) paired with the exact `default_failure` reason/advice text from
`completion_notice_contract.json`.

Rust: `crates/agent-run-core/src/delivery/mod.rs:211-245` (`guidance`) and
`:270-287` (`Notice::render`) already implement exactly this — `display =
escaped(kind, "unknown")` is included in the formatted failure line regardless of
whether `kind` matched a known `failure_guidance` entry. No implementation change
was needed.

## Safety properties preserved (beyond the letter of these three tests)

- Control characters and Unicode line separators in any metadata field
  (`runtime`/`model`/`effort`/`failure_kind`) are always escaped before reaching
  the rendered notice (`mod.rs:198-209`, matches `base.py:219-236`) — a notice can
  never gain a forged fifth list line.
- `failure_kind`/`runtime`/`model`/`effort` remain bounded to ≤128 characters and
  nonblank when present (`Notice::validate`, `mod.rs:258-265`, matches
  `_metadata`, `base.py:194-216`).
- Evidence stays bounded and redacted: `redact_tail` (`mod.rs:143-173`) masks
  credential-shaped substrings and caps tails at 4096 bytes; `persisted()`
  (`mod.rs:122-133`) re-validates and caps the whole record at 16 KiB — matching
  `_MAX_EVIDENCE_TAIL_BYTES`/`_MAX_ID_LENGTH` bounds in `base.py:25,88-89`.
- Evidence's `deny_unknown_fields` (no `#[serde(default)]`) means an *unexpected*
  key set — too few fields, too many, or merely different — is rejected, which is
  a strict superset of Python's `set(value) != expected` check in `from_payload`
  (`base.py:136`).

### 4. `fake_engine.rs::exit_zero_without_terminal_result_is_not_success` — test corrected

Old assertion: exit code 0 with no terminal result classifies as `failure_kind ==
"missing_result"`.

The fixture engine for this scenario (`crates/agent-run/tests/fixtures/engine.rs:58-61,101-103`)
emits one `"assistant"` text line (`"fixture partial\n"`) *before* returning
without ever writing a `"result"` line. Python's `StreamDecoder.finalize()`
(`src/agent_run/adapters/claude/stream.py:335-349`) distinguishes exactly this:
`subtype = "cut_off" if self._saw_assistant_text else "no_answer"` — an engine
that streamed content and then stopped short is `"cut_off"`; an engine that
produced nothing at all is `"no_answer"`. `session.py:344-349` then passes an
unrecognized/non-`"no_answer"` subtype straight through `classify_failure`
(`stream.py:220-236`) as the literal `failure_kind`. `"missing_result"` does not
exist anywhere in `src/agent_run/`. This was independently verified by feeding
the exact same two lines the fixture emits into a live `StreamDecoder` under
`/Users/pluto/projects/agent-run/.venv-py314/bin/python` (3.14.3,
`PYTHONPATH=src`): `finalize().subtype == "cut_off"` and
`classify_failure(...) == "cut_off"`.

Rust's own EOF handler in `crates/agent-run-core/src/stream.rs:309-321` already
implements the identical `saw_answer` (`"cut_off"`) vs. no-diagnostic
(`"no_answer"`) vs. diagnostic-classified (`"auth_failed"`/`"provider_error"`)
branching, so it already computed `"cut_off"` for this fixture correctly.

Verdict: the test encoded an invented label. Corrected the expected value to
`"cut_off"`; the `"no_answer"` branch (no streamed content at all) is exercised
by Rust's EOF-without-`saw_answer` path and needed no new test, since Python's
own distinguishing input (some vs. no assistant text) is what the fixture's
behavior already selects.

### 5. `fake_engine.rs::corrupted_answer_proof_is_rejected_even_after_a_real_seal` — test corrected

Old assertion: a post-seal digest mismatch surfaces as `agent_run::Error::Integrity(_)`.

Python raises `AnswerTamperedError` (`src/agent_run/verify.py:70`, an
`AnswerError` subclass) for exactly this condition —
`raise AnswerTamperedError("answer artifact hash does not match its recorded
proof")` at `verify.py:505`. The answer-proof work that landed on this branch
the same night (`b7553f2`, "verify and seal answer proofs against the Python
corpus") rewrote `crates/agent-run-platform/src/verify/mod.rs`'s `read()` to
raise `Error::AnswerIntegrity("answer artifact hash does not match its recorded
proof")` for this exact case (`mod.rs:242-245`), and moved every other
answer/proof check in that module (missing artifact, oversized, size mismatch,
non-UTF-8, malformed/missing/contradicting sidecar) onto the same
`AnswerIntegrity` variant. `crates/agent-run-domain/src/error.rs:92-97` states
the direction directly: `AnswerIntegrity` is documented as the live variant
("Verified-answer evidence failed an integrity contract"), while `Integrity` is
"Legacy integrity name retained temporarily for existing migration callers."
Both map to the identical public `MachineCode::AnswerIntegrityError`
(`error.rs:134`) and identical `public()` message handling (`error.rs:163-164`),
so no caller-visible JSON-RPC/MCP/CLI behavior changed — only the internal
variant name the test must match moved with the rest of the module.

Verdict: the test was correct at the moment it was written but was overtaken by
this branch's own subsequent migration progress, not by an invented rule.
Corrected the expected variant to `agent_run::Error::AnswerIntegrity(_)`,
matching both the current implementation and Python's `AnswerTamperedError`
naming intent.

## Outcome

All five tests were rewritten to assert the verified Python contract; no
production code changed for items 1-3 (delivery notices/evidence), since the
existing Rust implementation already matched Python. Items 4-5 needed no
implementation change either — `crates/agent-run-core/src/stream.rs`'s EOF
classifier and `crates/agent-run-platform/src/verify/mod.rs`'s `AnswerIntegrity`
migration were already correct; only the tests, written before either landed or
before Python was run, lagged behind. `cargo test --test protocol --test
fake_engine` is green (13/13 and 13/13).
