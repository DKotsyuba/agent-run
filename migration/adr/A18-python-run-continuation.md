# A18: Python-created Codex runs are readable but not Rust-resumable

## Status

Accepted for M34. This remains a named cutover blocker.

## Decision

The Rust service explicitly returns `Unsupported` for a Python-created parent
instead of attempting a lossy conversion. Its history, answer evidence, and
lineage stay readable.

## Evidence

Python persists a compact, versionless identity containing `runtime`,
`account`, account-scoped `home`, `auth_target`, `profile`, `write`,
`read_roots`, and `fast` ([resume.py](../../src/agent_run/resume.py:108),
[resume.py](../../src/agent_run/resume.py:155)). It proves the current
account/home/auth target, then reconstructs a child from the historical
request JSON and that snapshot ([resume.py](../../src/agent_run/resume.py:169),
[resume.py](../../src/agent_run/resume.py:235)). For snapshot continuations it
uses the root agent's `runtime-home` and checks its historical materialization
snapshot before native attach ([preparation.py](../../src/agent_run/preparation.py:225),
[preparation.py](../../src/agent_run/preparation.py:250)).

Rust requires a versioned identity which embeds a parsed Rust `Config`,
resolved `Profile`, `EffectivePolicy`, and sealed runtime-home/digest
([service.rs](../../crates/agent-run-core/src/service.rs:20)). Rust verifies
that exact sealed home before admitting the child
([service.rs](../../crates/agent-run-core/src/service.rs:227)); it then uses
the recorded lineage session in `thread/resume` and rejects a changed returned
thread identity ([codex.rs](../../crates/agent-run-core/src/codex.rs:462),
[codex.rs](../../crates/agent-run-core/src/codex.rs:487)). The two identities
therefore cannot establish the same immutable grant or snapshot proof. The
Python request replay also re-resolves its profile from its own snapshot,
whereas Rust clones a successfully decoded Rust `StartRequest`
([service.rs](../../crates/agent-run-core/src/service.rs:246)).

The golden Python v16 fixture includes an older, even smaller Python identity
(`{"account":"fixture","profile":"review"}`), confirming that schema
history cannot be inferred safely. `codex_resume.rs` asserts the resulting
typed refusal.

## Consequence

Rust-created Codex runs resume natively: their preserved identity, sealed
account-scoped home, store lineage session, and immutable grant are reused.
Python-created runs cannot be resumed by the Rust build until an explicit,
versioned importer can prove (not guess) all of: the historical resolved
config/profile/policy, the credential/home binding, sealed generated-home
digest, and request replay equivalence. No conversion is implemented in M34.
