# ADR A11: `required_constraints` is role ∪ request, not an override

## Status

Decided (board row A11 in `migration/tasks.md`). Implemented for M15a/M15b.

## Context

A `required_constraint` names a policy boundary (e.g.
`external_network_isolation`) that admission must refuse to start unless the
effective policy proves it is actually enforced (see
`effective_policy.py` / `crates/agent-run-config/src/policy.rs`). Two
independent parties can name a requirement:

- the **role** itself, via a canonical profile's
  `required_constraints = [...]` frontmatter field, or
- the **caller**, via `StartRequest.required_constraints` on an individual
  request.

### Python baseline

Python keeps the two sources separate and *picks one*, depending on whether
the profile is canonical (`src/agent_run/service.py`,
`AgentService._policy_for_profile`):

```python
required=(
    profile.required_constraints
    if profile.canonical
    else request.required_constraints
),
```

For a canonical role, the caller's `request.required_constraints` is read
into the frozen role plan's own field during `_runtime_for_profile` but is
**never merged into the admission check** — admission uses only
`profile.required_constraints`, so a caller-supplied requirement on a
canonical role is silently dropped if the role does not also declare it. The
gap analysis in `migration/inventory/core.md` (CO-7) names this directly:
"make service combine profile/request constraints instead of overwrite."

### Decision

The Rust port uses the union, `role ∪ request`, for every profile, canonical
or legacy, everywhere `required_constraints` is read. A union can only add
requirements, never drop one either side declared, so this is a **tightening
compatibility change**: nothing that passed admission under the union can
fail admission Python's override would have passed under the union, but not
vice versa is exactly the case this ADR intentionally changes.

This is implemented once, at profile-resolution time, so every downstream
reader (the frozen role plan, the effective-policy check) sees the already-
tightened set without needing to know which profile kind produced it:

- `crates/agent-run-config/src/profiles.rs`, `parse()`:
  ```rust
  let mut required_constraints: BTreeSet<_> = required_vec.iter().copied().collect();
  // ...
  // Explicit caller requirements can narrow but must never be silently discarded.
  required_constraints.extend(request.required_constraints.iter().copied());
  ```
  This line runs unconditionally, for both legacy and canonical profiles.
- `crates/agent-run-config/src/role_plan.rs`, `resolve_role_plan()`, passes
  `profile.required_constraints` straight through into the frozen
  `ResolvedRolePlan.required_constraints` field — no separate merge needed,
  because the union already happened above.
- `crates/agent-run-config/src/policy.rs`, `evaluate()` (the legacy narrow
  entry point) and `effective_policy()` (the ported general one) both read
  `required` from the caller-supplied set (`profile.required_constraints` in
  `evaluate()`), so admission is checked against the same tightened set.

## Consequences

- A canonical role can no longer be started while ignoring a caller's
  explicit `required_constraints`; if the runtime cannot prove that
  boundary, admission now correctly refuses instead of silently proceeding.
  This is a security-tightening behavior change from the Python release, not
  a bug-for-bug port.
- A caller that previously relied on being able to bypass a role's
  admission requirement by simply not asking for it (never a documented or
  tested Python behavior) cannot do so under Rust.
- Nothing that was required by only one side changes: if only the role
  declares a requirement, or only the request does, the union equals
  whichever side is non-empty, so ordinary single-source configurations
  observe identical admission outcomes to Python.

## Regression test

`crates/agent-run-config/tests/profiles_policy.rs`,
`required_constraints_union_role_and_request_matches_python_when_only_one_side_is_set`,
proves all three cases in one test:

1. legacy profile + request-only requirement → union equals the request's
   set (matches Python's legacy branch),
2. canonical role + role-only requirement → union equals the role's set
   (matches Python's canonical branch),
3. canonical role + a *different* request requirement → union contains
   **both** (`plugin_immutability` and `external_network_isolation`),
   which is the point where Python's override would have kept only the
   role's `plugin_immutability` and silently dropped the request's
   `external_network_isolation`.
