# Source provenance and review coverage

## Immutable baseline

Repository: https://github.com/DKotsyuba/agent-run

Commit: `c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`

Release observed for this baseline: `0.11.15`.

The preceding failed attempt read the previous `0.11.14` baseline `c2832f36ce9d089158427dab13d9231bf1f16d4a`. The continuation observed the newer main branch and pinned the newer commit. This archive is a separate development artifact; its Cargo version is not a claim of an upstream release.

## Reviewed source areas

The review used successful GitHub connector reads of README and architecture documentation; schema 16; domain/config/native-settings/profile/policy/role-plan/service contracts; state operations; process launch and completion evidence; Codex app-server/permissions/config/plugins; Claude and Qwen launch/session material; capacity topology, forecasts, collection/source normalization; delivery dispatcher, existing Desktop relay and Claude UDS transport; and the MIT license.

Representative immutable source paths:

- https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/state/schema.sql
- https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/adapters/codex/permissions.py
- https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/adapters/claude/adapter.py
- https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/capacity/codex_appserver.py
- https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/capacity/sources.py
- https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/capacity/forecast.py
- https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/delivery/dispatch.py
- https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/delivery/codex_desktop_relay.py
- https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/delivery/codex_desktop_host.cjs
- https://github.com/DKotsyuba/agent-run/blob/c9904f9843ba4a0772bdfa8bac5259f18fad9dc3/src/agent_run/delivery/claude_uds.py

The full repository and complete regression test suite were **not** locally cloned. This is selective source coverage, not an exhaustive module/test inventory. No claims about full parity should be inferred from the list above.

## Concrete corrections made during review

The actual schema is version 16 even though an earlier architecture document referred to version 10. Bound completion retries are unlimited when `max_attempts = 0`; an initially introduced one-day expiration was removed after reading the dispatcher source. Claude's `output_schema` in this baseline is a prompt instruction, not a native schema validator. Account labels must remain distinct from the absent global account. Codex must check effective grants, not merely send a desired sandbox setting.

## What was not done remotely

No branch, commit, pull request, release or workflow run was created in the user's repository. No real engine login, quota-bearing agent task or real completion delivery was performed. Documentation links are provenance, not copies of the complete original source tree.
