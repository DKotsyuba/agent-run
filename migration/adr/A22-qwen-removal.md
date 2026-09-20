# ADR A22: Remove the Qwen runtime

Status: Accepted

## Decision

Agent-run no longer implements or advertises Qwen. The Rust adapter variant,
Python adapter package, generated-home logic, credentials, probes, fixtures,
and live qualification requirement are removed. Legacy adapter identifiers are
recognized only long enough to return an actionable deprecation error telling
the operator to remove the runtime from `config.toml`.

The explicit OmniRoute capacity source remains independent of runtime adapters.
Historical changelog, migration-plan, and captured evidence references remain
unchanged because they describe earlier releases and the original baseline.

## Consequences

- Qwen cannot be started, resumed, authenticated, probed, or selected for
  capacity routing.
- Existing configurations must remove their Qwen runtime before this release is
  installed; invalid configuration continues to fail closed.
- Qwen-specific Python baseline behaviors are declared divergences under this
  ADR rather than reported as missing Rust work.
- Restoring Qwen would require a new product decision and a fresh adapter,
  security, and live-qualification contract.
