# CLI extensions disposition

Date: 2026-09-16

## Decision

The Rust CLI exposes exactly the 33 Python baseline command records in
`tests/fixtures/baseline/cli-spec.json`. `state check`, `state backup`, `api
ping`, and `api tools` were removed because they are Rust-only operator
additions and are not required to operate the compatible CLI.

`start --task-file`, `start --required-constraint`, `resume --wait`,
`agents --after-revision`, `agents --wait-seconds`, the `--timeout-seconds`
alias, and MCP session environment inference were removed for the same reason.
The corresponding domain features remain transport-internal where applicable;
they are not shell compatibility aliases.

`_supervisor` and `_deny-command` remain hidden implementation commands.
`_supervisor` is the Rust supervisor's own spawned executable entry point, and
`_deny-command` is emitted into generated native command-policy wrappers.
They are deliberately absent from help and may not be treated as public CLI
extensions.

## Consequence

Any new visible Rust operator command requires a separate ADR and a parser
contract test. It must be explicitly additive and cannot replace a Python
baseline command or flag.
