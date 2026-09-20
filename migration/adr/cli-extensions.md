# CLI extensions disposition

Status: accepted for 0.12.0.

The public Rust CLI preserves the 33-command baseline recorded in
`tests/fixtures/baseline/cli-spec.json`. Compatibility is defined by parser and
transport contract tests, not by retaining Python implementation files.

The current CLI also includes only the small additions required by the native
product contract and documented workflows. In particular, `resume --task-file`
is supported and documented; it is not a removed experiment. Hidden commands
such as `_supervisor`, `_deny-command`, and `_permission-request` are internal
process/policy entry points and are deliberately absent from public help.

New visible commands require documentation and parser-contract coverage. They
must be additive and may not silently change an existing command's wire or
persistence semantics.
