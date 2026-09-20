# A21 — Runtime platform divergence

Status: accepted for the migration.

The Python version gate cited below belongs to the frozen
`archive/python-legacy` implementation. The Rust product has no Python runtime
gate; supported target claims are governed by [A15](A15-platforms.md).

## Context

The Python distribution gates the interpreter it runs on. `_require_supported_python`
(`src/agent_run/__init__.py:9-24`) raises `RuntimeError` for anything below Python
3.14 and is invoked at import time, so an unsupported interpreter fails before any
runtime feature loads. `tests/test_domain.py::DomainTests::test_python_version_gate_refuses_unsupported_version`
asserts that gate directly, passing `(3, 13)`, `(3, 14)` and `(3, 15)` to the function.

## Decision

Not ported. The Rust artifact is a compiled binary with no interpreter to gate:
the language version is fixed at build time by the toolchain, and there is no
runtime value that could be unsupported. The behavior has no reachable state.

The equivalent protection — refusing to run against an artifact built by an
incompatible toolchain — is a build and release concern, covered by the sealed
release manifest rather than by a runtime check.

## Consequence

This row stays uncovered permanently and is excluded from remaining migration
work. It is not a gap to close later.
