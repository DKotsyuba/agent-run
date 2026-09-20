# Migration documentation

Rust is the primary implementation as of version 0.12.0. The former Python
implementation is frozen on branch `archive/python-legacy` at commit
`c58d5a0`; it is not maintained from `main`.

## Current records

- [Status](status.md) — current completion and remaining qualification gaps.
- [Qualification scope](evidence/qualification-scope.md) — scenario coverage
  and the remaining live Desktop and Linux checks.
- [Review checklist](review-checklist.md) — gates for a release candidate.
- [Recovery](recovery.md) and [post-cutover report](post-cutover-report.md) —
  operator records for production deployment.
- Accepted ADRs in [adr](adr/) define intentional compatibility and product
  decisions. ADR A22 removes Qwen.

## Immutable evidence

Dated files under [evidence](evidence/) record what was observed at the named
commit and date. They are historical facts, not current status pages. Do not
rewrite an old result to describe a later build; add a successor evidence file
and link it from the current status instead. The machine-verified inventory is
`evidence/index.json`.

## Historical planning material

The migration plan, author plan, task board, work log, defect log, baseline,
inventory, and port snapshot explain how the port was performed. Their counts,
paths, branch names, and future-tense statements describe the migration at that
time and are not operating instructions for 0.12.0.

Python paths in historical records use the citation form
`archive/python-legacy:<path>`. They refer to the frozen archive branch and do
not imply that the file exists on `main`. New documentation must not add broken
relative links to removed Python files.

## Product documentation

Current user and operator documentation lives in the repository [docs](../docs/)
directory and the embedded [operator guide](../assets/operator_guide/). The
files `start-here-ru.md`, `operator-guide-draft.md`, `architecture.md`, and
`dependencies.md` in this directory are migration-era pointers only.
