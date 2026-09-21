# Python baseline corpus

This is the sanitized Python reference corpus for the Rust port. It was captured from Python baseline commit
`c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`, which is an ancestor of BASE
`31f723ba881fe0e35146a7262103c89737dd52ef`; `src/` and `tests/` were unchanged
relative to that commit.

These fixtures are immutable historical inputs on the Rust-primary branch.
Regeneration is allowed only from the frozen `archive/python-legacy` branch,
using that branch's documented generator and an isolated temporary home. Copy
an accepted regenerated corpus back only through a separately reviewed change.

```text
git switch archive/python-legacy
# Follow the frozen branch's baseline-corpus instructions.
```

The generator derives CLI nodes from the live argparse parser, copies the live
dispatch/API declarations, and calls the real config, notice, and capacity
functions. Paths under temporary homes become `${TMP_HOME}` and the worktree
becomes `${WORKTREE}`. No real agent-run home, credentials, task text, answer
content, network state, session IDs, or secrets were read or persisted.

The corpus contains 33 CLI command nodes, 11 tools, 14 socket methods, 47
config/profile cases, 11 notice cases, and 8 capacity cases. The original Pytest log
is preserved only in Git history.
