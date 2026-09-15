# Python baseline corpus

This is the sanitized Python reference corpus for Rust migration plan P0
(backlog M01–M03). It was captured from Python baseline commit
`c9904f9843ba4a0772bdfa8bac5259f18fad9dc3`, which is an ancestor of BASE
`31f723ba881fe0e35146a7262103c89737dd52ef`; `src/` and `tests/` were unchanged
relative to that commit.

Regenerate the assigned fixtures from the worktree root with:

```text
PYTHONPATH=src AGENT_RUN_HOME=/private/tmp/agent-run-baseline-home HOME=/private/tmp/agent-run-baseline-home python3.14 migration/tools/capture_baseline.py
```

The generator derives CLI nodes from the live argparse parser, copies the live
dispatch/API declarations, and calls the real config, notice, and capacity
functions. Paths under temporary homes become `${TMP_HOME}` and the worktree
becomes `${WORKTREE}`. No real agent-run home, credentials, task text, answer
content, network state, session IDs, or secrets were read or persisted.

The corpus contains 33 CLI command nodes, 11 tools, 14 socket methods, 47
config/profile cases, 11 notice cases, and 8 capacity cases. Pytest evidence is
kept separately in `migration/evidence/python-baseline-pytest.log`.
