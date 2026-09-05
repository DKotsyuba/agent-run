# Contributing

agent-run is a Python 3.14+ standard-library runtime. Runtime dependencies are
not accepted; `pytest` and packaging tools are development-only.

```bash
python3.14 -m venv .venv-py314
.venv-py314/bin/python -m pip install -e ".[test,release]"
.venv-py314/bin/python -m pytest -q --rootdir . tests
```

If a timing-sensitive test in `tests/test_launch.py` fails under load, rerun
that module in isolation before treating it as a product failure. Never weaken
its assertions to make CI pass.

Keep changes focused, add tests for changed behaviour, preserve typed errors,
and follow the invariants in [AGENTS.md](AGENTS.md). A pull request should state
the problem, the verification command and result, and any platform-specific
validation that was not run.
