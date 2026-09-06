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

Some transport tests bind temporary local Unix sockets. If a sandbox denies
`bind`, check the fixture permission with the same interpreter and sandbox:

```bash
.venv-py314/bin/python - <<'PY'
import socket
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory(prefix="ar-check-") as directory:
    with socket.socket(socket.AF_UNIX) as server:
        server.bind(str(Path(directory) / "test.sock"))
print("AF_UNIX fixture bind: OK")
PY
```

A `PermissionError` here identifies an environment restriction, not a failed
delivery assertion. Use the host's normal approval path for the exact local
fixture command if needed, then rerun the affected tests under that permission:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:tests .venv-py314/bin/python -m unittest \
  test_bind_hook test_state_outbox test_codex_queue test_claude_uds
```

These fixtures must use temporary endpoints, never real agent inboxes. A local
socket permission does not authorize external delivery or a model launch.
Report the restricted and permitted results separately; do not convert socket
errors to skips, suppress assertions, or classify every socket error as a
sandbox denial. Run the full suite for code changes once its required fixture
permissions are available.

Keep changes focused, add tests for changed behaviour, preserve typed errors,
and follow the invariants in [AGENTS.md](AGENTS.md). A pull request should state
the problem, the verification command and result, and any platform-specific
validation that was not run.
