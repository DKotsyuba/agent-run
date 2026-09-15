# Python reference pytest baseline

Command:

```text
PYTHONPATH=src python3.14 -m pytest -q --rootdir . tests
```

Exit status: `0`

Result: `1146 passed, 1 skipped, 5 warnings, 427 subtests passed in 76.93s (0:01:16)`

Failures: 0. Errors: 0.

Skipped tests:

- `tests/test_supervisor_main.py::SupervisorMainTests::test_recorded_identity_matches_the_exec_command_line` — no macOS Framework Python build installed.

`tests/test_launch.py` had no failures, so no isolated launch-module rerun was required. The final output tail is in `migration/evidence/python-baseline-pytest.log`.
