"""Regression coverage for the checked-in CI test retry block."""

from __future__ import annotations

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path


class CiRetryTests(unittest.TestCase):
    """Ensure retries preserve the first test suite's exit status."""

    def test_run_tests_block_preserves_first_failure_and_runs_diagnostics(self) -> None:
        """Execute the literal workflow block against a counting fake Python."""

        workflow = Path(__file__).parents[1] / ".github/workflows/ci.yml"
        block = self._run_tests_block(workflow.read_text(encoding="utf-8"))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake_bin = root / "bin"
            fake_bin.mkdir()
            count = root / "count"
            fake_python = fake_bin / "python"
            fake_python.write_text(
                "#!/bin/sh\n"
                "count=\"$FAKE_COUNT_FILE\"\n"
                "calls=$(cat \"$count\" 2>/dev/null || echo 0)\n"
                "calls=$((calls + 1)); echo \"$calls\" > \"$count\"\n"
                "if [ \"$calls\" -eq 1 ] && { [ \"$FAKE_MODE\" = firstfail ] || [ \"$FAKE_MODE\" = diagfail ]; }; then exit 7; fi\n"
                "if [ \"$FAKE_MODE\" = diagfail ] && [ \"$calls\" -eq 2 ]; then exit 9; fi\n"
                "exit 0\n",
                encoding="utf-8",
            )
            fake_python.chmod(0o755)
            environment = {
                **os.environ,
                "PATH": str(fake_bin) + os.pathsep + os.environ["PATH"],
                "RUNNER_OS": "Linux",
                "FAKE_COUNT_FILE": str(count),
            }
            for mode, expected_status, expected_calls in (
                ("allpass", 0, 1),
                ("firstfail", 7, 3),
                ("diagfail", 7, 3),
            ):
                count.unlink(missing_ok=True)
                result = subprocess.run(
                    ["bash", "--noprofile", "--norc", "-e", "-o", "pipefail", "-c", block],
                    env={**environment, "FAKE_MODE": mode},
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, expected_status, msg=result.stderr)
                self.assertEqual(int(count.read_text(encoding="utf-8")), expected_calls)

    def test_release_workflows_publish_verified_lock_before_smoke_and_checksums(self) -> None:
        """Check the lock generation, hash install, smoke, and publication order graph."""

        ci = (Path(__file__).parents[1] / ".github/workflows/ci.yml").read_text(encoding="utf-8")
        release = (Path(__file__).parents[1] / ".github/workflows/release.yml").read_text(encoding="utf-8")
        for workflow in (ci, release):
            self.assertIn("python -m pip install uv==0.11.1", workflow)
            self.assertIn("uv lock --check", workflow)
            self.assertIn("uv export --frozen --no-dev --no-editable --no-emit-project --format requirements-txt --output-file dist/requirements.lock", workflow)
            self.assertIn("-m pip install --require-hashes --only-binary=:all: -r dist/requirements.lock", workflow)
            self.assertIn("-m pip check", workflow)
            self.assertLess(workflow.index("uv export --frozen"), workflow.index("requirements.lock"))
            self.assertLess(workflow.index("requirements.lock"), workflow.index("-m pip check"))
        self.assertIn("requirements.lock > SHA256SUMS", release)
        self.assertIn("dist/requirements.lock dist/SHA256SUMS", release)

    @staticmethod
    def _run_tests_block(workflow: str) -> str:
        """Extract the indented bash block belonging to the Run tests step."""

        match = re.search(
            r"(?ms)^      - name: Run tests\n.*?^        run: \|\n(?P<body>.*?)(?=^      - name: )",
            workflow,
        )
        if match is None:
            raise AssertionError("Run tests block missing from CI workflow")
        return "".join(
            line[10:] if line.startswith("          ") else line for line in match.group("body").splitlines(True)
        )
