import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.errors import PathEscapeError, ValidationError
from agent_run.paths import agent_dir, agent_run_home, config_path, state_db_path


AGENT_ID = "ag-20260825-010203-0123456789"


class PathTests(unittest.TestCase):
    def test_home_uses_environment_and_returns_resolved_paths(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            expected = Path(directory).resolve()
            with patch.dict(os.environ, {"AGENT_RUN_HOME": directory}):
                self.assertEqual(agent_run_home(), expected)
                self.assertEqual(config_path(), expected / "config.toml")
                self.assertEqual(state_db_path(), expected / "state.db")
                self.assertEqual(agent_dir(AGENT_ID), expected / "agents" / AGENT_ID)

    def test_blank_and_unresolved_home_are_rejected(self) -> None:
        with self.assertRaises(ValidationError):
            agent_run_home("")
        with self.assertRaises(ValidationError):
            agent_run_home("~this-user-must-not-exist/.agent-run")

    def test_agent_directory_cannot_escape_through_symlink(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            home = Path(directory)
            (home / "agents").mkdir()
            (home / "agents" / AGENT_ID).symlink_to(outside, target_is_directory=True)
            with self.assertRaises(PathEscapeError):
                agent_dir(AGENT_ID, home)

    def test_agent_directory_rejects_traversal_id(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(ValidationError):
                agent_dir("../outside", directory)


if __name__ == "__main__":
    unittest.main()
