"""Import-boundary tests for the standalone terminal dashboard package."""

from __future__ import annotations

import ast
import sys
import unittest
from pathlib import Path


class BoundaryTests(unittest.TestCase):
    """Ensure the dashboard uses only itself and Python's standard library."""

    def test_tui_has_no_core_or_third_party_imports(self) -> None:
        """Walk package modules and reject core or non-stdlib absolute imports."""
        package = Path(__file__).parents[1] / "src" / "agent_run_tui"
        allowed = set(sys.stdlib_module_names) | {"__future__", "agent_run_tui"}
        for path in package.glob("*.py"):
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
            for node in ast.walk(tree):
                names = []
                if isinstance(node, ast.Import):
                    names = [alias.name for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module and not node.level:
                    names = [node.module]
                for name in names:
                    root = name.split(".", 1)[0]
                    self.assertNotEqual(root, "agent_run", f"{path}: {name}")
                    self.assertIn(root, allowed, f"{path}: {name}")
