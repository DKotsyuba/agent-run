"""Diagnose freshness from current capacity snapshots."""

import tempfile
import unittest
from pathlib import Path

from agent_run.config import Config
from agent_run.doctor import _capacity
from agent_run.state import StateStore, diagnostic_snapshot


class CapacityDiagnosticTests(unittest.TestCase):
    """Check current scopes retain independent freshness."""

    def test_frequent_healthy_samples_do_not_hide_stale_identity(self) -> None:
        """A stale account still reaches doctor when a sibling fills the cap."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            store = StateStore.initialize(database)
            try:
                for scope, observed, valid in (("quiet", 1.0, 2.0), ("busy", 20.0, 200.0)):
                    store.replace_capacity_snapshot(
                        runtime="codex", scope_id=scope, payload={"samples": [], "pools": [], "routes": []},
                        observed_at=observed, valid_until=valid,
                    )
                snapshot = diagnostic_snapshot(database, at=100.0, limit=2)
            finally:
                store.close()
        self.assertEqual([row["scope_id"] for row in snapshot.capacity], ["busy", "quiet"])
        findings = []
        _capacity(Config(schema_version=1), snapshot.capacity, 100.0, findings)
        self.assertEqual([finding.code for finding in findings], ["capacity_stale"])
        self.assertEqual(findings[0].detail, "quiet")
