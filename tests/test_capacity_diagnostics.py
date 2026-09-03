"""Keep stale capacity identities visible despite frequent healthy samples."""

import tempfile
import unittest
from pathlib import Path

from agent_run.config import Config
from agent_run.doctor import _capacity
from agent_run.state import StateStore, diagnostic_snapshot


class CapacityDiagnosticTests(unittest.TestCase):
    """Check the diagnostic limit applies to identities, not repeated samples."""

    def test_frequent_healthy_samples_do_not_hide_stale_identity(self) -> None:
        """A stale account still reaches doctor when a sibling fills the cap."""
        with tempfile.TemporaryDirectory() as directory:
            database = Path(directory) / "state.db"
            store = StateStore.initialize(database)
            try:
                for target, observed, valid in (
                    ("quiet", 1.0, 2.0),
                    ("busy", 10.0, 200.0),
                    ("busy", 20.0, 200.0),
                    ("busy", 20.0, 200.0),
                ):
                    store.insert_capacity_sample(
                        runtime="codex", lane="codex", window="seven_day",
                        target=target, source="codex_appserver", payload={},
                        observed_at=observed, valid_until=valid,
                    )
                snapshot = diagnostic_snapshot(database, at=100.0, limit=2)
            finally:
                store.close()
        self.assertEqual([row["target"] for row in snapshot.capacity], ["busy", "quiet"])
        self.assertEqual(snapshot.capacity[0]["id"], 4)
        findings = []
        _capacity(Config(schema_version=1), snapshot.capacity, 100.0, findings)
        self.assertEqual([finding.code for finding in findings], ["capacity_stale"])
        self.assertIn("quiet", findings[0].detail)
