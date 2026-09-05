"""Regression tests for crash-safe workflow delivery leases."""

from __future__ import annotations

import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from agent_run.domain import OrchestratorRef
from agent_run.errors import ValidationError
from agent_run.state.store import StateStore


class WorkflowDeliveryLeaseTests(unittest.TestCase):
    """Prove expired latest notices recover without granting stale settlers."""

    def setUp(self) -> None:
        """Create one bound failed workflow notice in a private database."""

        self.temporary = tempfile.TemporaryDirectory()
        self.database = Path(self.temporary.name) / "state.db"
        self.store = StateStore.initialize(self.database)
        run_id = self.store.create_workflow_run(
            "named",
            "sha",
            plan=[],
            orchestrator=OrchestratorRef("stub", "session"),
            at=1,
        )
        self.store.start_workflow_run(run_id)
        self.store.finish_workflow_run(run_id, "failed", at=2)

    def tearDown(self) -> None:
        """Close SQLite before removing the private database."""

        self.store.close()
        self.temporary.cleanup()

    def test_crashed_sender_is_reclaimed_and_cannot_settle(self) -> None:
        """An expired claim keeps its id but only the new live owner may finish."""

        first = self.store.claim_workflow_delivery("sender-a", at=3, lease_seconds=30)
        assert first is not None
        delivery_id = str(first["id"])
        self.assertIsNone(
            self.store.claim_workflow_delivery("sender-b", at=32, lease_seconds=30)
        )
        for settle in (
            lambda: self.store.complete_workflow_delivery(
                delivery_id, "sender-a", at=33
            ),
            lambda: self.store.retry_workflow_delivery(
                delivery_id, "sender-a", "late", at=33
            ),
            lambda: self.store.fail_workflow_delivery(
                delivery_id, "sender-a", "late", at=33
            ),
        ):
            with self.assertRaisesRegex(ValidationError, "lease is not owned"):
                settle()

        second = self.store.claim_workflow_delivery(
            "sender-b", at=33, lease_seconds=30
        )
        assert second is not None
        self.assertEqual(second["id"], delivery_id)
        self.assertEqual(second["attempts"], 2)
        for settle in (
            lambda: self.store.complete_workflow_delivery(
                delivery_id, "sender-a", at=34
            ),
            lambda: self.store.retry_workflow_delivery(
                delivery_id, "sender-a", "stale", at=34
            ),
            lambda: self.store.fail_workflow_delivery(
                delivery_id, "sender-a", "stale", at=34
            ),
        ):
            with self.assertRaisesRegex(ValidationError, "lease is not owned"):
                settle()
        self.store.complete_workflow_delivery(
            delivery_id, "sender-b", remote_message_id="remote", at=34
        )
        row = self.store.connection.execute(
            "SELECT state, remote_message_id FROM workflow_deliveries WHERE id = ?",
            (delivery_id,),
        ).fetchone()
        self.assertEqual((row["state"], row["remote_message_id"]), ("delivered", "remote"))

    def test_only_one_concurrent_sender_reclaims_an_expired_lease(self) -> None:
        """SQLite ownership serializes competing reclaimers of the same row."""

        first = self.store.claim_workflow_delivery("crashed", at=3, lease_seconds=1)
        assert first is not None

        def claim(owner: str) -> dict[str, object] | None:
            """Claim through a thread-owned connection at the shared expiry."""

            store = StateStore.open(self.database)
            try:
                return store.claim_workflow_delivery(owner, at=4, lease_seconds=30)
            finally:
                store.close()

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = tuple(pool.map(claim, ("sender-b", "sender-c")))
        claimed = [row for row in results if row is not None]
        self.assertEqual(len(claimed), 1)
        self.assertEqual(claimed[0]["id"], first["id"])
        self.assertEqual(claimed[0]["attempts"], 2)


if __name__ == "__main__":
    unittest.main()
