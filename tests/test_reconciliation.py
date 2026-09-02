"""Regression tests for unowned asynchronous-start reconciliation."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from agent_run.domain import AgentStatus, StartRequest
from agent_run.errors import ValidationError
from agent_run.state.reconciliation import reconcile_unowned_starting
from agent_run.state.store import StateStore


class UnownedStartingReconciliationTests(unittest.TestCase):
    """Prove stale identity-less starts converge without touching owned rows."""

    def setUp(self) -> None:
        """Create one private current-schema store and work directory."""

        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.workdir = self.root / "work"
        self.workdir.mkdir()
        self.store = StateStore.initialize(self.root / "state.db")

    def tearDown(self) -> None:
        """Close SQLite before deleting the temporary home."""

        self.store.close()
        self.temporary.cleanup()

    def request(self, request_id: str) -> StartRequest:
        """Build one valid request with a distinct idempotency key."""

        return StartRequest(
            "fake",
            "model",
            "profile",
            "task",
            self.workdir,
            timeout_seconds=60,
            request_id=request_id,
        )

    def starting(self, request_id: str, *, at: float) -> str:
        """Create one agent and durably advance it to ``STARTING``."""

        created = self.store.create_agent(
            self.request(request_id),
            task_summary="task",
            config_revision="pending:materialization",
            at=at,
        )
        self.store.transition(created.agent_id, AgentStatus.STARTING, at=at)
        return str(created.agent_id)

    def test_only_stale_identity_less_starting_rows_become_lost(self) -> None:
        """Recent and supervisor-owned rows must survive the convergence sweep."""

        stale = self.starting("stale", at=10)
        recent = self.starting("recent", at=90)
        owned = self.starting("owned", at=10)
        self.store.record_supervisor(
            owned, pid=123, identity="identity", process_group_id=123
        )

        changed = reconcile_unowned_starting(
            self.store, at=100, grace_seconds=30
        )

        self.assertEqual(changed, (stale,))
        self.assertEqual(self.store.get_agent(stale)["status"], "lost")
        self.assertEqual(
            self.store.get_agent(stale)["failure_kind"], "unowned_starting"
        )
        self.assertEqual(self.store.get_agent(recent)["status"], "starting")
        self.assertEqual(self.store.get_agent(owned)["status"], "starting")
        self.assertEqual(
            reconcile_unowned_starting(self.store, at=101, grace_seconds=30), ()
        )

    def test_lost_convergence_releases_active_capacity(self) -> None:
        """A dead pre-ownership row must not exhaust the active-agent limit."""

        self.starting("first", at=10)
        with self.assertRaisesRegex(
            ValidationError, "global active agent limit reached"
        ):
            self.store.create_agent_limited(
                self.request("blocked"),
                task_summary="task",
                config_revision="pending:materialization",
                global_limit=1,
                runtime_limit=None,
                at=100,
            )

        reconcile_unowned_starting(self.store, at=100, grace_seconds=30)
        admitted = self.store.create_agent_limited(
            self.request("admitted"),
            task_summary="task",
            config_revision="pending:materialization",
            global_limit=1,
            runtime_limit=None,
            at=101,
        )
        self.assertTrue(admitted.created)


if __name__ == "__main__":
    unittest.main()
