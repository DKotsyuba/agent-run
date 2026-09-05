"""Regression tests for unowned asynchronous-start reconciliation."""

from __future__ import annotations

import tempfile
import unittest
import os
from pathlib import Path

from hypothesis import given, settings, strategies as st

from agent_run.domain import AgentStatus, StartRequest
from agent_run.errors import ValidationError
from agent_run.state.reconciliation import reconcile_unowned_starting
from agent_run.state.store import StateStore
from agent_run.supervisor import supervisor_identity


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
        self.assertEqual(
            self.store.get_agent(stale)["failure_text"],
            "accepted start exceeded its startup ownership deadline",
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

    def test_live_owner_is_bounded_by_startup_deadline(self) -> None:
        """A live broker protects preparation only before its fixed deadline."""

        agent_id = self.starting("owned-startup", at=10)
        self.store.claim_startup(
            agent_id,
            f"{os.getpid()} {supervisor_identity()}",
            at=10,
            deadline_seconds=120,
        )

        self.assertEqual(
            reconcile_unowned_starting(self.store, at=100, grace_seconds=30), ()
        )
        self.assertEqual(
            reconcile_unowned_starting(self.store, at=131, grace_seconds=30),
            (agent_id,),
        )

    def test_handoff_renews_deadline_until_late_supervisor_proof(self) -> None:
        """Atomic handoff prevents reconciliation between spawn and late READY."""

        owner = f"{os.getpid()} {supervisor_identity()}"
        agent_id = self.starting("handoff-ready", at=10)
        self.store.claim_startup(
            agent_id, owner, at=10, deadline_seconds=120
        )

        self.assertTrue(
            self.store.begin_supervisor_handoff(
                agent_id, owner, at=129, deadline_seconds=40
            )
        )
        self.assertEqual(
            reconcile_unowned_starting(self.store, at=131, grace_seconds=30), ()
        )
        self.store.record_supervisor(
            agent_id, pid=123, identity="identity", process_group_id=123, at=132
        )
        self.assertEqual(
            reconcile_unowned_starting(self.store, at=170, grace_seconds=30), ()
        )

    def test_expired_handoff_without_supervisor_proof_becomes_lost(self) -> None:
        """A handoff remains bounded when no child ever records ownership."""

        owner = f"{os.getpid()} {supervisor_identity()}"
        agent_id = self.starting("handoff-expired", at=10)
        self.store.claim_startup(
            agent_id, owner, at=10, deadline_seconds=120
        )
        self.assertTrue(
            self.store.begin_supervisor_handoff(
                agent_id, owner, at=129, deadline_seconds=10
            )
        )

        self.assertEqual(
            reconcile_unowned_starting(self.store, at=139, grace_seconds=30),
            (agent_id,),
        )

    @settings(max_examples=30, deadline=None)
    @given(
        preparation_delay=st.integers(min_value=1, max_value=9),
        handoff_extension=st.integers(min_value=0, max_value=10),
        ready_before_expiry=st.booleans(),
    )
    def test_generated_handoff_preserves_terminal_capacity_invariant(
        self,
        preparation_delay: int,
        handoff_extension: int,
        ready_before_expiry: bool,
    ) -> None:
        """Generated handoffs retain capacity through READY or release it on expiry."""

        with tempfile.TemporaryDirectory() as directory:
            store = StateStore.initialize(Path(directory) / "state.db")
            try:
                created = store.create_agent(
                    self.request("generated"),
                    task_summary="task",
                    config_revision="pending:materialization",
                    at=0,
                )
                agent_id = created.agent_id
                store.transition(agent_id, AgentStatus.STARTING, at=0)
                owner = f"{os.getpid()} {supervisor_identity()}"
                store.claim_startup(
                    agent_id, owner, at=0, deadline_seconds=10
                )
                handoff_seconds = 11 - preparation_delay + handoff_extension
                handoff_deadline = preparation_delay + handoff_seconds
                self.assertTrue(
                    store.begin_supervisor_handoff(
                        agent_id,
                        owner,
                        at=preparation_delay,
                        deadline_seconds=handoff_seconds,
                    )
                )
                self.assertEqual(
                    reconcile_unowned_starting(store, at=10, grace_seconds=0), ()
                )

                if ready_before_expiry:
                    store.record_supervisor(
                        agent_id,
                        pid=123,
                        identity="identity",
                        process_group_id=123,
                        at=handoff_deadline - 0.5,
                    )
                    self.assertEqual(
                        reconcile_unowned_starting(
                            store, at=handoff_deadline + 1, grace_seconds=0
                        ),
                        (),
                    )
                    with self.assertRaisesRegex(
                        ValidationError, "global active agent limit reached"
                    ):
                        store.create_agent_limited(
                            self.request("blocked"),
                            task_summary="task",
                            config_revision="pending:materialization",
                            global_limit=1,
                            runtime_limit=None,
                            at=handoff_deadline + 1,
                        )
                else:
                    self.assertEqual(
                        reconcile_unowned_starting(
                            store, at=handoff_deadline, grace_seconds=0
                        ),
                        (agent_id,),
                    )
                    replacement = store.create_agent_limited(
                        self.request("replacement"),
                        task_summary="task",
                        config_revision="pending:materialization",
                        global_limit=1,
                        runtime_limit=None,
                        at=handoff_deadline + 1,
                    )
                    self.assertTrue(replacement.created)
            finally:
                store.close()

    def test_expired_startup_cannot_bind_a_supervisor(self) -> None:
        """A delayed supervisor cannot revive an expired accepted start."""

        agent_id = self.starting("expired-bind", at=10)
        self.store.claim_startup(agent_id, "1 stale", at=10, deadline_seconds=1)

        with self.assertRaisesRegex(ValidationError, "expired startup"):
            self.store.record_supervisor(
                agent_id, pid=123, identity="identity", process_group_id=123, at=11
            )

    def test_owned_supervisor_can_refine_its_group_after_startup_expiry(self) -> None:
        """A completed handoff does not make later group refinement expire."""

        agent_id = self.starting("refine-after-expiry", at=10)
        self.store.claim_startup(agent_id, "1 owner", at=10, deadline_seconds=1)
        self.store.record_supervisor(
            agent_id, pid=123, identity="identity", process_group_id=123, at=10.5
        )

        self.store.record_supervisor(
            agent_id, pid=123, identity="identity", process_group_id=456, at=12
        )


if __name__ == "__main__":
    unittest.main()
