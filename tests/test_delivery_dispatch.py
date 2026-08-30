import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.config import DeliveryConfig
from agent_run.domain import AgentStatus, OrchestratorRef, Outcome, StartRequest
from agent_run.state import StateStore
from agent_run.delivery.base import (
    AmbiguousDeliveryError,
    DeliveryError,
    DeliveryReceipt,
)
from agent_run.delivery.dispatch import DeliveryDispatcher, dispatcher_lock


import time
from unittest import mock

from agent_run.errors import ValidationError


class FakeTransport:
    name = "codex_queue"
    api_version = 1

    def __init__(self, *behaviors) -> None:
        self.behaviors = list(behaviors)
        self.calls: list[tuple[OrchestratorRef, object]] = []
        self.validated = []

    def validate(self, config) -> None:
        self.validated.append(config)

    def send(self, target, notice) -> DeliveryReceipt:
        self.calls.append((target, notice))
        behavior = self.behaviors[min(len(self.calls), len(self.behaviors)) - 1]
        if isinstance(behavior, Exception):
            raise behavior
        if callable(behavior):
            return behavior(target, notice)
        return behavior


class DeliveryDispatchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.store = StateStore.initialize(self.root / "state.db")
        self.lock_path = self.root / "locks" / "delivery-dispatcher.lock"
        self.agent_id = self.finished_agent()

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def finished_agent(self) -> str:
        request = StartRequest(
            "codex", "model", "profile", "task", self.root, timeout_seconds=480
        )
        agent_id = self.store.create_agent(
            request, task_summary="summary", config_revision="cfg-1", at=1
        ).agent_id
        # An orchestrator-backed start binds its session before the agent runs,
        # so the completion notice is created pending rather than waiting.
        self.store.bind_orchestrator(
            agent_id, OrchestratorRef("codex_queue", "session-1", "turn-1"), at=2
        )
        self.store.transition(agent_id, AgentStatus.STARTING, at=3)
        self.store.transition(agent_id, AgentStatus.RUNNING, at=4)
        self.store.transition(
            agent_id, AgentStatus.SUCCEEDED, outcome=Outcome(AgentStatus.SUCCEEDED), at=5
        )
        return agent_id

    def delivery_row(self) -> dict:
        return dict(
            self.store.connection.execute(
                "SELECT * FROM deliveries WHERE agent_id = ?", (self.agent_id,)
            ).fetchone()
        )

    def dispatcher(
        self, transport, config=None, transports=None, lease_seconds=10
    ) -> DeliveryDispatcher:
        return DeliveryDispatcher(
            self.store,
            {transport.name: transport} if transports is None else transports,
            config,
            lease_seconds=lease_seconds,
        )

    def test_pending_notice_is_delivered_once_and_the_outbox_then_rests(self) -> None:
        transport = FakeTransport(DeliveryReceipt("remote-1"))
        dispatcher = self.dispatcher(transport)
        result = dispatcher.run(lock_path=self.lock_path, at=10)
        self.assertEqual((result.claimed, result.delivered), (1, 1))
        self.assertFalse(result.locked_out)

        target, notice = transport.calls[0]
        row = self.delivery_row()
        self.assertEqual(target, OrchestratorRef("codex_queue", "session-1", "turn-1"))
        self.assertEqual(notice.notification_id, row["id"])
        self.assertEqual((notice.agent_id, notice.status), (self.agent_id, AgentStatus.SUCCEEDED))
        self.assertEqual(row["state"], "delivered")
        self.assertEqual(row["remote_message_id"], "remote-1")
        self.assertIsNone(row["lease_owner"])

        again = dispatcher.run(lock_path=self.lock_path, at=11)
        self.assertEqual((again.claimed, len(transport.calls)), (0, 1))

    def test_trigger_drains_bounded_backlog_and_overflow_is_recoverable(self) -> None:
        self.finished_agent()
        self.finished_agent()
        transport = FakeTransport(DeliveryReceipt("remote"))
        dispatcher = self.dispatcher(transport)

        drained = dispatcher.run(lock_path=self.lock_path, at=10)

        self.assertEqual((drained.claimed, drained.delivered), (3, 3))
        self.assertEqual(len(transport.calls), 3)

        self.finished_agent()
        self.finished_agent()
        bounded = dispatcher.run(lock_path=self.lock_path, at=11, max_batch=1)
        recovered = dispatcher.run(lock_path=self.lock_path, at=11)
        self.assertEqual((bounded.claimed, recovered.claimed), (1, 1))

    def test_ambiguous_timeout_retries_with_capped_backoff_and_stays_durable(self) -> None:
        transport = FakeTransport(AmbiguousDeliveryError("unknown acceptance"))
        config = DeliveryConfig(retry_base_seconds=2, retry_cap_seconds=3, max_attempts=0)
        dispatcher = self.dispatcher(transport, config)

        result = dispatcher.run(lock_path=self.lock_path, at=10)
        self.assertEqual((result.retried, result.ambiguous, result.failed), (1, 1, 0))
        row = self.delivery_row()
        self.assertEqual(row["state"], "retry_wait")
        self.assertEqual(row["next_attempt_at"], 12)
        self.assertEqual(row["ambiguous_result"], 1)

        self.assertEqual(dispatcher.run(lock_path=self.lock_path, at=11).claimed, 0)
        dispatcher.run(lock_path=self.lock_path, at=12)
        self.assertEqual(self.delivery_row()["next_attempt_at"], 15)
        dispatcher.run(lock_path=self.lock_path, at=15)
        # Backoff stays capped and unlimited retries never turn into a failure.
        self.assertEqual(self.delivery_row()["next_attempt_at"], 18)
        self.assertEqual(self.delivery_row()["state"], "retry_wait")
        self.assertEqual(self.delivery_row()["attempts"], 3)

    def test_exhausted_attempt_budget_fails_the_delivery(self) -> None:
        transport = FakeTransport(DeliveryError("queue refused"))
        config = DeliveryConfig(retry_base_seconds=1, retry_cap_seconds=1, max_attempts=2)
        dispatcher = self.dispatcher(transport, config)

        first = dispatcher.run(lock_path=self.lock_path, at=10)
        self.assertEqual((first.retried, first.failed), (1, 0))
        second = dispatcher.run(lock_path=self.lock_path, at=11)
        self.assertEqual((second.retried, second.failed, second.ambiguous), (0, 1, 0))
        row = self.delivery_row()
        self.assertEqual(row["state"], "failed")
        self.assertEqual(row["last_error"], "queue refused")
        self.assertIsNone(row["lease_owner"])
        self.assertEqual(dispatcher.run(lock_path=self.lock_path, at=10_000).claimed, 0)

    def test_unknown_transport_is_a_permanent_configuration_failure(self) -> None:
        transport = FakeTransport(DeliveryReceipt("remote-1"))
        dispatcher = self.dispatcher(transport, transports={"slack": transport})
        result = dispatcher.run(lock_path=self.lock_path, at=10)
        self.assertEqual((result.retried, result.failed), (0, 1))
        self.assertEqual(transport.calls, [])
        self.assertEqual(self.delivery_row()["state"], "failed")
        self.assertEqual(
            self.delivery_row()["last_error"], "delivery transport is not configured"
        )

    def test_unexpected_transport_error_is_sanitized_and_releases_claim(self) -> None:
        transport = FakeTransport(RuntimeError("secret sender detail"))
        dispatcher = self.dispatcher(transport, DeliveryConfig(max_attempts=1))
        result = dispatcher.run(lock_path=self.lock_path, at=10)
        row = self.delivery_row()
        self.assertEqual((result.failed, result.ambiguous), (1, 1))
        self.assertEqual(row["state"], "failed")
        self.assertEqual(row["last_error"], "transport send raised RuntimeError")
        self.assertNotIn("secret", row["last_error"])
        self.assertIsNone(row["lease_owner"])

    def test_send_longer_than_lease_is_ambiguous_and_reclaimable(self) -> None:
        def slow_send(_target, _notice):
            time.sleep(0.03)
            return DeliveryReceipt("maybe-delivered")

        result = self.dispatcher(
            FakeTransport(slow_send), lease_seconds=0.01
        ).run(lock_path=self.lock_path)
        self.assertEqual((result.claim_lost, result.ambiguous, result.failed), (1, 1, 0))
        self.assertEqual(self.delivery_row()["state"], "sending")

        reclaimed = self.dispatcher(FakeTransport(DeliveryReceipt("remote-2"))).run(
            lock_path=self.lock_path
        )
        self.assertEqual(reclaimed.delivered, 1)
        self.assertEqual(self.delivery_row()["state"], "delivered")

    def test_cancel_during_send_is_terminal_and_does_not_retry(self) -> None:
        def cancel(_target, _notice):
            self.store.cancel_delivery(self.delivery_row()["id"])
            return DeliveryReceipt("late-ack")

        result = self.dispatcher(FakeTransport(cancel)).run(
            lock_path=self.lock_path, at=10
        )
        self.assertEqual((result.claim_lost, result.retried, result.failed), (1, 0, 0))
        self.assertEqual(self.delivery_row()["state"], "cancelled")
        self.assertEqual(
            self.dispatcher(FakeTransport(DeliveryReceipt("unused"))).run(
                lock_path=self.lock_path, at=100
            ).claimed,
            0,
        )

    def test_constructor_validates_config_and_every_transport(self) -> None:
        transport = FakeTransport(DeliveryReceipt("remote-1"))
        config = DeliveryConfig()
        self.dispatcher(transport, config)
        self.assertEqual(transport.validated, [config])
        for invalid in (
            DeliveryConfig(retry_base_seconds=0),
            DeliveryConfig(retry_base_seconds=2, retry_cap_seconds=1),
            DeliveryConfig(max_attempts=-1),
        ):
            with self.assertRaises(ValidationError):
                self.dispatcher(transport, invalid)

    def test_unsupported_flock_is_loud_not_contention(self) -> None:
        dispatcher = self.dispatcher(FakeTransport(DeliveryReceipt("remote-1")))
        with mock.patch(
            "agent_run.delivery.dispatch.fcntl.flock",
            side_effect=OSError(95, "operation not supported"),
        ):
            with self.assertRaises(DeliveryError):
                dispatcher.run(lock_path=self.lock_path, at=10)

    def test_only_one_dispatcher_runs_at_a_time(self) -> None:
        transport = FakeTransport(DeliveryReceipt("remote-1"))
        dispatcher = self.dispatcher(transport)
        with dispatcher_lock(self.lock_path) as owned:
            self.assertTrue(owned)
            blocked = dispatcher.run(lock_path=self.lock_path, at=10)
        self.assertTrue(blocked.locked_out)
        self.assertEqual((blocked.claimed, transport.calls), (0, []))
        self.assertEqual(self.delivery_row()["state"], "pending")

        released = dispatcher.run(lock_path=self.lock_path, at=10)
        self.assertEqual((released.delivered, released.locked_out), (1, False))

    def test_cancelled_delivery_stops_retries_and_preserves_the_result(self) -> None:
        transport = FakeTransport(DeliveryReceipt("remote-1"))
        dispatcher = self.dispatcher(transport)
        self.assertTrue(self.store.cancel_delivery(self.delivery_row()["id"]))

        result = dispatcher.run(lock_path=self.lock_path, at=10)
        self.assertEqual((result.claimed, transport.calls), (0, []))
        self.assertEqual(self.delivery_row()["state"], "cancelled")
        agent = self.store.get_agent(self.agent_id)
        self.assertEqual(agent["status"], "succeeded")
        self.assertEqual(agent["finished_at"], 5)

    def unbound_row(self, delivery_id: str = "ntf_unbound") -> dict:
        return dict(
            self.store.connection.execute(
                "SELECT * FROM deliveries WHERE id = ?", (delivery_id,)
            ).fetchone()
        )

    def plant_waiting_binding(self) -> str:
        """Create a start with no orchestrator ref and plant the never-bound
        notice a pre-T36B binary would have created for it."""

        request = StartRequest(
            "codex", "model", "profile", "task", self.root, timeout_seconds=480
        )
        agent_id = self.store.create_agent(
            request, task_summary="summary", config_revision="cfg-1", at=1
        ).agent_id
        self.store.transition(agent_id, AgentStatus.STARTING, at=2)
        self.store.transition(agent_id, AgentStatus.RUNNING, at=3)
        self.store.transition(
            agent_id, AgentStatus.SUCCEEDED, outcome=Outcome(AgentStatus.SUCCEEDED), at=4
        )
        seq = self.store.connection.execute(
            "SELECT MAX(seq) FROM events WHERE agent_id = ?", (agent_id,)
        ).fetchone()[0]
        self.store.connection.execute(
            """INSERT INTO deliveries
               (id, agent_id, orchestrator_session_id, terminal_event_seq, state)
               VALUES ('ntf_unbound', ?, NULL, ?, 'waiting_binding')""",
            (agent_id, seq),
        )
        self.store.connection.commit()
        return "ntf_unbound"

    def test_never_bound_notice_for_a_terminal_agent_expires_after_the_window(self) -> None:
        transport = FakeTransport(DeliveryReceipt("remote-1"))
        dispatcher = self.dispatcher(transport)
        delivery_id = self.plant_waiting_binding()

        with self.assertLogs("agent_run.delivery", level="INFO") as logged:
            first = dispatcher.drain(at=4 + 3600)
        self.assertEqual(
            [
                record.getMessage()
                for record in logged.records
                if "expired" in record.getMessage()
            ],
            [
                "dispatch expired delivery_id=%s reason=binding_window_elapsed"
                % delivery_id
            ],
        )
        self.assertEqual(self.unbound_row()["state"], "expired")
        # Only the bound notice from setUp was dispatched; the expired one is
        # never claimed again, sent, or logged on any later rescan.
        self.assertEqual(first.claimed, 1)
        with self.assertNoLogs("agent_run.delivery", level="INFO"):
            second = dispatcher.drain(at=4 + 7200)
        self.assertEqual(
            (second.claimed, len(transport.calls)),
            (0, 1),
        )

    def test_never_bound_notice_inside_the_window_is_left_to_bind(self) -> None:
        transport = FakeTransport(DeliveryReceipt("remote-1"))
        dispatcher = self.dispatcher(transport)
        self.plant_waiting_binding()

        dispatcher.drain(at=4 + 3599)

        self.assertEqual(self.unbound_row()["state"], "waiting_binding")

    def test_never_bound_notice_for_a_running_agent_never_expires(self) -> None:
        transport = FakeTransport(DeliveryReceipt("remote-1"))
        dispatcher = self.dispatcher(transport)
        request = StartRequest(
            "codex", "model", "profile", "task", self.root, timeout_seconds=480
        )
        agent_id = self.store.create_agent(
            request, task_summary="summary", config_revision="cfg-1", at=1
        ).agent_id
        self.store.transition(agent_id, AgentStatus.STARTING, at=2)
        self.store.transition(agent_id, AgentStatus.RUNNING, at=3)
        self.store.connection.execute(
            """INSERT INTO deliveries
               (id, agent_id, orchestrator_session_id, terminal_event_seq, state)
               VALUES ('ntf_unbound', ?, NULL,
                       (SELECT MAX(seq) FROM events WHERE agent_id = ?),
                       'waiting_binding')""",
            (agent_id, agent_id),
        )
        self.store.connection.commit()

        dispatcher.drain(at=3 + 100000)

        row = dict(
            self.store.connection.execute(
                "SELECT * FROM deliveries WHERE id = 'ntf_unbound'"
            ).fetchone()
        )
        self.assertEqual(row["state"], "waiting_binding")

    def test_bound_deliveries_are_never_expired_by_the_sweep(self) -> None:
        transport = FakeTransport(DeliveryReceipt("remote-1"))
        dispatcher = self.dispatcher(transport)
        self.plant_waiting_binding()
        self.assertEqual(self.delivery_row()["state"], "pending")

        # Far past the window: the bound notice still dispatches normally and
        # the sweep leaves it alone.
        result = dispatcher.drain(at=5 + 100000)

        self.assertEqual((result.claimed, result.delivered), (1, 1))
        self.assertEqual(self.delivery_row()["state"], "delivered")
        self.assertEqual(self.unbound_row()["state"], "expired")
        self.assertEqual(len(transport.calls), 1)


if __name__ == "__main__":
    unittest.main()
