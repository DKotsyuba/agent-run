from __future__ import annotations

import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path

from agent_run.adapters.base import Capability
from agent_run.config import Config, ProfilesConfig, RuntimeConfig
from agent_run.delivery.base import DeliveryReceipt
from agent_run.delivery.dispatch import DeliveryDispatcher
from agent_run.domain import AgentStatus, Message, MessageRole, OrchestratorRef, Outcome, StartRequest
from agent_run.dispatch import Session, call_tool
from agent_run.errors import ValidationError
from agent_run.hooks.bind import BindHookError, bind
from agent_run.mcp import serve
from agent_run.paths import agent_dir
from agent_run.service import AgentQuery, AgentService
from agent_run.state.store import StateStore
from agent_run.supervisor import Supervisor, SupervisorSettings
from agent_run.verify import DEFAULT_SENTINEL
from tests.test_service import ADAPTER as SERVICE_ADAPTER
from tests.test_supervisor import FakeAdapter as EngineAdapter
from tests.test_supervisor import FakeOps, FakeSession


class RecordingTransport:
    name = "codex_queue"
    api_version = 1

    def __init__(self) -> None:
        self.sent = []

    def validate(self, config) -> None:
        return None

    def send(self, target, notice):
        self.sent.append((target, notice))
        return DeliveryReceipt("remote-1")


class M008IntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        SERVICE_ADAPTER.reset()
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.workdir = self.root / "work"
        self.runtime_home = self.root / "runtime"
        self.profiles = self.root / "profiles"
        for path in (self.workdir, self.runtime_home, self.profiles):
            path.mkdir()
        (self.profiles / "profile.md").write_text(
            "+++\nwrite = true\n+++\nDo the work.\n", encoding="utf-8"
        )
        self.config = Config(
            schema_version=1,
            profiles=ProfilesConfig(self.profiles),
            runtimes={
                "fake": RuntimeConfig(
                    True,
                    "tests.test_service:ADAPTER",
                    Path("/bin/true"),
                    self.runtime_home,
                    ("model",),
                )
            },
        )
        self.store = StateStore.initialize(self.root / "state.db")
        self.launches = []
        self.service = AgentService(
            self.config,
            self.store,
            self.root,
            launch=lambda *args: self.launches.append(args),
            now=lambda: 100.0,
        )

    def tearDown(self) -> None:
        self.store.close()
        self.temporary.cleanup()

    def request(self, request_id: str, task: str = "safe task") -> StartRequest:
        return StartRequest(
            "fake", "model", "profile", task, self.workdir, request_id=request_id
        )

    def mcp_call(self, service, request_id, name, arguments):
        class Broker:
            def __init__(self, target):
                self.target = target
                self.session = Session()

            def call(self, method, params=None, timeout=600):
                return call_tool(self.target, method, params or {}, self.session)

        source = StringIO(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "tools/call",
                    "params": {"name": name, "arguments": arguments},
                }
            )
            + "\n"
        )
        output = StringIO()
        self.assertEqual(serve(Broker(service), source, output), 0)
        return json.loads(output.getvalue())["result"]

    def test_async_start_supervisor_late_bind_and_one_trusted_dispatch(self) -> None:
        started = self.service.start(self.request("async-completion"))
        self.assertTrue(started.created)
        self.assertIs(started.agent.status, AgentStatus.CREATED)
        self.assertEqual(len(self.launches), 1, "start returns after the launch decision")

        agent_id, _request, _adapter, plan, directory = self.launches[0]
        self.assertTrue(directory.is_dir())
        answer_path = directory / "answer.md"
        answer_path.write_text(f"done\n{DEFAULT_SENTINEL}\n", encoding="utf-8")
        # The orchestrator binds before the agent finishes, so the terminal
        # transition creates a pending notice rather than one that can never bind.
        bind(
            self.store,
            agent_id,
            OrchestratorRef("codex_queue", "root-session", "turn-1"),
            at=100,
        )
        ops = FakeOps()
        session = FakeSession(
            ops, outcome=Outcome(AgentStatus.SUCCEEDED), exit_after_polls=1
        )
        outcome = Supervisor(
            self.store,
            agent_id,
            EngineAdapter(session),
            plan,
            answer_path=answer_path,
            timeout_seconds=10,
            settings=SupervisorSettings(
                poll_seconds=0.1,
                grace_seconds=0.1,
                kill_grace_seconds=0.1,
                natural_grace_seconds=0.1,
            ),
            ops=ops,
        ).run()
        self.assertIs(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(self.service.get(agent_id).delivery.state, "pending")

        ref = OrchestratorRef("codex_queue", "root-session", "turn-1")
        # A repeat of the same bind is idempotent; a different session is refused.
        bind(self.store, agent_id, ref, at=200)
        with self.assertRaises(BindHookError):
            bind(
                self.store,
                agent_id,
                OrchestratorRef("codex_queue", "other-session"),
                at=201,
            )

        transport = RecordingTransport()
        dispatcher = DeliveryDispatcher(
            self.store, {transport.name: transport}, owner="integration-dispatcher"
        )
        # The notice was created pending at the agent's own terminal timestamp,
        # so the dispatcher is run well past it.
        first = dispatcher.drain(at=10_000_000_000)
        second = dispatcher.drain(at=10_000_000_000)
        self.assertEqual((first.claimed, first.delivered), (1, 1))
        self.assertEqual(second.claimed, 0)
        self.assertEqual(len(transport.sent), 1)
        target, notice = transport.sent[0]
        self.assertEqual(target, ref)
        self.assertEqual(
            set(notice.payload()),
            {"version", "notification_id", "agent_id", "status"},
        )
        self.assertNotIn("safe task", notice.render())
        self.assertEqual(self.service.get(agent_id).delivery.state, "delivered")

    def test_real_service_mcp_preserves_counts_pagination_capacity_and_gate(self) -> None:
        ids = [self.service.start(self.request(f"active-{index}")).agent_id for index in range(3)]
        listed = self.mcp_call(
            self.service, 1, "list_agents", {"active": True, "limit": 2}
        )["structuredContent"]
        self.assertEqual(listed["total"], 3)
        self.assertEqual(len(listed["items"]), 2)
        self.assertFalse(listed["complete"])

        for index, content in enumerate(("one", "two", "three"), 1):
            self.store.append_message(
                ids[0],
                Message(
                    index,
                    MessageRole.TOOL_RESULT,
                    content,
                    raw_ref="raw/two.json" if index == 2 else None,
                ),
            )
        transcript = self.mcp_call(
            self.service,
            2,
            "transcript",
            {"agent_id": str(ids[0]), "cursor": 0, "limit": 2},
        )["structuredContent"]
        self.assertFalse(transcript["complete"])
        self.assertIsNotNone(transcript["next_cursor"])
        self.assertEqual(transcript["messages"][1]["raw_ref"], "raw/two.json")

        self.assertEqual(self.service.limits().items, (), "missing capacity stays unknown")
        self.store.insert_capacity_sample(
            runtime="fake",
            lane="main",
            window="5h",
            source="stale-test",
            payload={},
            remaining_percent=50,
            reset_at=200,
            observed_at=10,
            valid_until=50,
        )
        stale = self.service.limits().items[0]
        self.assertFalse(stale.known)
        self.assertEqual(stale.risk, "unknown")
        self.assertEqual(SERVICE_ADAPTER.limits_calls, 0)

        SERVICE_ADAPTER.capabilities = frozenset(
            capability for capability in Capability if capability is not Capability.STEER
        )
        refused = self.mcp_call(
            self.service,
            3,
            "steer",
            {"agent_id": str(ids[0]), "text": "finish"},
        )
        self.assertTrue(refused["isError"])
        self.assertEqual(
            self.store.connection.execute("SELECT COUNT(*) FROM commands").fetchone()[0],
            0,
        )

    def test_mcp_launch_failure_is_terminal_and_retry_does_not_relaunch(self) -> None:
        calls = []

        def fail_launch(*args):
            calls.append(args)
            raise ValidationError("ready failed")

        service = AgentService(
            self.config,
            self.store,
            self.root,
            launch=fail_launch,
            now=lambda: 100.0,
        )
        arguments = {
            "runtime": "fake",
            "model": "model",
            "profile": "profile",
            "task": "safe task",
            "workdir": str(self.workdir),
            "request_id": "mcp-launch-failure",
        }
        failed = self.mcp_call(service, 1, "start", arguments)
        self.assertTrue(failed["isError"])
        row = self.store.list_agents()[0]
        self.assertEqual((row["status"], row["failure_kind"]), ("failed", "supervisor_start_failed"))
        retried = self.mcp_call(service, 2, "start", arguments)
        self.assertFalse(retried["isError"])
        self.assertFalse(retried["structuredContent"]["created"])
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
