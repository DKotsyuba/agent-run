from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from io import StringIO
from pathlib import Path

from mcp.types import LATEST_PROTOCOL_VERSION

from agent_run.adapters.base import Capability
from agent_run.api_socket import ApiServer
from agent_run.broker_client import BrokerClient
from agent_run.config import Config, ProfilesConfig, RuntimeConfig
from agent_run.domain import Message, MessageRole, StartRequest
from agent_run.errors import ValidationError
from agent_run.mcp import serve
from agent_run.paths import agent_dir
from agent_run.preparation import prepare_launch
from agent_run.service import AgentQuery, AgentService
from agent_run.state.store import StateStore
from tests.test_service import ADAPTER as SERVICE_ADAPTER


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
            now=time.time,
        )

    def tearDown(self) -> None:
        self.service.close()
        self.temporary.cleanup()

    def request(self, request_id: str, task: str = "safe task") -> StartRequest:
        return StartRequest(
            "fake", "model", "profile", task, self.workdir, request_id=request_id
        )

    def mcp_call(self, service, request_id, name, arguments):
        """Invoke one MCP tool through initialization and the real Unix broker."""

        class _DelayedEofInput(StringIO):
            """Keep the official server alive until its concurrent tool call completes."""

            def read(self, size=-1):
                """Return buffered frames, then delay the EOF that stops the SDK runner."""
                value = super().read(size)
                if not value:
                    time.sleep(0.2)
                return value

        socket_path = self.root / f"mcp-{request_id}-{time.time_ns()}.sock"
        store_path = service._store.path()
        config = service._config
        home = service._home
        launch = service._launch
        now = service._now

        def service_factory():
            """Create the broker-owned service and SQLite connection on its owner thread."""
            return AgentService(
                config,
                StateStore.initialize(store_path),
                home,
                launch=launch,
                now=now,
            )

        server = ApiServer(socket_path, service_factory)
        server_thread = threading.Thread(target=server.serve_forever, daemon=True)
        server_thread.start()
        initialize = {
            "jsonrpc": "2.0",
            "id": 0,
            "method": "initialize",
            "params": {
                "protocolVersion": LATEST_PROTOCOL_VERSION,
                "capabilities": {},
                "clientInfo": {"name": "test-m008", "version": "1"},
            },
        }
        source = _DelayedEofInput(
            "".join(
                json.dumps(frame) + "\n"
                for frame in [
                    initialize,
                    {"jsonrpc": "2.0", "method": "notifications/initialized"},
                    {
                        "jsonrpc": "2.0",
                        "id": request_id,
                        "method": "tools/call",
                        "params": {"name": name, "arguments": arguments},
                    },
                ]
            )
        )
        output = StringIO()
        try:
            self.assertEqual(serve(BrokerClient(socket_path), source, output), 0)
        finally:
            server.shutdown()
            server.server_close()
            server.release_socket_path()
            server_thread.join(timeout=1)
        responses = [json.loads(line) for line in output.getvalue().splitlines()]
        return next(response["result"] for response in responses if response.get("id") == request_id)

    def wait_until(self, predicate, *, timeout: float = 2.0) -> None:
        """Wait boundedly for an asynchronous integration condition."""

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.01)
        self.fail("asynchronous integration condition did not become true")


    def test_real_service_mcp_preserves_counts_pagination_and_gate(self) -> None:
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
        self.assertFalse(failed["isError"])
        self.assertTrue(failed["structuredContent"]["created"])
        agent_id = failed["structuredContent"]["agent_id"]
        self.wait_until(
            lambda: self.store.get_agent(agent_id)["status"]
            == AgentStatus.FAILED.value
        )
        row = self.store.list_agents()[0]
        self.assertEqual((row["status"], row["failure_kind"]), ("failed", "supervisor_start_failed"))
        retried = self.mcp_call(service, 2, "start", arguments)
        self.assertFalse(retried["isError"])
        self.assertFalse(retried["structuredContent"]["created"])
        self.assertEqual(len(calls), 1)
        service.close()


if __name__ == "__main__":
    unittest.main()
