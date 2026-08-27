"""Contract tests for the public workflow verbs and delivery outbox."""

from __future__ import annotations

import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from agent_run.cli import _Runtime, _execute, _parser
from agent_run.delivery.base import DeliveryReceipt
from agent_run.delivery.workflow_dispatch import WorkflowDeliveryDispatcher
from agent_run.domain import OrchestratorRef
from agent_run.errors import ValidationError
from agent_run.mcp import _TOOLS, _call_tool
from agent_run.state.store import StateStore


class _Transport:
    """Capture one workflow notice through the shared transport call shape."""

    name = "stub"
    api_version = 1

    def __init__(self) -> None:
        """Start with no captured sends."""

        self.calls: list[tuple[object, object]] = []

    def validate(self, config: object) -> None:
        """Accept the test delivery configuration without mutation."""

    def send(self, target: object, notice: object) -> DeliveryReceipt:
        """Capture a target and notice and return a deterministic receipt."""

        self.calls.append((target, notice))
        return DeliveryReceipt("remote-workflow")


class _WorkflowService:
    """Record CLI and MCP workflow dispatch without launching a process."""

    def __init__(self) -> None:
        """Start with an empty ordered call journal."""

        self.calls: list[tuple[object, ...]] = []

    def workflow_start(self, name: str, script: str, args: dict | None,
                       orchestrator: OrchestratorRef | None) -> dict[str, str]:
        """Record a start request and return a stable run identifier."""

        self.calls.append(("start", name, script, args, orchestrator))
        return {"run_id": "wf_test"}

    def workflow_status(self, run_id: str) -> dict[str, str]:
        """Record a status request and return a stable status."""

        self.calls.append(("status", run_id))
        return {"status": "running"}

    def workflow_cancel(self, run_id: str) -> dict[str, object]:
        """Record a cancellation request and report it accepted."""

        self.calls.append(("cancel", run_id))
        return {"cancel_requested": True}

    def workflow_answer(self, run_id: str) -> dict[str, object]:
        """Record an answer request and return a bounded result."""

        self.calls.append(("answer", run_id))
        return {"result": 7}


class WorkflowSurfaceTests(unittest.TestCase):
    """Exercise workflow visibility and its independent delivery channel."""

    def test_mcp_has_fifteen_schema_backed_tools_and_dispatches_all_workflow_verbs(self) -> None:
        """Expose exactly fifteen tools and decode each workflow request."""

        self.assertEqual(len(_TOOLS), 15)
        self.assertTrue(all(tool.get("inputSchema", {}).get("type") == "object" for tool in _TOOLS))
        service = _WorkflowService()
        self.assertEqual(_call_tool(service, "workflow_start", {"name": "n", "script": "result = 7"}),
                         {"run_id": "wf_test"})
        for name in ("workflow_status", "workflow_cancel", "workflow_answer"):
            _call_tool(service, name, {"run_id": "wf_test"})
        with self.assertRaises(ValidationError):
            _call_tool(service, "workflow_status", {"run_id": "wf_test", "extra": True})

    def test_cli_parses_and_dispatches_the_four_workflow_verbs(self) -> None:
        """Mirror workflow start, status, cancel, and answer in the CLI."""

        service = _WorkflowService()
        start = _parser().parse_args(["workflow", "start", "name", "result = args['x']",
                                      "--args", json.dumps({"x": 7})])
        self.assertEqual(_execute(start, service, StringIO()), {"run_id": "wf_test"})
        for verb in ("status", "cancel", "answer"):
            args = _parser().parse_args(["workflow", verb, "wf_test"])
            _execute(args, service, StringIO())
        self.assertEqual([call[0] for call in service.calls], ["start", "status", "cancel", "answer"])

    def test_terminal_transition_creates_and_dispatches_workflow_notice(self) -> None:
        """Commit terminal state and outbox row atomically, then drain it."""

        with tempfile.TemporaryDirectory() as directory:
            store = StateStore.initialize(Path(directory) / "state.db")
            try:
                run_id = store.create_workflow_run(
                    "named", "sha", plan=[], orchestrator=OrchestratorRef("stub", "session")
                )
                store.start_workflow_run(run_id)
                store.finish_workflow_run(run_id, "succeeded")
                row = store.connection.execute(
                    "SELECT state FROM workflow_deliveries WHERE run_id = ?", (run_id,)
                ).fetchone()
                self.assertEqual(row["state"], "pending")
                transport = _Transport()
                result = WorkflowDeliveryDispatcher(store, {"stub": transport}).drain()
                self.assertEqual((result.claimed, result.delivered), (1, 1))
                self.assertEqual(transport.calls[0][1].run_id, run_id)
            finally:
                store.close()

    def test_cancel_refuses_terminal_workflow(self) -> None:
        """Never signal the recorded process identity after terminal completion."""

        with tempfile.TemporaryDirectory() as directory:
            store = StateStore.initialize(Path(directory) / "state.db")
            run_id = store.create_workflow_run("named", "sha", plan=[])
            store.start_workflow_run(run_id)
            store.finish_workflow_run(run_id, "succeeded")
            runtime = object.__new__(_Runtime)
            runtime._inputs = lambda: (None, store)
            with patch("os.kill") as kill, self.assertRaises(ValidationError):
                runtime.workflow_cancel(run_id)
            kill.assert_not_called()


if __name__ == "__main__":
    unittest.main()
