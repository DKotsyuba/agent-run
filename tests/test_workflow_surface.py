"""Contract tests for the public workflow verbs and delivery outbox."""

from __future__ import annotations

import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from agent_run.cli import _execute, _parser
from agent_run.delivery.base import DeliveryReceipt
from agent_run.delivery.workflow_dispatch import WorkflowDeliveryDispatcher
from agent_run.domain import OrchestratorRef
from agent_run.errors import ValidationError
from agent_run.mcp import _TOOLS, _call_tool
from agent_run.service import AgentService
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

    def test_cli_batch_generates_flat_parallel_workflow(self) -> None:
        """Validate batch shape, embed each task, and reuse workflow start."""

        service = _WorkflowService()
        with tempfile.TemporaryDirectory() as directory:
            jobs = [
                {"runtime": "codex", "model": "m", "profile": "p", "task": "first task"},
                {"runtime": "claude", "model": "m", "profile": "p", "task": "second task"},
            ]
            path = Path(directory) / "jobs.json"
            path.write_text(json.dumps(jobs))
            args = _parser().parse_args(["batch", "--file", str(path)])
            self.assertEqual(_execute(args, service, StringIO()), {"run_id": "wf_test"})
        _, name, script, script_args, _ = service.calls[0]
        self.assertEqual(name, "batch")
        self.assertIsNone(script_args)
        self.assertIn("parallel", script)
        self.assertIn("first task", script)
        self.assertIn("second task", script)

    def test_cli_batch_refuses_invalid_shapes(self) -> None:
        """Reject empty, non-array, and non-object batch input."""

        service = _WorkflowService()
        for value in ("[]", "{}", "[1]"):
            args = _parser().parse_args(["batch", "--file", "-"])
            with self.assertRaises(ValidationError):
                _execute(args, service, StringIO(value))

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
            service = object.__new__(AgentService)
            service._store = store
            with patch("os.kill") as kill, self.assertRaises(ValidationError):
                service.workflow_cancel(run_id)
            kill.assert_not_called()

    def test_mcp_dispatch_service_methods_all_exist_on_the_real_agent_service(self) -> None:
        """Pin every `service.<name>` the real MCP dispatch reaches to a callable on
        the real AgentService, so a fake test double can never again hide a method
        the live service does not implement."""

        import ast
        import inspect

        from agent_run.mcp import _call_tool

        tree = ast.parse(inspect.getsource(_call_tool))
        names = sorted({
            node.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute)
            and isinstance(node.value, ast.Name)
            and node.value.id == "service"
        })
        self.assertTrue(names)
        for name in names:
            self.assertTrue(
                callable(getattr(AgentService, name, None)),
                f"AgentService has no callable {name!r}, but mcp.py dispatches to it",
            )

    def test_mcp_workflow_start_happy_path_dispatches_to_the_real_service(self) -> None:
        """Drive the real MCP dispatch for workflow_start through a real AgentService,
        not a fake, so the wiring from mcp.py to AgentService is actually exercised."""

        from agent_run.config import Config

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = StateStore.initialize(root / "state.db")
            service = AgentService(
                Config(schema_version=1), store, root,
                launch=lambda *args: None, now=lambda: 0.0,
            )
            try:
                with patch(
                    "agent_run.workflow_run.start_workflow", return_value="wf_live"
                ) as start:
                    result = _call_tool(
                        service, "workflow_start", {"name": "n", "script": "result = 1"}
                    )
                self.assertEqual(result, {"run_id": "wf_live"})
                start.assert_called_once_with(
                    service._home, "n", {"script": "result = 1"}, orchestrator=None
                )
            finally:
                store.close()



if __name__ == "__main__":
    unittest.main()
