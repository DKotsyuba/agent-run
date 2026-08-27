"""Unit coverage for service-backed workflow agent steps."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from agent_run.domain import AgentStatus
from agent_run.errors import ValidationError
from agent_run.state.store import StateStore
from agent_run.workflow_executor import WorkflowStepExecutor, validate_agent_spec


class _Service:
    """Minimal service double exposing the workflow executor's service surface."""

    def __init__(self, status: AgentStatus, answer: str | None = None) -> None:
        """Set the terminal status and optional inline answer returned by this fake."""

        self.status = status
        self.answer_text = answer
        self.requests = []
        self.cancelled = []

    def start(self, request):
        """Record a request and return the fixed agent identity."""

        self.requests.append(request)
        return SimpleNamespace(agent_id="a" * 32)

    def get(self, _agent_id):
        """Return the configured terminal agent view and failure metadata."""

        return SimpleNamespace(
            status=self.status,
            failure_kind="runtime_failed" if self.status is AgentStatus.FAILED else None,
            failure_params={"reason": "fake"},
        )

    def answer(self, _agent_id):
        """Return an inline answer view without a backing answer file."""

        return SimpleNamespace(
            available=self.answer_text is not None,
            content=self.answer_text,
            path=None,
        )

    def cancel(self, agent_id):
        """Record the requested cancellation."""

        self.cancelled.append(agent_id)


class WorkflowExecutorTests(unittest.TestCase):
    """Exercise strict request mapping and terminal workflow-step outcomes."""

    def setUp(self) -> None:
        """Create a claimed run and an existing directory for request path checks."""

        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = StateStore.initialize(self.root / "state.db")
        self.run_id = self.store.create_workflow_run("workflow", "digest")
        self.store.claim_workflow_run(self.run_id, "1 test")

    def tearDown(self) -> None:
        """Close the database and remove the isolated temporary tree."""

        self.store.close()
        self.temporary.cleanup()

    def spec(self, **overrides):
        """Return one complete workflow agent spec, optionally overridden."""

        result = {
            "runtime": "codex",
            "model": "model",
            "profile": "default",
            "task": "do work",
            "workdir": str(self.root),
        }
        result.update(overrides)
        return result

    def executor(self, service, **kwargs):
        """Build the executor under test with deterministic injected dependencies."""

        return WorkflowStepExecutor(self.root, self.store, self.run_id, service=service, **kwargs)

    def test_mapping_accepts_every_optional_field_and_refuses_unknown_keys(self) -> None:
        """Map exactly the contract fields to ``StartRequest`` without extras."""

        request = validate_agent_spec(
            self.spec(
                write=True,
                read_roots=[str(self.root)],
                timeout_seconds=2.5,
                output_schema={"type": "object", "properties": {}},
            )
        )
        self.assertTrue(request.write)
        self.assertEqual(request.read_roots, (self.root.resolve(),))
        self.assertEqual(request.timeout_seconds, 2.5)
        self.assertEqual(request.output_schema, {"type": "object", "properties": {}})
        with self.assertRaisesRegex(ValidationError, "unknown keys: extra"):
            validate_agent_spec(self.spec(extra=True))

    def test_success_failure_timeout_and_output_validation_are_journalled(self) -> None:
        """Persist normal, failed, timed-out, and invalid-output terminal results."""

        success = self.executor(_Service(AgentStatus.SUCCEEDED, '{"ok": true}'))
        result = success("one", self.spec())
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(success._service.requests[0].workdir, self.root.resolve())

        failed = self.executor(_Service(AgentStatus.FAILED))
        result = failed("two", self.spec())
        self.assertEqual(result["failure_params"], {"reason": "fake"})

        timed_out = self.executor(
            _Service(AgentStatus.RUNNING), monotonic=iter((0.0, 2.0)).__next__
        )
        result = timed_out("three", self.spec(timeout_seconds=1))
        self.assertEqual(result["failure_kind"], "step_timeout")
        self.assertEqual(timed_out._service.cancelled, ["a" * 32])

        invalid = self.executor(_Service(AgentStatus.SUCCEEDED, "not json"))
        result = invalid(
            "four",
            self.spec(output_schema={"type": "object", "properties": {"ok": {"type": "boolean"}}}),
        )
        self.assertEqual(result["failure_kind"], "step_output_invalid")
        self.assertEqual(result["failure_params"]["raw_answer"], "not json")


if __name__ == "__main__":
    unittest.main()
