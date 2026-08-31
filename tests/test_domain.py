import json
import math
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run import _require_supported_python
from agent_run.domain import (
    ACTIVE,
    TERMINAL,
    TRANSITIONS,
    AgentStatus,
    Message,
    MessageRole,
    Outcome,
    StartRequest,
    new_agent_id,
    validate_agent_id,
    validate_transition,
)
from agent_run.errors import StateTransitionError, ValidationError
from agent_run.state.db import request_json


class DomainTests(unittest.TestCase):
    def test_python_version_gate_refuses_unsupported_version(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "Python 3.11"):
            _require_supported_python((3, 10))
        _require_supported_python((3, 11))

    def test_state_machine_matches_frozen_contract(self) -> None:
        self.assertEqual(
            {status.value for status in AgentStatus},
            {
                "created",
                "starting",
                "running",
                "cancelling",
                "succeeded",
                "failed",
                "timed_out",
                "cancelled",
                "lost",
            },
        )
        self.assertEqual(ACTIVE | TERMINAL, frozenset(AgentStatus))
        self.assertFalse(ACTIVE & TERMINAL)
        self.assertEqual(
            TRANSITIONS[AgentStatus.RUNNING],
            frozenset(
                {
                    AgentStatus.SUCCEEDED,
                    AgentStatus.FAILED,
                    AgentStatus.TIMED_OUT,
                    AgentStatus.CANCELLING,
                    AgentStatus.LOST,
                }
            ),
        )
        validate_transition(AgentStatus.CREATED, AgentStatus.STARTING)
        with self.assertRaises(StateTransitionError):
            validate_transition(AgentStatus.SUCCEEDED, AgentStatus.RUNNING)
        with self.assertRaises(StateTransitionError):
            validate_transition("created", "starting")

    def test_agent_id_generation_and_validation(self) -> None:
        generated = new_agent_id()
        self.assertEqual(validate_agent_id(generated), generated)
        for invalid in (
            "",
            "../outside",
            "ag-20260230-010203-0123456789",
            "ag-20260825-010203-012345678G",
        ):
            with self.subTest(invalid=invalid), self.assertRaises(ValidationError):
                validate_agent_id(invalid)

    def test_start_request_validates_paths_timeout_and_duplicate_roots(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            request = StartRequest("codex", "model", "profile", "task", root)
            self.assertEqual(request.workdir, root)
            self.assertIsNone(request.timeout_seconds)
            self.assertEqual(
                StartRequest(
                    "codex", "model", "profile", "task", root, timeout_seconds=480
                ).timeout_seconds,
                480,
            )
            with self.assertRaises(ValidationError):
                StartRequest(
                    "codex",
                    "model",
                    "profile",
                    "task",
                    root,
                    read_roots=(root, root),
                )
            with self.assertRaises(ValidationError):
                StartRequest("codex", "model", "profile", "task", root, timeout_seconds=math.inf)
            first = StartRequest("codex", "model", "profile", "task", root, account="personal2")
            second = StartRequest("codex", "model", "profile", "task", root, account="work")
            self.assertEqual(json.loads(request_json(first))["account"], "personal2")
            self.assertNotEqual(request_json(first), request_json(second))

    def test_message_and_outcome_validation(self) -> None:
        self.assertEqual(Message(0, MessageRole.USER, "hello").content, "hello")
        with self.assertRaises(ValidationError):
            Message(-1, MessageRole.USER, "hello")
        with self.assertRaises(ValidationError):
            Message(0, MessageRole.USER, "  ")
        self.assertEqual(Outcome(AgentStatus.SUCCEEDED).status, AgentStatus.SUCCEEDED)
        with self.assertRaises(ValidationError):
            Outcome(AgentStatus.RUNNING)
        with self.assertRaises(ValidationError):
            Outcome("succeeded")
        with self.assertRaises(ValidationError):
            Outcome(AgentStatus.FAILED, answer_bytes=-1)


if __name__ == "__main__":
    unittest.main()
