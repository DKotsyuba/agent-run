"""Native continuation, history boundaries and dispatch authority regression tests."""

import io
import json
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from agent_run.adapters.base import Capability, LaunchPlan
from agent_run.adapters.continuation import cli_resume_plan
from agent_run.adapters.claude.adapter import ClaudeSession
from agent_run.adapters.claude.adapter import ADAPTER as CLAUDE
from agent_run.adapters.glm.adapter import ADAPTER as GLM
from agent_run.adapters.qwen.adapter import ADAPTER as QWEN
from agent_run.dispatch import Session, call_tool
from agent_run.domain import AgentStatus
from agent_run.errors import ValidationError
from test_claude_session import FakeSink


class ArgumentsTests(unittest.TestCase):
    """Verify that native selectors cannot accidentally request a fresh session."""

    def test_resume_arguments_preserve_other_settings(self):
        """Both CLI families target an exact ID without changing prompt or answer path."""
        plan = LaunchPlan(("engine", "--session-id", "new", "--model", "m"), Path("/tmp"), {}, "task", Path("/tmp/log"), {})
        self.assertIs(cli_resume_plan(plan), plan)
        resumed = cli_resume_plan(replace(plan, resume_session_id="saved"), session_option="--session-id")
        self.assertEqual(resumed.argv, ("engine", "--model", "m", "--resume", "saved"))
        qwen = cli_resume_plan(replace(plan, argv=("qwen", "-p", "task"), resume_session_id="saved"))
        self.assertEqual(qwen.argv, ("qwen", "-p", "task", "--resume", "saved"))
        with self.assertRaises(ValidationError):
            cli_resume_plan(replace(plan, resume_session_id="--latest"))

    def test_dispatch_inherits_authority_and_preserves_caller(self):
        """Resume ignores ephemeral fast defaults and rejects authority overrides."""
        service = Mock()
        caller = {"transport": "codex_queue", "external_session_id": "caller"}
        call_tool(service, "resume", {"agent_id": "parent", "task": "fix", "orchestrator": caller}, Session())
        self.assertEqual(service.resume.call_args.args, ("parent", "fix"))
        self.assertEqual(service.resume.call_args.kwargs["orchestrator"].external_session_id, "caller")
        with self.assertRaises(ValidationError):
            call_tool(service, "resume", {"agent_id": "parent", "task": "fix", "write": True}, Session())

    def test_adapters_pass_exact_native_resume_selector_to_process(self):
        """Exercise each real CLI adapter's launch wiring without contacting a provider."""
        for adapter, module, session_type, argv in (
            (CLAUDE, "claude", "ClaudeSession", ("claude", "--session-id", "fresh")),
            (GLM, "claude", "ClaudeSession", ("claude", "--session-id", "fresh")),
            (QWEN, "qwen", "QwenSession", ("qwen", "-p", "task")),
        ):
            with self.subTest(adapter=adapter.describe().name):
                plan = LaunchPlan(argv, Path("/tmp"), {}, "task", Path("/tmp/log"), {}, resume_session_id="saved")
                namespace = "agent_run.adapters." + module + ".adapter."
                with patch(namespace + "subprocess.Popen") as popen, patch(namespace + session_type):
                    adapter.launch(plan, Mock())
                self.assertEqual(popen.call_args.args[0][-2:], ["--resume", "saved"])
                self.assertNotIn("--session-id", popen.call_args.args[0])


class StreamIdentityTests(unittest.TestCase):
    """Exercise the actual stream decoder with a lightweight process double."""

    def test_resume_refuses_missing_or_wrong_stream_identity(self):
        """An otherwise successful result cannot certify a different native context."""
        import tempfile
        for identity in (None, "other", "saved"):
            with self.subTest(identity=identity), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                result = {"type": "result", "subtype": "success", "is_error": False, "result": "answer", "usage": {}}
                if identity is not None:
                    result["session_id"] = identity
                process = Mock()
                process.stdout = io.StringIO(json.dumps(result) + "\n")
                process.stderr = io.StringIO()
                process.stdin = io.StringIO()
                process.wait.return_value = 0
                plan = LaunchPlan(("engine",), root, {}, None, root / "runtime.jsonl", {}, root / "answer.md", "saved")
                session = ClaudeSession(process, plan, FakeSink())
                if identity == "saved":
                    self.assertEqual(session.wait(3).status, AgentStatus.SUCCEEDED)
                else:
                    with self.assertRaises(ValidationError):
                        session.wait(3)
                    self.assertFalse((root / "answer.md").exists())
