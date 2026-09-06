"""Native continuation, history boundaries and dispatch authority regression tests."""

import io
import json
import unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, patch

from agent_run.adapters.base import LaunchPlan
from agent_run.adapters.continuation import cli_resume_plan
from agent_run.adapters.claude.adapter import ClaudeSession
from agent_run.adapters.claude.adapter import ADAPTER as CLAUDE
from agent_run.adapters.glm.adapter import ADAPTER as GLM
from agent_run.adapters.qwen.adapter import ADAPTER as QWEN
from agent_run.adapters.opencode.adapter import OpenCodeRuntimeSession
from agent_run.adapters.opencode.continuation import resume_boundary
from agent_run.dispatch import Session, call_tool
from agent_run.domain import AgentStatus
from agent_run.errors import ValidationError
from test_claude_session import FakeSink
from test_opencode_adapter import AdapterCase, FakeService, message, PRIMARY_AGENT


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


class OpenCodeResumeTests(AdapterCase):
    """Use existing captured-native-shape fixtures to delimit a resumed turn."""

    def test_old_completion_does_not_finish_new_turn(self):
        """Old messages are excluded from both the result and normalized transcript."""
        self.prove_service()
        plan = replace(self.prepare(), resume_session_id="ses_1")
        old = {**message("assistant", "old"), "id": "old"}
        new = {**message("assistant", "new", at=2), "id": "new"}
        service = FakeService(self.agent_dir, [None], [[old], [old], [new, old]])
        info = {"id": "ses_1", "agent": PRIMARY_AGENT, "location": {"directory": str(self.workdir)}, "model": dict(plan.adapter_state["model"]), "outcome": "failed"}
        service.session_info = Mock(return_value=info)
        sink = FakeSink()
        session = self.adapter.launch(plan, sink, client=service)
        session._interval = 0.001
        outcome = session.wait(2)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual([item.content for item in sink.messages], ["new"])
        self.assertNotIn("create_session", [call[0] for call in service.calls])
        self.assertEqual(Path(outcome.answer_path).read_text().splitlines()[0], "new")

    def test_active_or_mismatched_context_is_never_prompted(self):
        """Refuse a busy or moved native context before a new prompt is sent."""
        model = {"providerID": "omniroute", "id": "model"}
        client = Mock()
        valid = {"id": "saved", "agent": PRIMARY_AGENT, "model": model, "location": {"directory": str(self.workdir)}}
        for info, status in ((valid, {"saved": {"type": "running"}}), ({**valid, "id": "other"}, {})):
            with self.subTest(info=info, status=status):
                client.session_info.return_value = info
                client.session_status.return_value = status
                with self.assertRaises(ValidationError):
                    resume_boundary(client, "saved", str(self.workdir), model)
        client.prompt_async.assert_not_called()
