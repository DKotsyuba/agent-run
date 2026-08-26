import json
import stat
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import MappingProxyType
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import LaunchPlan
from agent_run.adapters.claude import adapter as claude_adapter_module
from agent_run.adapters.claude.adapter import ADAPTER
from agent_run.domain import AgentStatus, MessageRole


class FakeSink:
    def __init__(self, *, fail_message_once: bool = False) -> None:
        self.messages: list = []
        self.sessions: list = []
        self.events: list = []
        self._fail_message_once = fail_message_once

    def message(self, message) -> None:
        if self._fail_message_once:
            self._fail_message_once = False
            raise RuntimeError("sink exploded")
        self.messages.append(message)

    def session(self, runtime_session_id: str) -> None:
        self.sessions.append(runtime_session_id)

    def event(self, kind: str, data) -> None:
        self.events.append((kind, dict(data)))


class ClaudeSessionTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name).resolve()
        self.workdir = self.root / "work"
        self.workdir.mkdir()
        self.agent_dir = self.root / "agents" / "ag-1"
        self.agent_dir.mkdir(parents=True)
        self.log_path = self.agent_dir / "runtime.jsonl"

    def plan(self, script: str, *, environment: dict | None = None, adapter_state: dict | None = None) -> LaunchPlan:
        initial_input = json.dumps({"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": "go"}]}}) + "\n"
        return LaunchPlan(
            argv=(sys.executable, "-u", "-c", script),
            cwd=self.workdir,
            environment=MappingProxyType(environment or {}),
            initial_input=initial_input,
            runtime_stream_path=self.log_path,
            adapter_state=MappingProxyType(adapter_state or {}),
        )

    # -- success / empty result -----------------------------------------

    def test_nonblank_result_succeeds_with_content_and_bounded_metadata_event(self) -> None:
        script = (
            "import sys, json\n"
            "sys.stdin.readline()\n"
            "print(json.dumps({'type': 'system', 'subtype': 'init'}))\n"
            "print(json.dumps({'type': 'assistant', 'session_id': 'sess-1', 'message': {'role': 'assistant', "
            "'content': [{'type': 'text', 'text': 'hi there'}]}}))\n"
            "print(json.dumps({'type': 'result', 'session_id': 'sess-1', 'subtype': 'success', 'is_error': False, "
            "'result': 'the answer', 'duration_ms': 1, 'num_turns': 1, 'total_cost_usd': 0.0, 'usage': {}}))\n"
        )
        sink = FakeSink()
        session = ADAPTER.launch(self.plan(script), sink)
        outcome = session.wait(timeout_seconds=5)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(outcome.exit_code, 0)
        self.assertEqual(outcome.runtime_session_id, "sess-1")
        self.assertTrue(sink.sessions and set(sink.sessions) == {"sess-1"})
        self.assertTrue(any(m.role == MessageRole.ASSISTANT and m.content == "hi there" for m in sink.messages))
        result_events = [data for kind, data in sink.events if kind == "runtime_result"]
        self.assertEqual(len(result_events), 1)
        self.assertNotIn("result_text", result_events[0])
        self.assertNotIn("runtime_session_id", result_events[0])
        self.assertEqual(result_events[0]["subtype"], "success")

    def test_blank_result_fails_even_when_exit_code_is_clean(self) -> None:
        script = (
            "import sys, json\n"
            "sys.stdin.readline()\n"
            "print(json.dumps({'type': 'result', 'session_id': 'sess-1', 'subtype': 'success', 'is_error': False, "
            "'result': '', 'duration_ms': 1, 'num_turns': 1, 'total_cost_usd': 0.0, 'usage': {}}))\n"
        )
        session = ADAPTER.launch(self.plan(script), FakeSink())
        outcome = session.wait(timeout_seconds=5)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.exit_code, 0)
        self.assertEqual(outcome.failure_kind, "empty_result")

    # -- redaction ---------------------------------------------------------

    def test_literal_secret_value_is_redacted_from_messages_and_the_disk_log(self) -> None:
        script = (
            "import sys, json\n"
            "sys.stdin.readline()\n"
            "print(json.dumps({'type': 'assistant', 'session_id': 'sess-1', 'message': {'role': 'assistant', "
            "'content': [{'type': 'text', 'text': 'leaked SECRET_VALUE_XYZ here'}]}}))\n"
            "print(json.dumps({'type': 'result', 'session_id': 'sess-1', 'subtype': 'success', 'is_error': False, "
            "'result': 'done', 'duration_ms': 1, 'num_turns': 1, 'total_cost_usd': 0.0, 'usage': {}}))\n"
        )
        plan = self.plan(
            script,
            environment={"FAKE_AUTH": "SECRET_VALUE_XYZ"},
            adapter_state={"secret_env_names": ("FAKE_AUTH",)},
        )
        sink = FakeSink()
        session = ADAPTER.launch(plan, sink)
        outcome = session.wait(timeout_seconds=5)
        self.assertIsNotNone(outcome)
        self.assertTrue(all("SECRET_VALUE_XYZ" not in m.content for m in sink.messages))
        self.assertTrue(any("leaked" in m.content and "<redacted>" in m.content for m in sink.messages))
        log_text = self.log_path.read_text(encoding="utf-8")
        self.assertNotIn("SECRET_VALUE_XYZ", log_text)

    def test_malformed_line_with_a_literal_secret_is_still_redacted_on_disk(self) -> None:
        script = (
            "import sys, json\n"
            "sys.stdin.readline()\n"
            "print('not json but SECRET_VALUE_XYZ leaked')\n"
            "print(json.dumps({'type': 'result', 'is_error': True, 'subtype': 'error_during_execution'}))\n"
        )
        plan = self.plan(
            script,
            environment={"FAKE_AUTH": "SECRET_VALUE_XYZ"},
            adapter_state={"secret_env_names": ("FAKE_AUTH",)},
        )
        session = ADAPTER.launch(plan, FakeSink())
        outcome = session.wait(timeout_seconds=5)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.FAILED)
        log_text = self.log_path.read_text(encoding="utf-8")
        self.assertNotIn("SECRET_VALUE_XYZ", log_text)

    # -- runtime log permissions --------------------------------------------

    def test_runtime_log_is_created_private_mode_0600(self) -> None:
        script = (
            "import sys, json\n"
            "sys.stdin.readline()\n"
            "print(json.dumps({'type': 'result', 'is_error': True, 'subtype': 'no_answer'}))\n"
        )
        session = ADAPTER.launch(self.plan(script), FakeSink())
        session.wait(timeout_seconds=5)
        mode = stat.S_IMODE(self.log_path.stat().st_mode)
        self.assertEqual(mode, 0o600)

    # -- reader/sink failure window ------------------------------------------

    def test_sink_exception_persists_and_makes_wait_fail_while_still_draining(self) -> None:
        script = (
            "import sys, json\n"
            "sys.stdin.readline()\n"
            "print(json.dumps({'type': 'assistant', 'session_id': 'sess-1', 'message': {'role': 'assistant', "
            "'content': [{'type': 'text', 'text': 'first'}]}}))\n"
            "print(json.dumps({'type': 'assistant', 'session_id': 'sess-1', 'message': {'role': 'assistant', "
            "'content': [{'type': 'text', 'text': 'second'}]}}))\n"
            "print(json.dumps({'type': 'result', 'session_id': 'sess-1', 'subtype': 'success', 'is_error': False, "
            "'result': 'done', 'duration_ms': 1, 'num_turns': 1, 'total_cost_usd': 0.0, 'usage': {}}))\n"
        )
        sink = FakeSink(fail_message_once=True)
        session = ADAPTER.launch(self.plan(script), sink)
        with self.assertRaisesRegex(RuntimeError, "sink exploded"):
            session.wait(timeout_seconds=5)
        # draining continued past the failing line: the second message and
        # the terminal line both still reached the log.
        log_text = self.log_path.read_text(encoding="utf-8")
        self.assertIn("second", log_text)
        self.assertIn("\"type\": \"result\"", log_text)

    def test_wait_refuses_to_finalize_while_reader_is_still_alive(self) -> None:
        script = (
            "import sys, json\n"
            "sys.stdin.readline()\n"
            "print(json.dumps({'type': 'result', 'session_id': 'sess-1', "
            "'subtype': 'success', 'is_error': False, 'result': 'done'}))\n"
        )
        session = ADAPTER.launch(self.plan(script), FakeSink())
        session._process.wait(timeout=5)
        session._reader.join(timeout=5)

        with patch.object(session._reader, "join") as bounded_join, patch.object(
            session._reader, "is_alive", return_value=True
        ):
            with self.assertRaisesRegex(RuntimeError, "reader_timeout"):
                session.wait(timeout_seconds=5)
            bounded_join.assert_called_once_with(timeout=5)
        self.assertFalse(session._raw_stream.closed)

        outcome = session.wait(timeout_seconds=5)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)

    # -- constructor failure aborts the child ------------------------------

    def test_constructor_failure_native_cancels_and_reaps_the_process_group(self) -> None:
        script = "import time\ntime.sleep(5)\n"
        missing_parent_log = self.root / "no-such-dir" / "runtime.jsonl"
        plan = LaunchPlan(
            argv=(sys.executable, "-u", "-c", script),
            cwd=self.workdir,
            environment=MappingProxyType({}),
            initial_input=None,
            runtime_stream_path=missing_parent_log,
            adapter_state=MappingProxyType({}),
        )
        captured: dict = {}
        original_popen = claude_adapter_module.subprocess.Popen

        def _capture(*args, **kwargs):
            process = original_popen(*args, **kwargs)
            captured["process"] = process
            return process

        with patch.object(claude_adapter_module.subprocess, "Popen", side_effect=_capture):
            with self.assertRaises(OSError):
                ADAPTER.launch(plan, FakeSink())

        process = captured["process"]
        deadline = time.time() + 3
        while process.poll() is None and time.time() < deadline:
            time.sleep(0.05)
        self.assertIsNotNone(process.poll())

    # -- cancellation --------------------------------------------------------

    def test_cancel_marks_the_outcome_cancelled(self) -> None:
        script = (
            "import sys, time, json\n"
            "sys.stdin.readline()\n"
            "time.sleep(10)\n"
            "print(json.dumps({'type': 'result', 'is_error': False, 'subtype': 'success', 'result': 'late'}))\n"
        )
        session = ADAPTER.launch(self.plan(script), FakeSink())
        session.cancel(grace_seconds=2)
        outcome = session.wait(timeout_seconds=5)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.CANCELLED)


if __name__ == "__main__":
    unittest.main()
