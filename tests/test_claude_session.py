import contextlib
import json
import os
import signal
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
    def __init__(self, *, fail_message_once: bool = False, fail_event_kind: str | None = None) -> None:
        self.messages: list = []
        self.sessions: list = []
        self.events: list = []
        self._fail_message_once = fail_message_once
        self._fail_event_kind = fail_event_kind

    def message(self, message) -> None:
        if self._fail_message_once:
            self._fail_message_once = False
            raise RuntimeError("sink exploded")
        self.messages.append(message)

    def session(self, runtime_session_id: str) -> None:
        self.sessions.append(runtime_session_id)

    def event(self, kind: str, data) -> None:
        if kind == self._fail_event_kind:
            raise RuntimeError(f"{kind} write failed")
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
        import hashlib

        from agent_run.verify import DEFAULT_SENTINEL

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
        answer = self.agent_dir / "answer.md"
        data = answer.read_bytes()
        self.assertTrue(data.endswith(f"{DEFAULT_SENTINEL}\n".encode()))
        self.assertEqual(outcome.answer_path, answer)
        self.assertEqual(outcome.answer_bytes, len(data))
        self.assertEqual(outcome.answer_sha256, hashlib.sha256(data).hexdigest())
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

    def test_a_failing_stream_diagnostic_write_does_not_mask_a_real_outcome(self) -> None:
        """Regression: the diagnostic event write is best-effort.

        Unlike a message-sink failure (fatal, tested above), a durable-write
        hiccup on the purely informational "stream_diagnostic" event must not
        abort the read loop or turn a genuine outcome into a raised error
        that the supervisor would otherwise report as supervision_failed.
        """

        script = (
            "import sys, json\n"
            "sys.stdin.readline()\n"
            "print('not json')\n"
            "print(json.dumps({'type': 'result', 'session_id': 'sess-1', 'subtype': 'success', 'is_error': False, "
            "'result': 'done', 'duration_ms': 1, 'num_turns': 1, 'total_cost_usd': 0.0, 'usage': {}}))\n"
        )
        sink = FakeSink(fail_event_kind="stream_diagnostic")
        session = ADAPTER.launch(self.plan(script), sink)
        outcome = session.wait(timeout_seconds=5)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertNotIn("stream_diagnostic", [kind for kind, _ in sink.events])

    def test_wait_yields_when_an_open_stream_keeps_the_reader_alive(self) -> None:
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
            self.assertIsNone(session.wait(timeout_seconds=0.01))
            bounded_join.assert_called_once()
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

    # -- settling on stream content instead of process exit ----------------
    #
    # Regression for the live canary: a real claude 2.1.245 child answered
    # with a clean ``result``/``success`` line but never exited on its own
    # -- it held stdin open for another turn, per the stream-json protocol,
    # and (unprompted) later replayed a second full init-to-result cycle on
    # the same session id. The old ``wait()`` blocked on OS process exit, so
    # it never settled: no claude agent ever reached ``succeeded``. These
    # scripts model that exact shape -- they never exit on their own after
    # answering, only in reaction to being killed or to stdin closing -- so
    # they fail (``wait`` times out and returns ``None``) on unfixed code.

    def _force_kill(self, session) -> None:
        process = session._process
        if process.poll() is None:
            with contextlib.suppress(OSError):
                os.killpg(process.pid, signal.SIGKILL)
            with contextlib.suppress(Exception):
                process.wait(timeout=2)

    def test_wait_settles_on_first_result_without_waiting_for_process_exit(self) -> None:
        script = (
            "import sys, json\n"
            "sys.stdin.readline()\n"
            "print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': 'sess-live'}))\n"
            "print(json.dumps({'type': 'assistant', 'session_id': 'sess-live', 'message': {'role': 'assistant', "
            "'content': [{'type': 'text', 'text': 'CANARY_OK'}]}}))\n"
            "print(json.dumps({'type': 'result', 'session_id': 'sess-live', 'subtype': 'success', "
            "'is_error': False, 'result': 'CANARY_OK', 'duration_ms': 1, 'num_turns': 1, "
            "'total_cost_usd': 0.01, 'usage': {}}))\n"
            "sys.stdout.flush()\n"
            # The real engine holds stdin open for another turn instead of
            # exiting. If agent-run left stdin open, feed a duplicate second
            # cycle here to prove settling is also idempotent against it.
            "line = sys.stdin.readline()\n"
            "if line:\n"
            "    print(json.dumps({'type': 'system', 'subtype': 'init', 'session_id': 'sess-live'}))\n"
            "    print(json.dumps({'type': 'result', 'session_id': 'sess-live', 'subtype': 'success', "
            "'is_error': False, 'result': 'CANARY_OK', 'duration_ms': 1, 'num_turns': 1, "
            "'total_cost_usd': 0.02, 'usage': {}}))\n"
        )
        session = ADAPTER.launch(self.plan(script), FakeSink())
        self.addCleanup(self._force_kill, session)

        started = time.monotonic()
        outcome = session.wait(timeout_seconds=15)
        elapsed = time.monotonic() - started

        self.assertIsNotNone(outcome, "wait() never settled even though a result/success line was streamed")
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertLess(elapsed, 10, "wait() waited near the full timeout instead of settling on the stream")
        answer = self.agent_dir / "answer.md"
        self.assertIn("CANARY_OK", answer.read_text(encoding="utf-8"))

        # The still-running engine was actually ended, not just ignored.
        self.assertIsNotNone(session._process.poll())

        # No duplicate cycle was ever allowed to happen: closing stdin (and
        # signalling) as soon as the first result arrived pre-empts it.
        log_lines = [
            json.loads(line) for line in self.log_path.read_text(encoding="utf-8").splitlines() if line.strip()
        ]
        result_lines = [entry for entry in log_lines if entry.get("type") == "result"]
        self.assertEqual(len(result_lines), 1)

    def test_wait_settles_failed_on_first_error_result_without_waiting_for_process_exit(self) -> None:
        script = (
            "import sys, json\n"
            "sys.stdin.readline()\n"
            "print(json.dumps({'type': 'result', 'session_id': 'sess-live', 'subtype': 'error_during_execution', "
            "'is_error': True, 'result': 'boom'}))\n"
            "sys.stdout.flush()\n"
            "sys.stdin.readline()\n"  # holds stdin open, exactly like the success case above
        )
        session = ADAPTER.launch(self.plan(script), FakeSink())
        self.addCleanup(self._force_kill, session)

        started = time.monotonic()
        outcome = session.wait(timeout_seconds=15)
        elapsed = time.monotonic() - started

        self.assertIsNotNone(outcome, "wait() never settled even though a terminal error result was streamed")
        self.assertEqual(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "error_during_execution")
        self.assertLess(elapsed, 10, "wait() waited near the full timeout instead of settling on the stream")
        self.assertIsNotNone(session._process.poll())


if __name__ == "__main__":
    unittest.main()
