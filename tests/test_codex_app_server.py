import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import LaunchPlan
from agent_run.adapters.codex.app_server import (
    EffectiveTurnParams,
    SteerRejected,
    VerificationError,
    start_session,
    verify_effective_params,
)
from agent_run.domain import AgentStatus, MessageRole
from agent_run.errors import ValidationError


class FakeTransport:
    def __init__(self, responses=None, events=None, pid=4242):
        self._responses = {method: list(items) for method, items in (responses or {}).items()}
        self._events = list(events or [])
        self._pid = pid
        self.requests = []
        self.terminated = None
        self.closed = False

    @property
    def pid(self):
        return self._pid

    def request(self, method, params):
        self.requests.append((method, dict(params)))
        queue = self._responses.get(method)
        if not queue:
            return {}
        return queue.pop(0)

    def poll_event(self, timeout):
        if self._events:
            return self._events.pop(0)
        return None

    def terminate(self, grace_seconds):
        self.terminated = grace_seconds

    def close(self):
        self.closed = True


class FakeSink:
    def __init__(self):
        self.messages = []
        self.sessions = []
        self.events = []

    def message(self, message):
        self.messages.append(message)

    def session(self, runtime_session_id):
        self.sessions.append(runtime_session_id)

    def event(self, kind, data):
        self.events.append((kind, dict(data)))


def make_plan(cwd, adapter_state, initial_input="do the thing"):
    return LaunchPlan(
        argv=("codex", "app-server"),
        cwd=cwd,
        environment={},
        initial_input=initial_input,
        runtime_stream_path=cwd / "runtime.jsonl",
        adapter_state=adapter_state,
    )


def thread_response(cwd, roots=("/work",), writable_roots=(), thread_id="th_1", **overrides):
    response = {
        "model": "gpt-5.6-sol",
        "cwd": str(cwd),
        "sandbox": "read-only",
        "approvalPolicy": "never",
        "roots": list(roots),
        "writableRoots": list(writable_roots),
        "threadId": thread_id,
    }
    response.update(overrides)
    return response


class VerifyEffectiveParamsTests(unittest.TestCase):
    def expected(self, **overrides):
        values = dict(
            model="gpt-5.6-sol",
            cwd="/work",
            roots=("/work",),
            sandbox="read-only",
            approval_policy="never",
            writable_roots=(),
        )
        values.update(overrides)
        return EffectiveTurnParams(**values)

    def test_matching_params_pass(self) -> None:
        actual = thread_response("/work")
        verify_effective_params(self.expected(), actual)

    def test_read_root_leaking_into_writable_roots_is_refused(self) -> None:
        actual = thread_response("/work", roots=("/work", "/extra"), writable_roots=("/extra",))
        with self.assertRaisesRegex(VerificationError, "writableRoots mismatch"):
            verify_effective_params(self.expected(roots=("/work", "/extra")), actual)

    def test_sandbox_mismatch_is_refused(self) -> None:
        actual = thread_response("/work", writable_roots=())
        actual["sandbox"] = "workspace-write"
        with self.assertRaisesRegex(VerificationError, "sandbox mismatch"):
            verify_effective_params(self.expected(), actual)


class StartSessionTests(unittest.TestCase):
    def test_success_verifies_params_and_starts_the_turn(self) -> None:
        cwd = Path("/work")
        plan = make_plan(
            cwd,
            {
                "model": "gpt-5.6-sol",
                "effort": None,
                "sandbox_mode": "read-only",
                "approval_policy": "never",
                "roots": (str(cwd),),
                "writable_roots": (),
            },
        )
        transport = FakeTransport(
            responses={
                "initialize": [{}],
                "thread/start": [thread_response(cwd)],
                "turn/start": [{}],
            }
        )
        sink = FakeSink()
        session = start_session(transport, plan, sink)
        self.assertEqual(session.pid, 4242)
        self.assertEqual(sink.sessions, ["th_1"])
        methods = [method for method, _ in transport.requests]
        self.assertEqual(methods, ["initialize", "thread/start", "turn/start"])
        self.assertEqual(transport.requests[-1][1]["input"], "do the thing")

    def test_refuses_when_effective_params_drift(self) -> None:
        cwd = Path("/work")
        plan = make_plan(
            cwd,
            {
                "model": "gpt-5.6-sol",
                "effort": None,
                "sandbox_mode": "read-only",
                "approval_policy": "never",
                "roots": (str(cwd),),
                "writable_roots": (),
            },
        )
        mismatched = thread_response(cwd)
        mismatched["writableRoots"] = [str(cwd)]
        transport = FakeTransport(
            responses={"initialize": [{}], "thread/start": [mismatched], "turn/start": [{}]}
        )
        with self.assertRaisesRegex(VerificationError, "writableRoots mismatch"):
            start_session(transport, plan, FakeSink())
        methods = [method for method, _ in transport.requests]
        self.assertNotIn("turn/start", methods)


class CodexAppServerSessionTests(unittest.TestCase):
    def start(self, events=()):
        cwd = Path("/work")
        plan = make_plan(
            cwd,
            {
                "model": "gpt-5.6-sol",
                "effort": None,
                "sandbox_mode": "read-only",
                "approval_policy": "never",
                "roots": (str(cwd),),
                "writable_roots": (),
            },
        )
        transport = FakeTransport(
            responses={"initialize": [{}], "thread/start": [thread_response(cwd)], "turn/start": [{}]},
            events=list(events),
        )
        sink = FakeSink()
        session = start_session(transport, plan, sink)
        return session, transport, sink

    def test_wait_normalizes_messages_then_returns_the_terminal_outcome(self) -> None:
        session, _transport, sink = self.start(
            events=[
                {"type": "message", "role": "assistant", "content": "hi", "at": 1.0},
                {"type": "turn/completed", "status": "completed", "exit_code": 0},
            ]
        )
        self.assertIsNone(session.wait(0))
        self.assertEqual(len(sink.messages), 1)
        self.assertEqual(sink.messages[0].role, MessageRole.ASSISTANT)
        self.assertEqual(sink.messages[0].content, "hi")

        outcome = session.wait(0)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(outcome.runtime_session_id, "th_1")

        self.assertIsNone(session.wait(0))

    def test_steer_buffers_an_already_pending_completion_until_ack(self) -> None:
        session, transport, _sink = self.start(
            events=[{"type": "turn/completed", "status": "completed", "exit_code": 0}]
        )
        transport._responses["turn/steer"] = [{"accepted": True}]

        session.steer("please wrap up")

        outcome = session.wait(0)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        methods = [method for method, _ in transport.requests]
        self.assertIn("turn/steer", methods)

    def test_steer_rejection_raises_and_keeps_buffered_completion(self) -> None:
        session, transport, _sink = self.start(
            events=[{"type": "turn/completed", "status": "completed", "exit_code": 0}]
        )
        transport._responses["turn/steer"] = [{"accepted": False, "reason": "turn already finished"}]

        with self.assertRaisesRegex(SteerRejected, "turn already finished"):
            session.steer("please wrap up")

        outcome = session.wait(0)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)

    def test_steer_rejects_blank_text(self) -> None:
        session, _transport, _sink = self.start()
        with self.assertRaises(ValidationError):
            session.steer("   ")

    def test_cancel_sends_native_interrupt(self) -> None:
        session, transport, _sink = self.start()
        session.cancel(5)
        self.assertIn(("turn/interrupt", {"threadId": "th_1"}), transport.requests)
        with self.assertRaises(ValidationError):
            session.cancel(-1)

    def test_malformed_message_is_routed_to_sink_event_instead_of_raising(self) -> None:
        session, _transport, sink = self.start(
            events=[{"type": "message", "role": "not-a-role", "content": "hi", "at": 1.0}]
        )
        self.assertIsNone(session.wait(0))
        self.assertEqual(sink.messages, [])
        self.assertEqual(sink.events[0][0], "malformed_message")

    def test_unknown_event_kind_is_forwarded_verbatim(self) -> None:
        session, _transport, sink = self.start(events=[{"type": "log", "text": "hello"}])
        self.assertIsNone(session.wait(0))
        self.assertEqual(sink.events, [("log", {"text": "hello"})])


if __name__ == "__main__":
    unittest.main()
