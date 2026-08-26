import os
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import LaunchPlan
from agent_run.adapters.codex.app_server import (
    EffectiveTurnParams,
    ProcessTransport,
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
        self.timeouts = []
        self.terminated = None
        self.closed = False

    @property
    def pid(self):
        return self._pid

    def request(self, method, params, *, timeout_seconds=30.0):
        self.requests.append((method, dict(params)))
        self.timeouts.append(timeout_seconds)
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
        self.assertEqual(len(transport.timeouts), 3)
        self.assertTrue(all(0 < value <= 30 for value in transport.timeouts))
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


def notification(method, params=None):
    """A real JSON-RPC server notification envelope."""

    return {"jsonrpc": "2.0", "method": method, "params": dict(params or {})}


def completed(status="completed", items=None, thread_id="th_1", **turn):
    turn = {"status": status, "items": list(items or []), **turn}
    return notification("turn/completed", {"threadId": thread_id, "turn": turn})


class RaisingSink(FakeSink):
    def __init__(self, failures=1):
        super().__init__()
        self.failures = failures

    def message(self, message):
        if self.failures > 0:
            self.failures -= 1
            raise RuntimeError("sink is down")
        super().message(message)


class CodexAppServerSessionTests(unittest.TestCase):
    def start(self, events=(), sink=None):
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
        sink = FakeSink() if sink is None else sink
        session = start_session(transport, plan, sink)
        return session, transport, sink

    def test_wait_normalizes_assistant_items_then_returns_the_terminal_outcome(self) -> None:
        session, _transport, sink = self.start(
            events=[
                notification("turn/started", {"threadId": "th_1"}),
                completed(
                    items=[
                        {"type": "reasoning", "text": "thinking"},
                        {"type": "agentMessage", "text": "hi", "at": 1.0, "id": "item_1"},
                    ],
                    exit_code=0,
                ),
            ]
        )
        self.assertIsNone(session.wait(0))
        self.assertEqual(sink.events, [("turn/started", {"threadId": "th_1"})])

        outcome = session.wait(0)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(outcome.runtime_session_id, "th_1")
        self.assertEqual(outcome.exit_code, 0)
        self.assertEqual(len(sink.messages), 1)
        self.assertEqual(sink.messages[0].role, MessageRole.ASSISTANT)
        self.assertEqual(sink.messages[0].content, "hi")
        self.assertEqual(sink.messages[0].raw_ref, "item_1")

        self.assertIsNone(session.wait(0))

    def test_success_seals_one_canonical_answer_with_proof(self) -> None:
        import hashlib

        from agent_run.adapters.codex.app_server import CodexAppServerSession
        from agent_run.verify import DEFAULT_SENTINEL

        with tempfile.TemporaryDirectory() as directory:
            answer = Path(directory).resolve() / "answer.md"
            transport = FakeTransport(
                events=[
                    completed(
                        items=[
                            {
                                "type": "agentMessage",
                                "text": "final answer",
                                "at": 1.0,
                                "id": "item_1",
                            }
                        ],
                        exit_code=0,
                    )
                ]
            )
            session = CodexAppServerSession(
                transport, FakeSink(), "th_1", answer_path=answer
            )
            outcome = session.wait(0)
            data = answer.read_bytes()
        self.assertIs(outcome.status, AgentStatus.SUCCEEDED)
        self.assertTrue(data.endswith(f"{DEFAULT_SENTINEL}\n".encode()))
        self.assertEqual(outcome.answer_path, answer)
        self.assertEqual(outcome.answer_bytes, len(data))
        self.assertEqual(outcome.answer_sha256, hashlib.sha256(data).hexdigest())

    def test_terminal_statuses_map_to_domain_outcomes(self) -> None:
        for status, expected in (
            ("completed", AgentStatus.SUCCEEDED),
            ("interrupted", AgentStatus.CANCELLED),
            ("failed", AgentStatus.FAILED),
        ):
            with self.subTest(status=status):
                session, _transport, _sink = self.start(
                    events=[completed(status=status, error={"kind": "oom", "message": "boom"})]
                )
                outcome = session.wait(0)
                self.assertEqual(outcome.status, expected)
                if status == "failed":
                    self.assertEqual(outcome.failure_kind, "oom")
                    self.assertEqual(outcome.failure_text, "boom")

    def test_in_progress_completion_is_refused_and_the_raw_event_is_retained(self) -> None:
        session, transport, _sink = self.start(events=[completed(status="inProgress")])
        with self.assertRaisesRegex(VerificationError, "nonterminal or unknown status"):
            session.wait(0)
        self.assertEqual(transport._events, [])  # the transport already handed it over
        with self.assertRaisesRegex(VerificationError, "nonterminal or unknown status"):
            session.wait(0)  # still retained, not silently dropped

    def test_unknown_status_is_refused(self) -> None:
        session, _transport, _sink = self.start(events=[completed(status="exploded")])
        with self.assertRaisesRegex(VerificationError, "nonterminal or unknown status"):
            session.wait(0)

    def test_outcome_survives_a_raising_sink(self) -> None:
        session, _transport, sink = self.start(
            events=[completed(items=[{"type": "agentMessage", "text": "hi"}])],
            sink=RaisingSink(),
        )
        with self.assertRaisesRegex(RuntimeError, "sink is down"):
            session.wait(0)
        outcome = session.wait(0)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(sink.messages, [])

    def test_completion_for_another_thread_is_forwarded_not_consumed(self) -> None:
        session, _transport, sink = self.start(events=[completed(thread_id="th_other")])
        self.assertIsNone(session.wait(0))
        self.assertEqual(sink.events[0][0], "turn/completed")
        self.assertEqual(sink.events[0][1]["threadId"], "th_other")

    def test_envelope_without_a_method_is_reported_not_dropped(self) -> None:
        session, _transport, sink = self.start(events=[{"jsonrpc": "2.0", "result": {}}])
        self.assertIsNone(session.wait(0))
        self.assertEqual(sink.events[0][0], "malformed_event")

    def test_steer_buffers_an_already_pending_completion_until_ack(self) -> None:
        session, transport, _sink = self.start(events=[completed(exit_code=0)])
        transport._responses["turn/steer"] = [{"accepted": True}]

        session.steer("please wrap up")

        outcome = session.wait(0)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        methods = [method for method, _ in transport.requests]
        self.assertIn("turn/steer", methods)

    def test_steer_rejection_raises_and_keeps_buffered_completion(self) -> None:
        session, transport, _sink = self.start(events=[completed(exit_code=0)])
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

    def test_malformed_assistant_item_is_reported_without_losing_the_outcome(self) -> None:
        session, _transport, sink = self.start(
            events=[
                completed(
                    items=[
                        {"type": "agentMessage", "text": "fine"},
                        {"type": "agentMessage", "text": "bad", "at": -1},
                    ]
                )
            ]
        )
        outcome = session.wait(0)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual([message.content for message in sink.messages], ["fine"])
        self.assertEqual(sink.events[0][0], "malformed_message")

    def test_unknown_method_forwards_its_params(self) -> None:
        session, _transport, sink = self.start(
            events=[notification("item/completed", {"item": {"type": "commandExecution"}})]
        )
        self.assertIsNone(session.wait(0))
        self.assertEqual(
            sink.events, [("item/completed", {"item": {"type": "commandExecution"}})]
        )


def transport_plan(script, tmpdir):
    return LaunchPlan(
        argv=(sys.executable, "-c", script),
        cwd=Path(tmpdir),
        environment={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        initial_input=None,
        runtime_stream_path=Path(tmpdir) / "runtime.jsonl",
        adapter_state={},
    )


_SERVER = """
import json, sys
def send(payload):
    sys.stdout.write(json.dumps(payload) + "\\n")
    sys.stdout.flush()
send({"jsonrpc": "2.0", "method": "turn/started", "params": {"n": 1}})
line = sys.stdin.readline()
request = json.loads(line)
send({"jsonrpc": "2.0", "method": "turn/log", "params": {"n": 2}})
send({"jsonrpc": "2.0", "id": request["id"], "result": {"ok": True}})
sys.stdin.readline()
"""

_SILENT_SERVER = """
import sys
sys.stdin.readline()
"""

_SLEEPING_SERVER = """
import time
time.sleep(30)
"""


class ProcessTransportTests(unittest.TestCase):
    def transport(self, script):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        transport = ProcessTransport(transport_plan(script, directory.name))
        self.addCleanup(transport.terminate, 5)
        return transport

    def test_request_returns_its_response_and_buffers_interleaved_notifications(self) -> None:
        transport = self.transport(_SERVER)
        result = transport.request("initialize", {"clientInfo": {"name": "agent-run"}})
        self.assertEqual(result, {"ok": True})

        first = transport.poll_event(2)
        second = transport.poll_event(2)
        self.assertEqual(first["method"], "turn/started")
        self.assertEqual(second["method"], "turn/log")

    def test_request_has_a_finite_deadline_while_stream_stays_open(self):
        # Use a child that sleeps well past the deadline instead of exiting,
        # so the stream is verifiably still open when the deadline fires and
        # this can't flap into the "stream closed" ConnectionError branch.
        transport = self.transport(_SLEEPING_SERVER)
        with self.assertRaisesRegex(TimeoutError, "timed out waiting"):
            transport.request("initialize", {}, timeout_seconds=0.2)

    def test_zero_timeout_never_blocks_and_finite_timeout_is_honored(self) -> None:
        transport = self.transport(_SILENT_SERVER)
        started = time.monotonic()
        self.assertIsNone(transport.poll_event(0))
        self.assertLess(time.monotonic() - started, 0.5)

        started = time.monotonic()
        self.assertIsNone(transport.poll_event(0.25))
        waited = time.monotonic() - started
        self.assertGreaterEqual(waited, 0.2)
        self.assertLess(waited, 5)

    def test_end_of_stream_yields_none_and_stays_closed(self) -> None:
        transport = self.transport(_SILENT_SERVER)
        transport.close()
        transport._process.wait(timeout=5)
        self.assertIsNone(transport.poll_event(2))
        self.assertIsNone(transport.poll_event(2))
        with self.assertRaises(ConnectionError):
            transport.request("initialize", {})


if __name__ == "__main__":
    unittest.main()
