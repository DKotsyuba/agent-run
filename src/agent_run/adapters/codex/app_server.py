"""Codex ``app-server`` protocol: initialize/thread/turn/steer/completion.

No live process I/O happens in this module's tested paths; ``ProcessTransport``
wraps the real subprocess and is exercised only inside the detached
supervisor (deferred to M011). Every other function here is a pure,
deterministic transformation driven through the ``AppServerTransport``
protocol so it can be tested with a fake transport.
"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Mapping, Protocol

from ...domain import AgentStatus, Message, MessageRole, Outcome
from ...errors import ValidationError


class VerificationError(ValidationError):
    """Effective app-server parameters do not match the requested launch plan."""


class SteerRejected(ValidationError):
    """The codex app-server rejected a steer request."""


class AppServerTransport(Protocol):
    @property
    def pid(self) -> int | None: ...

    def request(self, method: str, params: Mapping[str, object]) -> Mapping[str, object]: ...

    def poll_event(self, timeout: float | None) -> Mapping[str, object] | None: ...

    def terminate(self, grace_seconds: float) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class EffectiveTurnParams:
    model: str
    cwd: str
    roots: tuple[str, ...]
    sandbox: str
    approval_policy: str
    writable_roots: tuple[str, ...]


def verify_effective_params(expected: EffectiveTurnParams, actual: Mapping[str, object]) -> None:
    """Refuse a thread whose effective params drift from what was requested.

    In particular, read roots must never appear in ``writableRoots``: only the
    cwd may be writable, and only when the request actually granted write.
    """

    scalar_checks = (
        ("model", expected.model),
        ("cwd", expected.cwd),
        ("sandbox", expected.sandbox),
        ("approvalPolicy", expected.approval_policy),
    )
    for key, wanted in scalar_checks:
        got = actual.get(key)
        if got != wanted:
            raise VerificationError(
                f"codex thread/start {key} mismatch: expected {wanted!r}, got {got!r}"
            )
    got_roots = tuple(actual.get("roots") or ())
    if got_roots != expected.roots:
        raise VerificationError(
            f"codex thread/start roots mismatch: expected {expected.roots!r}, got {got_roots!r}"
        )
    got_writable = tuple(actual.get("writableRoots") or ())
    if got_writable != expected.writable_roots:
        raise VerificationError(
            "codex thread/start writableRoots mismatch: expected "
            f"{expected.writable_roots!r}, got {got_writable!r}"
        )


_STATUS_MAP: Mapping[str, AgentStatus] = {
    "completed": AgentStatus.SUCCEEDED,
    "failed": AgentStatus.FAILED,
    "cancelled": AgentStatus.CANCELLED,
}


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _normalize_message(event: Mapping[str, object]) -> Message:
    return Message(
        at=float(event.get("at", 0.0)),
        role=MessageRole(event.get("role")),
        content=str(event.get("content", "")),
        name=_optional_str(event.get("name")),
        raw_ref=_optional_str(event.get("raw_ref")),
    )


def _normalize_outcome(event: Mapping[str, object], thread_id: str) -> Outcome:
    status = _STATUS_MAP.get(event.get("status"))
    if status is None:
        raise VerificationError(
            f"codex turn/completed reported an unknown status: {event.get('status')!r}"
        )
    answer_path = _optional_str(event.get("answer_path"))
    return Outcome(
        status=status,
        exit_code=_optional_int(event.get("exit_code")),
        failure_kind=_optional_str(event.get("failure_kind")),
        failure_text=_optional_str(event.get("failure_text")),
        runtime_session_id=thread_id,
        answer_path=Path(answer_path) if answer_path else None,
        answer_bytes=_optional_int(event.get("answer_bytes")),
        answer_sha256=_optional_str(event.get("answer_sha256")),
    )


class CodexAppServerSession:
    """Drives one codex thread and normalizes its events for an ``EventSink``."""

    def __init__(self, transport: AppServerTransport, sink, thread_id: str) -> None:
        self._transport = transport
        self._sink = sink
        self._thread_id = thread_id
        self._buffered_outcome: Outcome | None = None
        self._closed = False

    @property
    def pid(self) -> int | None:
        return self._transport.pid

    def wait(self, timeout_seconds: float | None) -> Outcome | None:
        if self._buffered_outcome is not None:
            return self._pop_outcome()
        event = self._transport.poll_event(timeout_seconds)
        if event is None:
            return None
        self._handle_event(event)
        if self._buffered_outcome is not None:
            return self._pop_outcome()
        return None

    def steer(self, text: str) -> None:
        if not isinstance(text, str) or not text.strip():
            raise ValidationError("steer text must be nonblank")
        self._drain_pending()
        response = self._transport.request("turn/steer", {"threadId": self._thread_id, "text": text})
        if not response.get("accepted"):
            reason = response.get("reason")
            raise SteerRejected(str(reason) if reason else "codex rejected the steer request")

    def cancel(self, grace_seconds: float) -> None:
        if isinstance(grace_seconds, bool) or not isinstance(grace_seconds, (int, float)) or grace_seconds < 0:
            raise ValidationError("grace_seconds must be a nonnegative number")
        self._drain_pending()
        self._transport.request("turn/interrupt", {"threadId": self._thread_id})

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._transport.close()

    def _drain_pending(self) -> None:
        while True:
            event = self._transport.poll_event(0)
            if event is None:
                return
            self._handle_event(event)

    def _pop_outcome(self) -> Outcome:
        outcome = self._buffered_outcome
        self._buffered_outcome = None
        return outcome

    def _handle_event(self, event: Mapping[str, object]) -> None:
        kind = event.get("type")
        if kind == "message":
            try:
                self._sink.message(_normalize_message(event))
            except (ValidationError, ValueError) as error:
                self._sink.event("malformed_message", {"error": str(error), "raw": dict(event)})
        elif kind == "turn/completed":
            self._buffered_outcome = _normalize_outcome(event, self._thread_id)
        elif kind == "session":
            thread_id = event.get("threadId")
            if isinstance(thread_id, str) and thread_id:
                self._sink.session(thread_id)
        else:
            data = {key: value for key, value in event.items() if key != "type"}
            self._sink.event(str(kind), data)


def start_session(transport: AppServerTransport, plan, sink) -> CodexAppServerSession:
    """Run initialize/thread/turn/start and verify the effective params."""

    state = plan.adapter_state
    transport.request("initialize", {"clientInfo": {"name": "agent-run", "version": "1"}})
    roots = tuple(state["roots"])
    writable_roots = tuple(state["writable_roots"])
    thread = transport.request(
        "thread/start",
        {
            "cwd": str(plan.cwd),
            "model": state["model"],
            "effort": state.get("effort"),
            "sandbox": state["sandbox_mode"],
            "approvalPolicy": state["approval_policy"],
            "roots": list(roots),
            "writableRoots": list(writable_roots),
            "mcpServers": list(state.get("mcp", ())),
            "skills": list(state.get("skills", ())),
        },
    )
    expected = EffectiveTurnParams(
        model=state["model"],
        cwd=str(plan.cwd),
        roots=roots,
        sandbox=state["sandbox_mode"],
        approval_policy=state["approval_policy"],
        writable_roots=writable_roots,
    )
    verify_effective_params(expected, thread)
    thread_id = thread.get("threadId")
    if not isinstance(thread_id, str) or not thread_id:
        raise VerificationError("codex thread/start did not return a threadId")
    sink.session(thread_id)
    transport.request("turn/start", {"threadId": thread_id, "input": plan.initial_input})
    return CodexAppServerSession(transport, sink, thread_id)


class ProcessTransport:
    """Real ``codex app-server`` subprocess speaking newline-delimited JSON-RPC."""

    def __init__(self, plan) -> None:
        self._process = subprocess.Popen(
            list(plan.argv),
            cwd=str(plan.cwd),
            env=dict(plan.environment),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        self._next_id = 1
        self._buffer: list[Mapping[str, object]] = []

    @property
    def pid(self) -> int | None:
        return self._process.pid

    def request(self, method: str, params: Mapping[str, object]) -> Mapping[str, object]:
        request_id = self._next_id
        self._next_id += 1
        payload = json.dumps(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": dict(params)}
        ).encode("utf-8")
        assert self._process.stdin is not None and self._process.stdout is not None
        self._process.stdin.write(payload + b"\n")
        self._process.stdin.flush()
        while True:
            line = self._process.stdout.readline()
            if not line:
                raise ConnectionError(f"codex app-server closed the stream while waiting for {method}")
            message = json.loads(line)
            if message.get("id") == request_id:
                if "error" in message:
                    raise ValidationError(f"codex app-server rejected {method}: {message['error']}")
                return message.get("result", {})
            self._buffer.append(message)

    def poll_event(self, timeout: float | None) -> Mapping[str, object] | None:
        if self._buffer:
            return self._buffer.pop(0)
        assert self._process.stdout is not None
        line = self._process.stdout.readline()
        if not line:
            return None
        return json.loads(line)

    def terminate(self, grace_seconds: float) -> None:
        self._process.terminate()
        try:
            self._process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=max(grace_seconds, 1))

    def close(self) -> None:
        if self._process.stdin:
            self._process.stdin.close()
