"""Codex ``app-server`` protocol: initialize/thread/turn/steer/completion.

No live process I/O happens in this module's tested paths; ``ProcessTransport``
wraps the real subprocess and is exercised only inside the detached
supervisor (deferred to M011). Every other function here is a pure,
deterministic transformation driven through the ``AppServerTransport``
protocol so it can be tested with a fake transport.
"""

from __future__ import annotations

import json
import queue
import subprocess
import threading
import time
from collections import deque
from dataclasses import dataclass, replace

from ..home import seal_answer
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

    def request(
        self,
        method: str,
        params: Mapping[str, object],
        *,
        timeout_seconds: float = 30.0,
    ) -> Mapping[str, object]: ...

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


#: Only these turn statuses end a turn; ``inProgress`` is not a completion.
_TERMINAL_STATUS: Mapping[str, AgentStatus] = {
    "completed": AgentStatus.SUCCEEDED,
    "interrupted": AgentStatus.CANCELLED,
    "failed": AgentStatus.FAILED,
}


def _optional_str(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _optional_int(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


def _item_time(value: object) -> object:
    """Absent timestamps default to now; a present one is validated, not repaired."""

    return time.time() if value is None else value


def _normalize_message(item: Mapping[str, object]) -> Message:
    return Message(
        at=_item_time(item.get("at")),
        role=MessageRole.ASSISTANT,
        content=str(item.get("text", "")),
        name=_optional_str(item.get("name")),
        raw_ref=_optional_str(item.get("id")),
    )


def _assistant_messages(
    turn: Mapping[str, object],
) -> tuple[tuple[Message, ...], tuple[Mapping[str, object], ...]]:
    """Split ``turn.items`` into normalized assistant messages and malformed items."""

    items = turn.get("items")
    if not isinstance(items, list):
        return (), ()
    messages: list[Message] = []
    malformed: list[Mapping[str, object]] = []
    for item in items:
        if not isinstance(item, Mapping) or item.get("type") != "agentMessage":
            continue
        try:
            messages.append(_normalize_message(item))
        except (ValidationError, ValueError) as error:
            malformed.append({"error": str(error), "raw": dict(item)})
    return tuple(messages), tuple(malformed)


def _normalize_outcome(turn: Mapping[str, object], thread_id: str) -> Outcome:
    status_raw = turn.get("status")
    status = _TERMINAL_STATUS.get(status_raw) if isinstance(status_raw, str) else None
    if status is None:
        raise VerificationError(
            f"codex turn/completed reported a nonterminal or unknown status: {status_raw!r}"
        )
    error = _mapping(turn.get("error"))
    answer_path = _optional_str(turn.get("answer_path"))
    return Outcome(
        status=status,
        exit_code=_optional_int(turn.get("exit_code")),
        failure_kind=_optional_str(error.get("kind") or error.get("code")),
        failure_text=_optional_str(error.get("message")),
        runtime_session_id=thread_id,
        answer_path=Path(answer_path) if answer_path else None,
        answer_bytes=_optional_int(turn.get("answer_bytes")),
        answer_sha256=_optional_str(turn.get("answer_sha256")),
    )


class CodexAppServerSession:
    """Drives one codex thread and normalizes its events for an ``EventSink``."""

    def __init__(
        self,
        transport: AppServerTransport,
        sink,
        thread_id: str,
        *,
        answer_path: Path | None = None,
    ) -> None:
        self._transport = transport
        self._sink = sink
        self._thread_id = thread_id
        self._answer_path = answer_path
        self._buffered_outcome: Outcome | None = None
        self._pending_raw: list[Mapping[str, object]] = []
        self._closed = False

    @property
    def pid(self) -> int | None:
        return self._transport.pid

    @property
    def owns_process_group(self) -> bool:
        return True

    def wait(self, timeout_seconds: float | None) -> Outcome | None:
        if self._buffered_outcome is not None:
            return self._pop_outcome()
        event = self._next_raw(timeout_seconds)
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
        response = self._transport.request(
            "turn/steer",
            {"threadId": self._thread_id, "text": text},
            timeout_seconds=30.0,
        )
        if not response.get("accepted"):
            reason = response.get("reason")
            raise SteerRejected(str(reason) if reason else "codex rejected the steer request")

    def cancel(self, grace_seconds: float) -> None:
        if isinstance(grace_seconds, bool) or not isinstance(grace_seconds, (int, float)) or grace_seconds < 0:
            raise ValidationError("grace_seconds must be a nonnegative number")
        self._drain_pending()
        self._transport.request(
            "turn/interrupt",
            {"threadId": self._thread_id},
            timeout_seconds=max(float(grace_seconds), 0.001),
        )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._transport.close()

    def _drain_pending(self) -> None:
        while True:
            event = self._next_raw(0)
            if event is None:
                return
            self._handle_event(event)

    def _next_raw(self, timeout: float | None) -> Mapping[str, object] | None:
        """Return the next raw envelope, keeping it retained until it is normalized."""

        if self._pending_raw:
            return self._pending_raw[0]
        event = self._transport.poll_event(timeout)
        if event is not None:
            self._pending_raw.append(event)
        return event

    def _consume_raw(self) -> None:
        if self._pending_raw:
            self._pending_raw.pop(0)

    def _pop_outcome(self) -> Outcome:
        outcome = self._buffered_outcome
        self._buffered_outcome = None
        return outcome

    def _handle_event(self, event: Mapping[str, object]) -> None:
        """Interpret one JSON-RPC notification: ``{method, params}``.

        The raw envelope stays retained until it has been normalized, so a
        refused completion is not lost; once an outcome exists it is buffered
        before any sink call, so a raising sink cannot drop it either.
        """

        method = event.get("method")
        params = _mapping(event.get("params"))
        if not isinstance(method, str) or not method:
            self._consume_raw()
            self._sink.event("malformed_event", {"raw": dict(event)})
            return
        if method != "turn/completed":
            self._consume_raw()
            self._sink.event(method, dict(params))
            return
        thread_id = params.get("threadId")
        if isinstance(thread_id, str) and thread_id and thread_id != self._thread_id:
            self._consume_raw()
            self._sink.event(method, dict(params))
            return
        turn = _mapping(params.get("turn"))
        outcome = _normalize_outcome(turn, self._thread_id)
        messages, malformed = _assistant_messages(turn)
        if (
            outcome.status is AgentStatus.SUCCEEDED
            and self._answer_path is not None
            and messages
        ):
            size, digest = seal_answer(
                self._answer_path, "\n\n".join(message.content for message in messages)
            )
            outcome = replace(
                outcome,
                answer_path=self._answer_path,
                answer_bytes=size,
                answer_sha256=digest,
            )
        self._consume_raw()
        self._buffered_outcome = outcome
        for message in messages:
            self._sink.message(message)
        for record in malformed:
            self._sink.event("malformed_message", dict(record))


def start_session(transport: AppServerTransport, plan, sink) -> CodexAppServerSession:
    """Run initialize/thread/turn/start and verify the effective params."""

    state = plan.adapter_state
    configured = state.get("request_timeout_seconds")
    timeout = (
        min(float(configured), 30.0)
        if isinstance(configured, (int, float))
        and not isinstance(configured, bool)
        and configured > 0
        else 30.0
    )
    deadline = time.monotonic() + timeout

    def remaining() -> float:
        value = deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError("codex app-server startup timed out")
        return value

    transport.request(
        "initialize",
        {"clientInfo": {"name": "agent-run", "version": "1"}},
        timeout_seconds=remaining(),
    )
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
        timeout_seconds=remaining(),
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
    transport.request(
        "turn/start",
        {"threadId": thread_id, "input": plan.initial_input},
        timeout_seconds=remaining(),
    )
    return CodexAppServerSession(
        transport, sink, thread_id, answer_path=plan.answer_path
    )


_STREAM_CLOSED = object()


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
        self._incoming: queue.Queue = queue.Queue()
        self._notifications: deque = deque()
        self._reader = threading.Thread(
            target=self._read_stream, name="codex-app-server-reader", daemon=True
        )
        self._reader.start()

    @property
    def pid(self) -> int | None:
        return self._process.pid

    def _read_stream(self) -> None:
        """Drain stdout on a daemon thread so no caller ever blocks on readline."""

        stream = self._process.stdout
        try:
            if stream is not None:
                for line in iter(stream.readline, b""):
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        message = json.loads(text)
                    except ValueError:
                        continue
                    if isinstance(message, dict):
                        self._incoming.put(message)
        finally:
            self._incoming.put(_STREAM_CLOSED)

    def _take(self, timeout: float | None) -> Mapping[str, object] | None:
        """Pull one parsed message; ``None`` on timeout or end of stream."""

        try:
            if timeout is None:
                message = self._incoming.get()
            elif timeout <= 0:
                message = self._incoming.get_nowait()
            else:
                message = self._incoming.get(timeout=timeout)
        except queue.Empty:
            return None
        if message is _STREAM_CLOSED:
            self._incoming.put(_STREAM_CLOSED)
            return None
        return message

    def request(
        self,
        method: str,
        params: Mapping[str, object],
        *,
        timeout_seconds: float = 30.0,
    ) -> Mapping[str, object]:
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not 0 < timeout_seconds < float("inf")
        ):
            raise ValidationError("timeout_seconds must be positive and finite")
        deadline = time.monotonic() + float(timeout_seconds)
        request_id = self._next_id
        self._next_id += 1
        payload = json.dumps(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": dict(params)}
        ).encode("utf-8")
        if self._process.stdin is None:
            raise ConnectionError(f"codex app-server has no stdin for {method}")
        try:
            self._process.stdin.write(payload + b"\n")
            self._process.stdin.flush()
        except (OSError, ValueError) as error:
            raise ConnectionError(f"codex app-server stdin is closed for {method}") from error
        while True:
            remaining = max(deadline - time.monotonic(), 0.0)
            message = self._take(remaining)
            if message is None:
                if self._process.poll() is None:
                    raise TimeoutError(f"codex app-server timed out waiting for {method}")
                raise ConnectionError(
                    f"codex app-server closed the stream while waiting for {method}"
                )
            if message.get("id") == request_id:
                if "error" in message:
                    raise ValidationError(f"codex app-server rejected {method}: {message['error']}")
                result = message.get("result", {})
                return result if isinstance(result, Mapping) else {}
            if isinstance(message.get("method"), str):
                self._notifications.append(message)

    def poll_event(self, timeout: float | None) -> Mapping[str, object] | None:
        if self._notifications:
            return self._notifications.popleft()
        deadline = None if timeout is None else time.monotonic() + max(float(timeout), 0.0)
        while True:
            remaining = None if deadline is None else max(deadline - time.monotonic(), 0.0)
            message = self._take(remaining)
            if message is None:
                return None
            if isinstance(message.get("method"), str):
                return message
            if deadline is not None and time.monotonic() >= deadline:
                return None

    def terminate(self, grace_seconds: float) -> None:
        self._process.terminate()
        try:
            self._process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=max(grace_seconds, 1))
        finally:
            # The reader owns stdout, so the pipes can only be released once it
            # has seen end of stream.
            self._reader.join(timeout=1)
            self._close_pipes()

    def close(self) -> None:
        if self._process.stdin:
            self._process.stdin.close()

    def _close_pipes(self) -> None:
        for pipe in (self._process.stdin, self._process.stdout):
            if pipe is not None:
                try:
                    pipe.close()
                except OSError:
                    pass
