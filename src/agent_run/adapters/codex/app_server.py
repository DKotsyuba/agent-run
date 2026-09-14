"""Codex app-server protocol transformations and session control.
ProcessTransport owns subprocess/pipes and bounded secret-safe diagnostics;
AppServerTransport keeps protocol transformations testable without Codex.
"""

from __future__ import annotations

import json
import time
from dataclasses import replace

from ..home import seal_answer
from pathlib import Path
from typing import Mapping, Protocol

from ...domain import AgentStatus, Message, MessageRole, Outcome
from ...errors import ValidationError
from ._error_classification import _structured_failure_kind
from .permissions import EffectiveTurnParams, VerificationError, thread_grant_params, verify_effective_params
from .process_transport import ProcessTransport


# Startup is outside the agent deadline: default to 30s, cap production at 120s.
_DEFAULT_STARTUP_TIMEOUT_SECONDS = 30.0
_MAX_STARTUP_TIMEOUT_SECONDS = 120.0
_PENDING_DRAIN_LIMIT, _PENDING_DRAIN_SECONDS = 64, 0.05
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

    def notify(self, method: str, params: Mapping[str, object] | None = None) -> None: ...

    def poll_event(self, timeout: float | None) -> Mapping[str, object] | None: ...

    def terminate(self, grace_seconds: float) -> None: ...

    def close(self) -> None: ...


def _thread_id_echo(actual: Mapping[str, object]) -> object:
    """Locate the started thread's id.

    Prefers the legacy top-level ``threadId``; falls back to the beta
    contract's nested ``thread.id``.
    """
    if "threadId" in actual:
        return actual.get("threadId")
    nested = actual.get("thread")
    return nested.get("id") if isinstance(nested, Mapping) else None


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
        failure_kind=_structured_failure_kind(error),
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
        turn_id: str | None = None,
        answer_path: Path | None = None,
        require_turn_id: bool = False,
    ) -> None:
        """Bind one transport to the active thread and turn.

        ``require_turn_id`` is true only for a resumed thread.  It rejects
        replayed events lacking the newly-started turn id, so a historical
        completion cannot complete the new agent run.
        """
        self._transport = transport
        self._sink = sink
        self._thread_id = thread_id
        self._turn_id = turn_id
        self._answer_path = answer_path
        self._require_turn_id = require_turn_id
        self._buffered_outcome: Outcome | None = None
        self._pending_raw: list[Mapping[str, object]] = []
        self._completed_item_ids: set[str] = set()
        self._emitted_text: dict[str, str] = {}
        self._pending_streamed: dict[str, str] = {}
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
        try:
            event = self._next_raw(timeout_seconds)
        except ConnectionError:
            self._flush_streamed()
            raise
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
        try:
            self._transport.request(
                "turn/steer",
                {
                    "threadId": self._thread_id,
                    "input": [{"type": "text", "text": text}],
                    "expectedTurnId": self._turn_id,
                },
                timeout_seconds=30.0,
            )
        except ValidationError as error:
            raise SteerRejected(str(error)) from error

    def cancel(self, grace_seconds: float) -> None:
        if isinstance(grace_seconds, bool) or not isinstance(grace_seconds, (int, float)) or grace_seconds < 0:
            raise ValidationError("grace_seconds must be a nonnegative number")
        self._transport.request(
            "turn/interrupt",
            {"threadId": self._thread_id, "turnId": self._turn_id},
            timeout_seconds=max(float(grace_seconds), 0.001),
        )

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._flush_streamed()
            self._transport.close()

    def _drain_pending(self) -> None:
        """Handle a bounded pending-event slice before lower-priority steering."""
        deadline = time.monotonic() + _PENDING_DRAIN_SECONDS
        for _ in range(_PENDING_DRAIN_LIMIT):
            event = self._next_raw(0)
            if event is None or time.monotonic() >= deadline:
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

    def _current_event(self, params: Mapping[str, object]) -> bool:
        """Return whether event parameters identify this run's thread and turn.

        Accept both the flat item-event turnId and terminal-event turn.id.
        Resumed runs require an explicit matching turn; missing identity is
        insufficient evidence that a historical event belongs to this run.
        """
        thread_id = params.get("threadId")
        if isinstance(thread_id, str) and thread_id and thread_id != self._thread_id:
            return False
        turn_id = params.get("turnId")
        if not isinstance(turn_id, str):
            turn_id = _mapping(params.get("turn")).get("id")
        if self._require_turn_id:
            return isinstance(turn_id, str) and turn_id == self._turn_id
        return not (
            isinstance(turn_id, str)
            and turn_id
            and self._turn_id is not None
            and turn_id != self._turn_id
        )

    def _emit_messages(
        self, messages: tuple[Message, ...], malformed: tuple[Mapping[str, object], ...]
    ) -> tuple[Message, ...]:
        """Persist canonical items after subtracting only accepted stream text."""

        fresh = []
        for message in messages:
            item_id = message.raw_ref
            if item_id and item_id in self._completed_item_ids:
                continue
            emitted = self._emitted_text.get(item_id, "") if item_id else ""
            if item_id and emitted and message.content.startswith(emitted):
                suffix = message.content[len(emitted) :]
                if not suffix:
                    self._completed_item_ids.add(item_id)
                    self._emitted_text.pop(item_id, None)
                    self._pending_streamed.pop(item_id, None)
                    continue
                message = replace(
                    message, content=suffix, raw_ref=f"{item_id}:stream:{len(emitted)}"
                )
            self._sink.message(message)
            fresh.append(message)
            if item_id:
                self._completed_item_ids.add(item_id)
                self._emitted_text.pop(item_id, None)
                self._pending_streamed.pop(item_id, None)
        for record in malformed:
            self._sink.event("malformed_message", dict(record))
        return tuple(fresh)

    def _emit_delta(self, params: Mapping[str, object]) -> bool:
        """Buffer a matching assistant delta without inferring an outcome."""

        item_id = params.get("itemId")
        delta = params.get("delta")
        if (
            not isinstance(item_id, str)
            or not item_id
            or not isinstance(delta, str)
            or not delta
            or item_id in self._completed_item_ids
        ):
            return False
        pending = self._pending_streamed.get(item_id, "")
        if delta.strip() and pending.strip():
            self._flush_streamed(item_id)
            pending = ""
        self._pending_streamed[item_id] = pending + delta
        return True

    def _flush_streamed(self, item_id: str | None = None) -> None:
        """Persist pending nonblank text at an actual stream boundary."""

        item_ids = tuple(self._pending_streamed) if item_id is None else (item_id,)
        for current_id in item_ids:
            pending = self._pending_streamed.get(current_id, "")
            if pending.strip() and current_id not in self._completed_item_ids:
                emitted = self._emitted_text.get(current_id, "")
                self._sink.message(
                    _normalize_message(
                        {
                            "type": "agentMessage",
                            "id": f"{current_id}:stream:{len(emitted)}",
                            "text": pending,
                            "at": time.time(),
                        }
                    )
                )
                self._emitted_text[current_id] = emitted + pending
            self._pending_streamed.pop(current_id, None)



    def _handle_event(self, event: Mapping[str, object]) -> None:
        """Normalize completed assistant items without treating stream text as proof."""

        method = event.get("method")
        params = _mapping(event.get("params"))
        if not isinstance(method, str) or not method:
            self._consume_raw()
            self._sink.event("malformed_event", {"raw": dict(event)})
            return
        if method == "item/agentMessage/delta" and self._current_event(params):
            self._consume_raw()
            self._sink.event(method, dict(params))
            self._emit_delta(params)
            return
        if method == "item/completed" and self._current_event(params):
            item = _mapping(params.get("item"))
            item_id = item.get("id")
            messages, malformed = _assistant_messages({"items": [item]})
            if messages or malformed:
                self._consume_raw()
                self._emit_messages(messages, malformed)
                return
        if method != "turn/completed":
            self._consume_raw()
            self._sink.event(method, dict(params))
            return
        if not self._current_event(params):
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
        self._emit_messages(messages, malformed)
        self._flush_streamed()


def start_session(transport: AppServerTransport, plan, sink) -> CodexAppServerSession:
    """Start a Codex session and verify its effective parameters.

    Initializes the experimental API required by workspace roots and permission
    profile provenance. A named-profile plan relies on its generated
    ``default_permissions`` and omits conflicting legacy sandbox fields; legacy
    plans still send ``runtimeWorkspaceRoots`` and ``sandbox``. Both paths
    compare the returned sandbox, roots, approval settings, and active profile.
    A plan with ``resume_session_id`` uses ``thread/resume``
    and must return that exact identity; it never falls back to a fresh
    thread. Raises ``VerificationError`` for effective-param drift or an
    already active native session.
    Unsupported history is rejected by the runtime's resume request itself.
    """

    state = plan.adapter_state
    configured = state.get("request_timeout_seconds")
    timeout = (
        min(float(configured), _MAX_STARTUP_TIMEOUT_SECONDS)
        if isinstance(configured, (int, float))
        and not isinstance(configured, bool)
        and configured > 0
        else _DEFAULT_STARTUP_TIMEOUT_SECONDS
    )
    deadline = time.monotonic() + timeout

    def remaining() -> float:
        value = deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError("codex app-server startup timed out")
        return value

    transport.request(
        "initialize",
        {
            "clientInfo": {"name": "agent-run", "version": "1"},
            "capabilities": {"experimentalApi": True},
        },
        timeout_seconds=remaining(),
    )
    transport.notify("initialized")
    roots = tuple(state["roots"])
    writable_roots = tuple(state["writable_roots"])
    sandbox_mode = state["sandbox_mode"]
    network_access = bool(state.get("network_access", False))
    approvals_reviewer = state.get("approvals_reviewer")
    permission_profile = state.get("permission_profile")
    grant_params = thread_grant_params(
        str(plan.cwd), state["model"], sandbox_mode, state["approval_policy"], roots, network_access,
        approvals_reviewer, permission_profile,
    )
    resume_session_id = plan.resume_session_id
    if resume_session_id is None:
        thread = transport.request("thread/start", grant_params, timeout_seconds=remaining())
    else:
        # A bare ``{"threadId": ...}`` resume once let a native continuation
        # silently drop its original write grant (observed live: resume
        # returned a read-only echo against a recorded workspace-write
        # session). Supplying the original grant does not by itself
        # guarantee the runtime honors it -- a control probe kept its grant
        # even when these fields were omitted -- so this only prevents
        # unnoticed drift; ``verify_effective_params`` below still fails
        # closed on whatever the server actually echoes.
        thread = transport.request(
            "thread/resume",
            {**grant_params, "threadId": resume_session_id},
            timeout_seconds=remaining(),
        )
        native_status = _mapping(thread.get("thread")).get("status", thread.get("status"))
        if isinstance(native_status, Mapping):
            native_status = native_status.get("type")
        if native_status in ("active", "running"):
            raise VerificationError("codex thread/resume found an active native session")
    expected = EffectiveTurnParams(
        model=state["model"],
        cwd=str(plan.cwd),
        roots=roots,
        sandbox=state["sandbox_mode"],
        approval_policy=state["approval_policy"],
        writable_roots=writable_roots,
        network_access=network_access,
        permission_profile=permission_profile,
    )
    verify_effective_params(expected, thread)
    if approvals_reviewer is not None and thread.get("approvalsReviewer") != approvals_reviewer:
        raise VerificationError("codex thread/start approvalsReviewer mismatch")
    thread_id = _thread_id_echo(thread)
    if not isinstance(thread_id, str) or not thread_id:
        raise VerificationError("codex thread start/resume did not return a threadId")
    if resume_session_id is not None and thread_id != resume_session_id:
        raise VerificationError(
            "codex thread/resume returned a different threadId: "
            f"expected {resume_session_id!r}, got {thread_id!r}"
        )
    sink.session(thread_id)
    turn_params: dict[str, object] = {
        "threadId": thread_id,
        "input": [{"type": "text", "text": plan.initial_input}],
    }
    effort = state.get("effort")
    if effort is not None:
        turn_params["effort"] = effort
    turn_ack = transport.request("turn/start", turn_params, timeout_seconds=remaining())
    turn = turn_ack.get("turn")
    turn_id = turn.get("id") if isinstance(turn, Mapping) else None
    if not isinstance(turn_id, str) or not turn_id:
        raise VerificationError("codex turn/start did not return a turn id")
    return CodexAppServerSession(
        transport,
        sink,
        thread_id,
        turn_id=turn_id,
        answer_path=plan.answer_path,
        require_turn_id=resume_session_id is not None,
    )


def fetch_models(plan, *, timeout_seconds: float = 20.0) -> tuple[Mapping[str, object], ...]:
    """Fetch the app-server model catalog without starting a thread or turn."""

    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not 0 < timeout_seconds < float("inf")
    ):
        raise ValidationError("timeout_seconds must be positive and finite")
    transport = ProcessTransport(plan)
    deadline = time.monotonic() + float(timeout_seconds)

    def remaining() -> float:
        value = deadline - time.monotonic()
        if value <= 0:
            raise TimeoutError("codex app-server model refresh timed out")
        return value

    try:
        transport.request(
            "initialize",
            {"clientInfo": {"name": "agent-run", "version": "1"}},
            timeout_seconds=remaining(),
        )
        transport.notify("initialized")
        models: list[Mapping[str, object]] = []
        cursor: str | None = None
        while True:
            params: dict[str, object] = {"includeHidden": True, "limit": 1000}
            if cursor is not None:
                params["cursor"] = cursor
            response = transport.request(
                "model/list", params, timeout_seconds=remaining()
            )
            data = response.get("data")
            if not isinstance(data, list) or not all(isinstance(item, Mapping) for item in data):
                raise ValidationError("codex app-server model/list returned malformed data")
            models.extend(data)
            next_cursor = response.get("nextCursor")
            if next_cursor is None:
                next_cursor = response.get("next_cursor")
            if next_cursor is None:
                return tuple(models)
            if not isinstance(next_cursor, str) or not next_cursor or next_cursor == cursor:
                raise ValidationError("codex app-server model/list returned malformed cursor")
            cursor = next_cursor
    finally:
        try:
            transport.terminate(1.0)
        except Exception:
            transport.close()
