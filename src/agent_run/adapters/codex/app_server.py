"""Codex ``app-server`` protocol: initialize/thread/turn/steer/completion.

``ProcessTransport`` owns the real subprocess, its JSON-RPC pipes, and bounded
secret-safe startup diagnostics. Protocol transformations remain testable
through the ``AppServerTransport`` fake without launching Codex.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, replace

from ..home import seal_answer
from pathlib import Path
from typing import Mapping, Protocol

from ...domain import AgentStatus, Message, MessageRole, Outcome
from ...errors import ValidationError
from .process_transport import ProcessTransport


# Startup is outside the agent deadline: default to 30s, cap production at 120s.
_DEFAULT_STARTUP_TIMEOUT_SECONDS = 30.0
_MAX_STARTUP_TIMEOUT_SECONDS = 120.0
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

    def notify(self, method: str, params: Mapping[str, object] | None = None) -> None: ...

    def poll_event(self, timeout: float | None) -> Mapping[str, object] | None: ...

    def terminate(self, grace_seconds: float) -> None: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class EffectiveTurnParams:
    """Security-relevant parameters requested for one Codex thread."""

    model: str
    cwd: str
    roots: tuple[str, ...]
    sandbox: str
    approval_policy: str
    writable_roots: tuple[str, ...]
    network_access: bool = False


#: The app-server's beta contract echoes ``sandbox`` in ``thread/start`` as an
#: object (``{'type': 'readOnly', 'networkAccess': False, ...}``) instead of
#: the legacy kebab-case string. Map its camelCase ``type`` to this codebase's
#: kebab-case sandbox names, following the same word-boundary convention the
#: legacy names already use (``read-only``/``readOnly``,
#: ``workspace-write``/``workspaceWrite``); ``danger-full-access`` is the
#: third sandbox mode codex ships and follows the identical convention.
#: ``networkAccess`` is not checked here: this codebase does not encode a
#: per-sandbox network expectation, so it stays purely informational.
_SANDBOX_ECHO_TYPES: Mapping[str, str] = {
    "readOnly": "read-only",
    "workspaceWrite": "workspace-write",
    "dangerFullAccess": "danger-full-access",
}


def _normalized_sandbox_echo(value: object) -> object:
    """Reduce a ``thread/start`` sandbox echo to its kebab-case form.

    Passes the legacy string form through unchanged. Reduces the beta object
    form via ``_SANDBOX_ECHO_TYPES``. Returns ``value`` unchanged for any
    other shape (including an unrecognized ``type``), so the caller's
    equality check against the requested sandbox still fails closed.
    """
    if isinstance(value, Mapping):
        return _SANDBOX_ECHO_TYPES.get(value.get("type"), value)
    return value


#: The same beta contract renames ``thread/start``'s top-level ``roots`` echo
#: to ``runtimeWorkspaceRoots``, and moves ``writableRoots`` inside the
#: ``sandbox`` object -- dropping ``cwd`` from that list entirely, since a
#: ``workspaceWrite`` sandbox already implies the cwd is writable. The
#: ``sandbox`` scalar check above verifies the sandbox *type* against the
#: request separately, so this cwd substitution below never masks a genuine
#: sandbox-mode mismatch. It also renames the started thread's top-level
#: ``threadId`` to a nested ``thread.id``.
def _normalized_roots_echo(actual: Mapping[str, object]) -> tuple[str, ...]:
    """Reduce a ``thread/start`` roots echo to a tuple.

    Prefers the legacy top-level ``roots`` key when present (covers any
    codex version still using it); falls back to the beta contract's
    ``runtimeWorkspaceRoots``. Neither key present normalizes to ``()``, so
    the caller's equality check against the requested roots still fails
    closed.
    """
    if "roots" in actual:
        return tuple(actual.get("roots") or ())
    return tuple(actual.get("runtimeWorkspaceRoots") or ())


def _normalized_writable_roots_echo(actual: Mapping[str, object]) -> tuple[str, ...]:
    """Reduce a ``thread/start`` writableRoots echo to a tuple.

    Prefers the legacy top-level ``writableRoots`` key when present. The
    beta contract nests it under ``sandbox`` instead and omits ``cwd`` from
    the list, so an empty nested list under a ``workspaceWrite`` sandbox is
    normalized back to ``(cwd,)``. Any other shape -- a non-empty nested
    list, a non-``workspaceWrite`` sandbox, or no ``sandbox`` object at all
    -- passes through unchanged, so a genuine mismatch still fails closed.
    """
    if "writableRoots" in actual:
        return tuple(actual.get("writableRoots") or ())
    sandbox = actual.get("sandbox")
    if not isinstance(sandbox, Mapping):
        return ()
    nested = tuple(sandbox.get("writableRoots") or ())
    if nested:
        return nested
    if sandbox.get("type") == "workspaceWrite":
        cwd = actual.get("cwd")
        if isinstance(cwd, str) and cwd:
            return (cwd,)
    return nested


def _thread_id_echo(actual: Mapping[str, object]) -> object:
    """Locate the started thread's id.

    Prefers the legacy top-level ``threadId``; falls back to the beta
    contract's nested ``thread.id``.
    """
    if "threadId" in actual:
        return actual.get("threadId")
    nested = actual.get("thread")
    return nested.get("id") if isinstance(nested, Mapping) else None


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
        compare = _normalized_sandbox_echo(got) if key == "sandbox" else got
        if compare != wanted:
            raise VerificationError(
                f"codex thread/start {key} mismatch: expected {wanted!r}, got {got!r}"
            )
    got_roots = _normalized_roots_echo(actual)
    if got_roots != expected.roots:
        raise VerificationError(
            f"codex thread/start roots mismatch: expected {expected.roots!r}, got {got_roots!r}"
        )
    got_writable = _normalized_writable_roots_echo(actual)
    if got_writable != expected.writable_roots:
        raise VerificationError(
            "codex thread/start writableRoots mismatch: expected "
            f"{expected.writable_roots!r}, got {got_writable!r}"
        )
    if expected.network_access:
        sandbox = actual.get("sandbox")
        if not isinstance(sandbox, Mapping) or sandbox.get("networkAccess") is not True:
            raise VerificationError("codex thread/start did not enable requested network access")


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
        turn_id: str | None = None,
        answer_path: Path | None = None,
    ) -> None:
        self._transport = transport
        self._sink = sink
        self._thread_id = thread_id
        self._turn_id = turn_id
        self._answer_path = answer_path
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
        self._drain_pending()
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

    def _current_event(self, params: Mapping[str, object]) -> bool:
        """Return whether an item event belongs to this active thread and turn."""

        thread_id = params.get("threadId")
        if isinstance(thread_id, str) and thread_id and thread_id != self._thread_id:
            return False
        turn_id = params.get("turnId")
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

    Forwards the adapter's sandbox request unchanged, including a tagged
    network sandbox, then compares the server's flat echo to the requested
    permissions. Raises ``VerificationError`` for any effective-param drift.
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
        {"clientInfo": {"name": "agent-run", "version": "1"}},
        timeout_seconds=remaining(),
    )
    transport.notify("initialized")
    roots = tuple(state["roots"])
    writable_roots = tuple(state["writable_roots"])
    sandbox = state.get("sandbox", state["sandbox_mode"])
    network_access = False
    if isinstance(sandbox, Mapping) and len(sandbox) == 1:
        _, sandbox_params = next(iter(sandbox.items()))
        network_access = (
            isinstance(sandbox_params, Mapping)
            and sandbox_params.get("networkAccess") is True
        )
    thread = transport.request(
        "thread/start",
        {
            "cwd": str(plan.cwd),
            "model": state["model"],
            "effort": state.get("effort"),
            "sandbox": sandbox,
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
        network_access=network_access,
    )
    verify_effective_params(expected, thread)
    thread_id = _thread_id_echo(thread)
    if not isinstance(thread_id, str) or not thread_id:
        raise VerificationError("codex thread/start did not return a threadId")
    sink.session(thread_id)
    turn_ack = transport.request(
        "turn/start",
        {"threadId": thread_id, "input": [{"type": "text", "text": plan.initial_input}]},
        timeout_seconds=remaining(),
    )
    turn = turn_ack.get("turn")
    turn_id = turn.get("id") if isinstance(turn, Mapping) else None
    if not isinstance(turn_id, str) or not turn_id:
        raise VerificationError("codex turn/start did not return a turn id")
    return CodexAppServerSession(
        transport, sink, thread_id, turn_id=turn_id, answer_path=plan.answer_path
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
