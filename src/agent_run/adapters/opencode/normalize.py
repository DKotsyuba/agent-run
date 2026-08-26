"""Pure OpenCode model, transcript, and status normalization.

The flat contract proven live against both the beta-18286 service's own
``/openapi.json`` and the pinned v1 1.18.18 service's ``/doc``: a session's
``model`` is ``Model.Ref`` (``providerID`` + ``id``, not ``modelID``);
``/api/session/active`` reports only ``{sessionID: {"type": "running"}}`` for a
session currently executing a turn, and omits any session that is not; a
``Session.Message.Info`` is a flat, ``type``-discriminated object -- no
``info``/``parts`` wrapper -- with a direct ``text`` field for user/system
messages and a ``content`` parts array for assistant messages, ordered
newest-first by ``GET .../message``. The terminal ``outcome`` (succeeded,
failed, interrupted) lives on the session record on beta, but is optional
there too (permanently absent, not delayed, when a turn ends through an
aborted tool call) and is never present at all on v1 1.18.18's
``SessionV2Info`` -- proven live: a real v1 session that has left
``/api/session/active`` carries no ``outcome`` key whatsoever. Either way that
ending is inferred from the most recent message instead.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping, Sequence

from ...domain import AgentStatus, Message, MessageRole, Outcome
from ...errors import ValidationError
from ..base import ModelInfo


PRIMARY_AGENT = "agent-run"

#: ``Session.Info.outcome`` is exactly these three values once a session is
#: no longer running.
_OUTCOME_STATUS: Mapping[str, AgentStatus] = MappingProxyType(
    {
        "succeeded": AgentStatus.SUCCEEDED,
        "failed": AgentStatus.FAILED,
        "interrupted": AgentStatus.CANCELLED,
    }
)
#: ``Session.Message.Info`` types that carry a conversational role.
_ROLES: Mapping[str, MessageRole] = MappingProxyType(
    {
        "user": MessageRole.USER,
        "assistant": MessageRole.ASSISTANT,
        "system": MessageRole.SYSTEM,
    }
)
#: The union's other known types: session events with no message role. Kept
#: distinct from a genuinely unrecognized type, which still fails closed.
_EVENT_TYPES = frozenset(
    {
        "agent_selected",
        "model_selected",
        "location_switched",
        "synthetic",
        "skill",
        "shell",
        "compaction",
    }
)


def split_model(value: object) -> tuple[str, str]:
    """Split a canonical ``providerID/modelID`` identifier, or refuse it."""

    if not isinstance(value, str) or "/" not in value:
        raise ValidationError(
            f"opencode model must be canonical 'providerID/modelID', not {value!r}"
        )
    provider, model = value.split("/", 1)
    if provider != "omniroute" or not model.strip():
        raise ValidationError(
            f"opencode model must be canonical 'providerID/modelID', not {value!r}"
        )
    return provider, model


def model_reference(value: str) -> Mapping[str, str]:
    """The v2 ``Model.Ref`` shape: ``{"providerID": ..., "id": ...}``."""

    provider, model = split_model(value)
    return {"providerID": provider, "id": model}


def normalize_models(
    payload: Mapping[str, object], allowed: Sequence[str]
) -> tuple[ModelInfo, ...]:
    """Intersect the reported ``/api/model`` roster with the configured allowlist.

    Each entry is flat: ``providerID`` + ``id`` form the canonical
    ``providerID/id`` identifier. An entry outside the allowlist is the normal
    case for a builtin model (e.g. provider ``opencode``) and is silently
    dropped; a malformed entry fails closed. An entry that cannot actually be
    started right now (``enabled`` is ``False``, or ``status`` is not
    ``"active"``) is not reported, even if it is on the allowlist.
    """

    if not isinstance(payload, Mapping):
        raise ValidationError("opencode model roster must be a mapping")
    reported: dict[str, str] = {}
    for entry in _sequence(payload.get("data")):
        if not isinstance(entry, Mapping):
            raise ValidationError("opencode model entry must be a mapping")
        provider_id = entry.get("providerID")
        model_id = entry.get("id")
        if not isinstance(provider_id, str) or not provider_id.strip():
            raise ValidationError("opencode model entry providerID must be a nonblank string")
        if not isinstance(model_id, str) or not model_id.strip():
            raise ValidationError("opencode model entry id must be a nonblank string")
        if entry.get("enabled") is False or entry.get("status") != "active":
            continue
        name = entry.get("name")
        identifier = f"{provider_id}/{model_id}"
        reported.setdefault(identifier, name if isinstance(name, str) else identifier)
    return tuple(
        ModelInfo(identifier, reported[identifier])
        for identifier in allowed
        if identifier in reported
    )


def _sequence(value: object) -> tuple[object, ...]:
    if value is None:
        return ()
    if isinstance(value, (list, tuple)):
        return tuple(value)
    raise ValidationError("opencode payload expected an array")


def _text(item: Mapping[str, object]) -> str:
    """User/system messages carry ``text`` directly; assistant text is the
    ``text`` parts of its ``content`` array."""

    direct = item.get("text")
    if isinstance(direct, str):
        return direct.strip()
    chunks: list[str] = []
    for part in _sequence(item.get("content")):
        if not isinstance(part, Mapping):
            raise ValidationError("opencode message content item must be a mapping")
        if part.get("type") != "text":
            continue
        text = part.get("text")
        if isinstance(text, str) and text.strip():
            chunks.append(text.strip())
    return "\n\n".join(chunks)


def _at(item: Mapping[str, object]) -> float:
    time_value = item.get("time")
    raw = time_value.get("created") if isinstance(time_value, Mapping) else None
    if isinstance(raw, bool) or not isinstance(raw, (int, float)) or raw < 0:
        return 0.0
    return float(raw)


def _role(item: Mapping[str, object]) -> MessageRole | None:
    """The v2 conversational role, or None for a role-less session event."""

    raw = item.get("type")
    if raw in _ROLES:
        return _ROLES[raw]  # type: ignore[index]
    if raw in _EVENT_TYPES:
        return None
    raise ValidationError(f"unknown opencode message type: {raw!r}")


def _agent(item: Mapping[str, object]) -> str:
    value = item.get("agent")
    return value if isinstance(value, str) and value.strip() else PRIMARY_AGENT


def normalize_transcript(
    payload: Mapping[str, object] | Sequence[object], *, raw_ref: str | None = None
) -> tuple[Message, ...]:
    """Turn a captured message page into domain messages.

    A role-less session event (model/agent switch, a synthetic retry marker,
    a tool-only turn, ...) is dropped, not refused; only a genuinely
    unrecognized message type fails closed.
    """

    messages: list[Message] = []
    for item in _messages(payload):
        role = _role(item)
        if role is None:
            continue
        content = _text(item)
        if not content:
            continue
        messages.append(
            Message(_at(item), role, content, name=_agent(item), raw_ref=raw_ref)
        )
    return tuple(messages)


def _messages(
    payload: Mapping[str, object] | Sequence[object],
) -> tuple[Mapping[str, object], ...]:
    """``GET .../message`` replies ``{"data": [...], "cursor": {...}}``."""

    items = payload.get("data") if isinstance(payload, Mapping) else payload
    result = []
    for item in _sequence(items):
        if not isinstance(item, Mapping):
            raise ValidationError("opencode message must be a mapping")
        result.append(item)
    return tuple(result)


def extract_answer(
    payload: Mapping[str, object] | Sequence[object], *, agent: str = PRIMARY_AGENT
) -> str:
    """Every assistant text this agent produced in the session, in order.

    One session is one agent run, so nothing in it predates the task. Steering
    inserts a real user message mid-run and retries insert a synthetic one;
    neither starts a new answer, so text written before them is preserved. A
    sub-agent's output is never mistaken for the primary agent's answer.
    """

    return "\n\n".join(
        text
        for item in _messages(payload)
        if _role(item) is MessageRole.ASSISTANT and _agent(item) == agent
        for text in (_text(item),)
        if text
    )


def has_reported_error(
    payload: Mapping[str, object] | Sequence[object], *, agent: str = PRIMARY_AGENT
) -> bool:
    """Whether this agent's most recent message already carries a structured
    error -- a terminal shape with no text to extract (a provider failure, an
    aborted turn). A bare tool-call part with neither text nor a top-level
    error (proven live, T17B: v1 1.18.18 mid-tool-round) is not covered here
    and must not be mistaken for one. Scoped to ``agent`` for the same reason
    ``extract_answer`` is: a sub-agent's error must never settle the primary
    session's ``wait()``.
    """

    for item in _messages(payload):
        if _role(item) is MessageRole.ASSISTANT and _agent(item) == agent:
            return isinstance(item.get("error"), Mapping)
    return False


def normalize_outcome(
    info: Mapping[str, object],
    payload: Mapping[str, object] | Sequence[object] = (),
    *,
    runtime_session_id: str | None = None,
    cancelled: bool = False,
) -> Outcome:
    """Map a session record's ``outcome`` to exactly one terminal Outcome.

    ``outcome`` is an optional ``Session.Info`` field, not a required one:
    beta reports it once a turn finishes cleanly but omits it when a turn
    instead ends through an aborted tool call, and v1 1.18.18 never reports it
    at all. The session record carries no error detail either way; a
    failure's detail, when the provider reported one, is read off the last
    assistant message's structured error in the already-fetched transcript
    ``payload``. When ``outcome`` is absent, the terminal status itself is
    inferred from that same last message instead -- ``cancelled`` tells that
    inference the one thing no message shape can: this adapter's own
    ``OpenCodeRuntimeSession.cancel()`` already called ``/interrupt``
    synchronously, before ``wait()`` resumed polling, so an error-shaped
    message (or no settled message at all) after that call is a
    cancellation, never a raised "indeterminate" error. Ignored when
    ``outcome`` is present -- the server already told us.
    """

    if not isinstance(info, Mapping):
        raise ValidationError("opencode session info must be a mapping")
    raw = info.get("outcome")
    if isinstance(raw, str) and raw in _OUTCOME_STATUS:
        status = _OUTCOME_STATUS[raw]
        kind, text = (None, None) if status is AgentStatus.SUCCEEDED else _last_error(payload)
        return Outcome(
            status=status,
            failure_kind=kind,
            failure_text=text,
            runtime_session_id=runtime_session_id,
        )
    if raw is not None:
        raise ValidationError(f"opencode session outcome is not terminal: {raw!r}")
    status, kind, text = _infer_settled_outcome(payload, cancelled=cancelled)
    return Outcome(
        status=status,
        failure_kind=kind,
        failure_text=text,
        runtime_session_id=runtime_session_id,
    )


#: A named v1 error that, on the SSE session-error event stream, distinctly
#: means "this session's own /interrupt fired," not "the turn failed" --
#: documented in v1 1.18.18's own ``/doc`` OpenAPI for that stream's error
#: union. Checked here as a defensive extra, not the primary signal: three
#: live 401 captures through the persisted REST message endpoint (T041) all
#: came back with the flatter ``{"type": "unknown", "message": ...}`` shape,
#: never this named union member, so ``normalize_outcome``'s ``cancelled``
#: argument -- what the adapter already knows it did, not a guess from
#: message shape -- is what actually carries the interrupt signal for v1.
_ABORTED_ERROR = "MessageAbortedError"


def _error_detail(error: Mapping[str, object]) -> tuple[str, str | None]:
    """A structured error's kind and message.

    Beta names the discriminant ``type`` and carries the message directly;
    v1 names it ``name`` and nests the message under ``data`` instead
    (``{"name": "MessageAbortedError", "data": {"message": ...}}``, proven
    live via v1's ``/doc``). Both are accepted so one call site works for
    either service generation.
    """

    name = error.get("type")
    if not isinstance(name, str):
        name = error.get("name")
    kind = name if isinstance(name, str) else "opencode_error"
    message = error.get("message")
    if not isinstance(message, str):
        data = error.get("data")
        message = data.get("message") if isinstance(data, Mapping) else None
    text = message if isinstance(message, str) else None
    return kind, text


def _last_error(
    payload: Mapping[str, object] | Sequence[object],
) -> tuple[str | None, str | None]:
    """The last assistant message's ``Session.StructuredError``, if any."""

    kind = text = None
    for item in _messages(payload):
        error = item.get("error") if item.get("type") == "assistant" else None
        if isinstance(error, Mapping):
            kind, text = _error_detail(error)
    return kind, text


def _infer_settled_outcome(
    payload: Mapping[str, object] | Sequence[object], *, cancelled: bool = False
) -> tuple[AgentStatus, str | None, str | None]:
    """Derive success/failure/cancellation when the session record carries no
    ``outcome`` at all -- the only path on v1 1.18.18, and the fallback for a
    beta turn that ended through an aborted tool call.

    ``GET .../message`` orders ``data`` newest-first (proven live against
    both beta-18314 and v1 1.18.18), so the most recent message is always the
    first item. An assistant message with real text and no error is a
    success. An assistant message with a structured error is a cancellation
    when it names ``MessageAbortedError``, or when ``cancelled`` is set --
    that local signal matters because a v1 ``/interrupt`` fired shortly after
    ``prompt`` does not always leave an error-shaped message to check at all
    (proven live, T041: it can settle with no assistant message whatsoever) --
    and a failure otherwise. A session cancelled before any settled assistant
    message ever landed is a cancellation too, not a raised error:
    ``cancelled`` is a fact the caller already knows, never a guess, so it is
    never indeterminate. Absent that signal, no messages at all or a
    non-settled last message is genuinely indeterminate and still fails
    closed.
    """

    messages = _messages(payload)
    if not messages or messages[0].get("type") != "assistant":
        if cancelled:
            return AgentStatus.CANCELLED, None, None
        raise ValidationError("opencode session outcome is not terminal: None")
    latest = messages[0]
    error = latest.get("error")
    if isinstance(error, Mapping):
        kind, text = _error_detail(error)
        if cancelled or kind == _ABORTED_ERROR:
            return AgentStatus.CANCELLED, kind, text
        return AgentStatus.FAILED, kind, text
    if _text(latest):
        return AgentStatus.SUCCEEDED, None, None
    if cancelled:
        return AgentStatus.CANCELLED, None, None
    raise ValidationError("opencode session outcome is not terminal: None")


def session_state(status: Mapping[str, object]) -> str | None:
    """The ``/api/session/active`` entry's discriminant, or None when absent."""

    raw = status.get("type") if isinstance(status, Mapping) else None
    return raw if isinstance(raw, str) else None


def is_working(state: Mapping[str, object]) -> bool:
    return session_state(state) == "running"


def is_settled(state: Mapping[str, object]) -> bool:
    """A session absent from ``/active`` is not currently running.

    That is the only settle signal the endpoint offers: it cannot distinguish
    "not started yet" from "just finished", so ``wait()`` only trusts this
    once the session has been seen working, or the transcript already carries
    an answer.
    """

    return not is_working(state)
