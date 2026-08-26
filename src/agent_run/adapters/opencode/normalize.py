"""Pure OpenCode model, transcript, and status normalization.

Every shape here is the flat beta-18286 v2 contract proven live against the
service's own ``/openapi.json``: a session's ``model`` is ``Model.Ref``
(``providerID`` + ``id``, not ``modelID``); ``/api/session/active`` reports only
``{sessionID: {"type": "running"}}`` for a session currently executing a turn,
and omits any session that is not; the terminal ``outcome`` (succeeded, failed,
interrupted) lives on the session record itself, not on that active map; and a
``Session.Message.Info`` is a flat, ``type``-discriminated object -- no
``info``/``parts`` wrapper -- with a direct ``text`` field for user/system
messages and a ``content`` parts array for assistant messages.
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


def normalize_outcome(
    info: Mapping[str, object],
    payload: Mapping[str, object] | Sequence[object] = (),
    *,
    runtime_session_id: str | None = None,
) -> Outcome:
    """Map a session record's ``outcome`` to exactly one terminal Outcome.

    The session record carries no error detail; a non-``succeeded`` outcome's
    detail, when the provider reported one, is read off the last assistant
    message's structured error in the already-fetched transcript ``payload``.
    """

    if not isinstance(info, Mapping):
        raise ValidationError("opencode session info must be a mapping")
    raw = info.get("outcome")
    if not isinstance(raw, str) or raw not in _OUTCOME_STATUS:
        raise ValidationError(f"opencode session outcome is not terminal: {raw!r}")
    status = _OUTCOME_STATUS[raw]
    kind, text = (None, None) if status is AgentStatus.SUCCEEDED else _last_error(payload)
    return Outcome(
        status=status,
        failure_kind=kind,
        failure_text=text,
        runtime_session_id=runtime_session_id,
    )


def _last_error(
    payload: Mapping[str, object] | Sequence[object],
) -> tuple[str | None, str | None]:
    """The last assistant message's ``Session.StructuredError``, if any."""

    kind = text = None
    for item in _messages(payload):
        error = item.get("error") if item.get("type") == "assistant" else None
        if isinstance(error, Mapping):
            name = error.get("type")
            message = error.get("message")
            kind = name if isinstance(name, str) else "opencode_error"
            text = message if isinstance(message, str) else None
    return kind, text


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
