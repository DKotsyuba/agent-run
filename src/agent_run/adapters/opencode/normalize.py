"""Pure OpenCode model, transcript, and status normalization."""

from __future__ import annotations

from types import MappingProxyType
from typing import Mapping, Sequence

from ...domain import AgentStatus, Message, MessageRole, Outcome
from ...errors import ValidationError
from ..base import ModelInfo


PRIMARY_AGENT = "agent-run"
_TERMINAL_STATES: Mapping[str, AgentStatus] = MappingProxyType(
    {
        "completed": AgentStatus.SUCCEEDED,
        "idle": AgentStatus.SUCCEEDED,
        "aborted": AgentStatus.CANCELLED,
        "cancelled": AgentStatus.CANCELLED,
        "error": AgentStatus.FAILED,
        "failed": AgentStatus.FAILED,
        "timeout": AgentStatus.TIMED_OUT,
        "timed_out": AgentStatus.TIMED_OUT,
    }
)
_ACTIVE_STATES = frozenset(
    {"running", "busy", "pending", "queued", "streaming", "retry", "retrying"}
)
_ROLES: Mapping[str, MessageRole] = MappingProxyType(
    {
        "user": MessageRole.USER,
        "assistant": MessageRole.ASSISTANT,
        "system": MessageRole.SYSTEM,
    }
)


def split_model(value: object) -> tuple[str, str]:
    """Split a canonical ``providerID/modelID`` identifier, or refuse it."""

    if not isinstance(value, str) or "/" not in value:
        raise ValidationError(
            f"opencode model must be canonical 'providerID/modelID', not {value!r}"
        )
    provider, model = value.split("/", 1)
    if provider != "omniroute" or not model.startswith("opencode/") or not model[9:].strip():
        raise ValidationError(
            f"opencode model must be canonical 'providerID/modelID', not {value!r}"
        )
    return provider, model


def model_reference(value: str) -> Mapping[str, str]:
    """The v2 prompt body's model shape."""

    provider, model = split_model(value)
    return {"providerID": provider, "modelID": model}


def normalize_models(
    payload: Mapping[str, object], allowed: Sequence[str]
) -> tuple[ModelInfo, ...]:
    """Intersect the reported roster with the configured allowlist, in order."""

    if not isinstance(payload, Mapping):
        raise ValidationError("opencode model roster must be a mapping")
    reported: dict[str, str] = {}
    for provider in _sequence(payload.get("data", payload.get("providers"))):
        if not isinstance(provider, Mapping):
            raise ValidationError("opencode provider entry must be a mapping")
        provider_id = provider.get("id")
        models = provider.get("models")
        items = models.values() if isinstance(models, Mapping) else _sequence(models)
        for model in items:
            if not isinstance(model, Mapping):
                raise ValidationError("opencode model entry must be a mapping")
            identifier = model.get("id")
            if not isinstance(identifier, str) or not identifier.strip():
                raise ValidationError("opencode model id must be a nonblank string")
            if isinstance(provider_id, str) and provider_id.strip():
                identifier = f"{provider_id}/{identifier}"
            name = model.get("name")
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


def _info(item: Mapping[str, object]) -> Mapping[str, object]:
    """A v2 message is ``{info, parts}``; the metadata lives in ``info``."""

    info = item.get("info")
    if isinstance(info, Mapping):
        return info
    if info is not None:
        raise ValidationError("opencode message info must be a mapping")
    return item


def _text_parts(item: Mapping[str, object]) -> str:
    chunks: list[str] = []
    for part in _sequence(item.get("parts")):
        if not isinstance(part, Mapping):
            raise ValidationError("opencode message part must be a mapping")
        if part.get("type") != "text":
            continue
        text = part.get("text")
        if isinstance(text, str) and text.strip():
            chunks.append(text.strip())
    return "\n\n".join(chunks)


def _at(item: Mapping[str, object]) -> float:
    info = _info(item)
    time_value = info.get("time")
    raw = time_value.get("created") if isinstance(time_value, Mapping) else info.get("created")
    if isinstance(raw, bool) or not isinstance(raw, (int, float)) or raw < 0:
        return 0.0
    return float(raw)


def _role(item: Mapping[str, object]) -> MessageRole:
    raw = _info(item).get("role")
    try:
        return _ROLES[raw]  # type: ignore[index]
    except (KeyError, TypeError) as error:
        raise ValidationError(f"unknown opencode message role: {raw!r}") from error


def _agent(item: Mapping[str, object]) -> str:
    value = _info(item).get("agent")
    return value if isinstance(value, str) and value.strip() else PRIMARY_AGENT


def normalize_transcript(
    payload: Mapping[str, object] | Sequence[object], *, raw_ref: str | None = None
) -> tuple[Message, ...]:
    """Turn a captured message page into domain messages, dropping empty text."""

    messages: list[Message] = []
    for item in _messages(payload):
        content = _text_parts(item)
        if not content:
            continue
        messages.append(
            Message(_at(item), _role(item), content, name=_agent(item), raw_ref=raw_ref)
        )
    return tuple(messages)


def _messages(
    payload: Mapping[str, object] | Sequence[object],
) -> tuple[Mapping[str, object], ...]:
    items = payload.get("messages") if isinstance(payload, Mapping) else payload
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
        for text in (_text_parts(item),)
        if text
    )


def normalize_outcome(
    state: Mapping[str, object], *, runtime_session_id: str | None = None
) -> Outcome:
    """Map a reported session state to exactly one terminal outcome."""

    if not isinstance(state, Mapping):
        raise ValidationError("opencode session state must be a mapping")
    raw = state.get("state", state.get("status"))
    if not isinstance(raw, str) or raw not in _TERMINAL_STATES:
        raise ValidationError(f"opencode session state is not terminal: {raw!r}")
    status = _TERMINAL_STATES[raw]
    error = state.get("error")
    kind = None
    text = None
    if isinstance(error, Mapping):
        name = error.get("name")
        message = error.get("message")
        kind = name if isinstance(name, str) else "opencode_error"
        text = message if isinstance(message, str) else None
    elif isinstance(error, str) and error.strip():
        kind = "opencode_error"
        text = error
    if kind is not None and status is AgentStatus.SUCCEEDED:
        status = AgentStatus.FAILED
    return Outcome(
        status=status,
        failure_kind=kind if status is not AgentStatus.SUCCEEDED else None,
        failure_text=text if status is not AgentStatus.SUCCEEDED else None,
        runtime_session_id=runtime_session_id,
    )


def session_state(status: Mapping[str, object]) -> str | None:
    raw = status.get("state", status.get("status")) if isinstance(status, Mapping) else None
    return raw if isinstance(raw, str) else None


def is_settled(state: Mapping[str, object]) -> bool:
    raw = session_state(state)
    return raw is not None and raw not in _ACTIVE_STATES and raw in _TERMINAL_STATES


def is_working(state: Mapping[str, object]) -> bool:
    return session_state(state) in _ACTIVE_STATES
