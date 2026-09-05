"""Decoder for the Claude Code ``stream-json`` output protocol.

Each line on the child's stdout is one JSON object. This module turns that
byte stream into domain ``Message`` values plus terminal session/cost/usage
metadata, without ever forwarding a value that looks like a credential.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from types import MappingProxyType
from typing import Iterable, Mapping

from ...domain import Message, MessageRole


_SECRET_KEY = re.compile(r"(key|token|secret|password|credential)", re.IGNORECASE)


def is_secret_env_name(name: str) -> bool:
    """Report whether an environment variable name is credential-shaped.

    Applies the same key-name test :func:`sanitize_line` uses structurally, so
    an adapter registering the variables it actually injected redacts exactly
    the ones the JSON pass would already redact.

    :param name: Environment variable name, e.g. ``ANTHROPIC_AUTH_TOKEN``.
    :returns: ``True`` when the name contains ``key``, ``token``, ``secret``,
        ``password``, or ``credential`` in any case; ``False`` otherwise, so a
        public companion value such as ``ANTHROPIC_BASE_URL`` is not registered
        as a literal secret and does not blank out endpoint URLs in the log.
    """

    return _SECRET_KEY.search(name) is not None


def _redact(value: object) -> object:
    """Drop string values whose key names look like a credential, recursively.

    Numeric fields (token counts, costs) are never redacted even when their
    key contains "token"; only string-valued fields can hold a live secret.
    """

    if isinstance(value, Mapping):
        result = {}
        for key, item in value.items():
            if isinstance(item, str) and _SECRET_KEY.search(str(key)):
                result[key] = "<redacted>"
            else:
                result[key] = _redact(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_redact(item) for item in value]
    return value


def _safe_event_data(payload: Mapping[str, object]) -> Mapping[str, object]:
    # A plain dict, not MappingProxyType: this return value is nested as a
    # field value inside other event payloads (see ``_terminal_metadata``),
    # and only the outer mapping gets unwrapped before JSON encoding.
    return dict(_redact(payload))  # type: ignore[arg-type]


def sanitize_line(raw_line: str, literal_secrets: Iterable[str]) -> str:
    """Strip known live secret values and secret-shaped keys from one raw line.

    Runs before the line ever reaches the decoder or the on-disk runtime log.
    Literal substitution covers text the child might echo verbatim (including
    a malformed, non-JSON line); the structural key-based pass then covers a
    well-formed JSON object whose fields are named like a credential. Values
    under keys that do not look like a credential, such as message text
    blocks, are left untouched.
    """

    text = raw_line
    for secret in literal_secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    stripped = text.strip()
    if not stripped:
        return text
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        return text
    if not isinstance(payload, dict):
        return text
    return json.dumps(_redact(payload), sort_keys=True)


def _clean_session_id(value: object) -> str | None:
    if isinstance(value, str) and value.strip():
        return value
    return None


def _text_of(block: Mapping[str, object]) -> str | None:
    text = block.get("text")
    return text if isinstance(text, str) and text.strip() else None


def _stringify_content(content: object) -> str | None:
    if isinstance(content, str):
        return content if content.strip() else None
    if content is None:
        return None
    try:
        rendered = json.dumps(_redact(content), sort_keys=True)
    except (TypeError, ValueError):
        rendered = str(content)
    return rendered if rendered.strip() and rendered != "null" else None


def _assistant_messages(payload: Mapping[str, object], at: float) -> tuple[Message, ...]:
    message = payload.get("message")
    if not isinstance(message, Mapping):
        return ()
    content = message.get("content")
    if not isinstance(content, list):
        return ()
    messages: list[Message] = []
    for block in content:
        if not isinstance(block, Mapping):
            continue
        block_type = block.get("type")
        if block_type == "text":
            text = _text_of(block)
            if text is not None:
                messages.append(Message(at=at, role=MessageRole.ASSISTANT, content=text))
        elif block_type == "tool_use":
            name = block.get("name")
            rendered = _stringify_content(block.get("input", {}))
            messages.append(
                Message(
                    at=at,
                    role=MessageRole.TOOL_CALL,
                    content=rendered or "{}",
                    name=name if isinstance(name, str) and name else None,
                )
            )
    return tuple(messages)


def _tool_result_messages(payload: Mapping[str, object], at: float) -> tuple[Message, ...]:
    message = payload.get("message")
    if not isinstance(message, Mapping):
        return ()
    content = message.get("content")
    if not isinstance(content, list):
        return ()
    messages: list[Message] = []
    for block in content:
        if not isinstance(block, Mapping) or block.get("type") != "tool_result":
            continue
        rendered = _stringify_content(block.get("content"))
        if rendered is None:
            continue
        tool_use_id = block.get("tool_use_id")
        messages.append(
            Message(
                at=at,
                role=MessageRole.TOOL_RESULT,
                content=rendered,
                name=tool_use_id if isinstance(tool_use_id, str) and tool_use_id else None,
            )
        )
    return tuple(messages)


@dataclass(frozen=True)
class StreamMetadata:
    """Trusted terminal facts extracted from a Claude ``result`` line."""

    runtime_session_id: str | None
    subtype: str | None
    is_error: bool
    result_text: str | None
    duration_ms: int | None
    num_turns: int | None
    total_cost_usd: float | None
    usage: Mapping[str, object]


def _terminal_metadata(payload: Mapping[str, object], session_id: str | None) -> StreamMetadata:
    """Extract the trusted terminal facts from one decoded ``result`` payload.

    Every field is an untrusted external value, so each is bound once and
    accepted only when it already has the declared type; anything else becomes
    ``None`` (or ``{}`` for ``usage``) rather than being coerced. ``is_error``
    is the sole deliberately loose field: any truthy value means failure.

    :param payload: Decoded ``result`` JSON object from the child.
    :param session_id: Session id observed on this line or carried by the
        decoder; ``None`` when the child never reported one.
    :returns: The populated :class:`StreamMetadata`; never raises on a
        malformed payload.
    """

    usage = payload.get("usage")
    result_text = payload.get("result")
    subtype = payload.get("subtype")
    duration_ms = payload.get("duration_ms")
    num_turns = payload.get("num_turns")
    total_cost_usd = payload.get("total_cost_usd")
    return StreamMetadata(
        runtime_session_id=session_id,
        subtype=subtype if isinstance(subtype, str) else None,
        is_error=bool(payload.get("is_error", False)),
        result_text=result_text if isinstance(result_text, str) and result_text.strip() else None,
        duration_ms=duration_ms if isinstance(duration_ms, int) and not isinstance(duration_ms, bool) else None,
        num_turns=num_turns if isinstance(num_turns, int) and not isinstance(num_turns, bool) else None,
        total_cost_usd=(
            float(total_cost_usd)
            if isinstance(total_cost_usd, (int, float)) and not isinstance(total_cost_usd, bool)
            else None
        ),
        usage=_safe_event_data(usage) if isinstance(usage, Mapping) else {},
    )


_AUTH_MARKERS = (
    "failed to authenticate",
    "oauth access token has expired",
    "oauth token has expired",
    "authentication_error",
    "invalid api key",
    "invalid bearer token",
    "please run /login",
)

#: Subtypes the engine reports on its happy path. The CLI labels an
#: engine-level error line ``subtype: "success"`` while setting
#: ``is_error: true`` (observed live on an expired OAuth token), so the
#: subtype alone can never be trusted to name a failure.
_NON_FAILURE_SUBTYPES = frozenset({"success", "", "none"})


def classify_failure(metadata: StreamMetadata) -> str:
    """Name the failure behind a terminal line that did not succeed.

    Never returns a success-shaped label: a ``result`` line that carries an
    engine error while still calling itself ``success`` is classified from
    what it actually said, and anything left unexplained becomes the honest
    generic ``engine_error``.
    """

    text = (metadata.result_text or "").casefold()
    if any(marker in text for marker in _AUTH_MARKERS):
        return "auth_failed"
    subtype = (metadata.subtype or "").strip()
    if subtype.casefold() in _NON_FAILURE_SUBTYPES:
        return "engine_error"
    return subtype


def terminal_event_data(metadata: StreamMetadata) -> Mapping[str, object]:
    """Bounded terminal metadata for a ``runtime_result`` event.

    Excludes ``result_text`` and ``runtime_session_id`` so the event never
    duplicates the answer content or the session-scoped sink call.
    """

    return MappingProxyType(
        {
            "subtype": metadata.subtype,
            "is_error": metadata.is_error,
            "duration_ms": metadata.duration_ms,
            "num_turns": metadata.num_turns,
            "total_cost_usd": metadata.total_cost_usd,
            "usage": metadata.usage,
        }
    )


@dataclass(frozen=True)
class FeedResult:
    """What one raw stdout line produced."""

    messages: tuple[Message, ...] = ()
    session_id: str | None = None
    event: tuple[str, Mapping[str, object]] | None = None
    terminal: StreamMetadata | None = None
    warning: str | None = None


class StreamDecoder:
    """Stateful decoder for one runtime session's stdout stream.

    Feed raw lines in order. Malformed or unrecognized lines never raise;
    they surface as a ``warning`` on the returned ``FeedResult`` so the
    caller can record a diagnostic and keep decoding subsequent lines.
    """

    def __init__(self) -> None:
        self._session_id: str | None = None
        self._terminal: StreamMetadata | None = None
        self._saw_assistant_text = False
        self._diagnostic_count = 0

    @property
    def session_id(self) -> str | None:
        return self._session_id

    @property
    def terminal(self) -> StreamMetadata | None:
        return self._terminal

    @property
    def diagnostic_count(self) -> int:
        return self._diagnostic_count

    def feed(self, raw_line: str, *, at: float) -> FeedResult:
        text = raw_line.strip()
        if not text:
            return FeedResult()
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            self._diagnostic_count += 1
            return FeedResult(warning="malformed_json_line")
        if not isinstance(payload, dict):
            self._diagnostic_count += 1
            return FeedResult(warning="malformed_json_line")
        kind = payload.get("type")
        if not isinstance(kind, str):
            self._diagnostic_count += 1
            return FeedResult(warning="missing_type_field")

        session_id = _clean_session_id(payload.get("session_id"))
        if session_id is not None:
            self._session_id = session_id

        if kind == "system":
            return FeedResult(session_id=session_id, event=("system", _safe_event_data(payload)))
        if kind == "assistant":
            messages = _assistant_messages(payload, at)
            if any(message.role == MessageRole.ASSISTANT for message in messages):
                self._saw_assistant_text = True
            return FeedResult(messages=messages, session_id=session_id)
        if kind == "user":
            return FeedResult(messages=_tool_result_messages(payload, at), session_id=session_id)
        if kind == "result":
            if self._terminal is not None:
                self._diagnostic_count += 1
                return FeedResult(session_id=session_id, warning="duplicate_terminal_line")
            metadata = _terminal_metadata(payload, session_id or self._session_id)
            self._terminal = metadata
            return FeedResult(session_id=session_id, terminal=metadata)

        self._diagnostic_count += 1
        return FeedResult(session_id=session_id, warning=f"unknown_type:{kind}")

    def finalize(self) -> StreamMetadata:
        """Classify the run when the child exited without a clean terminal line.

        Distinguishes an engine that produced no answer at all from one whose
        answer was cut off mid-write, per the no-answer-vs-cut-off invariant.
        """

        if self._terminal is not None:
            return self._terminal
        subtype = "cut_off" if self._saw_assistant_text else "no_answer"
        return StreamMetadata(
            runtime_session_id=self._session_id,
            subtype=subtype,
            is_error=True,
            result_text=None,
            duration_ms=None,
            num_turns=None,
            total_cost_usd=None,
            usage={},
        )
