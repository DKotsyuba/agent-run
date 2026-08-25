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
from typing import Mapping

from ...domain import Message, MessageRole


_SECRET_KEY = re.compile(r"(key|token|secret|password|credential)", re.IGNORECASE)


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
    return MappingProxyType(dict(_redact(payload)))  # type: ignore[arg-type]


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
    usage = payload.get("usage")
    result_text = payload.get("result")
    return StreamMetadata(
        runtime_session_id=session_id,
        subtype=payload.get("subtype") if isinstance(payload.get("subtype"), str) else None,
        is_error=bool(payload.get("is_error", False)),
        result_text=result_text if isinstance(result_text, str) and result_text.strip() else None,
        duration_ms=payload.get("duration_ms") if isinstance(payload.get("duration_ms"), int) else None,
        num_turns=payload.get("num_turns") if isinstance(payload.get("num_turns"), int) else None,
        total_cost_usd=(
            float(payload["total_cost_usd"])
            if isinstance(payload.get("total_cost_usd"), (int, float))
            and not isinstance(payload.get("total_cost_usd"), bool)
            else None
        ),
        usage=_safe_event_data(usage) if isinstance(usage, Mapping) else MappingProxyType({}),
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
            usage=MappingProxyType({}),
        )
