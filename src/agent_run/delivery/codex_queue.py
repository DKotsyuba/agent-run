"""Codex queue chat transport: fixed trusted text into an existing session."""

from __future__ import annotations

import math
from typing import Callable

from ..domain import OrchestratorRef
from ..errors import ValidationError
from .base import (
    TRANSPORT_API_VERSION,
    AmbiguousDeliveryError,
    ChatTransportConfig,
    CompletionNotice,
    DeliveryError,
    DeliveryReceipt,
)


TRANSPORT_NAME = "codex_queue"

#: Enqueue `text` into the Codex session named by `external_session_id` and
#: return the remote message id when the queue reports one. Raise
#: `TimeoutError` when acceptance is unknown, `LookupError` when the session is
#: gone, and `OSError` when the queue is unreachable.
QueueSender = Callable[[str, str], "str | None"]


class CodexQueueTransport:
    """Deliver a completion notice by queueing one message on a live session.

    The transport has no session-open path by construction. A missing session
    is a hard failure: a completion wake never starts a replacement session and
    never starts a replacement agent.
    """

    name = TRANSPORT_NAME
    api_version = TRANSPORT_API_VERSION

    def __init__(self, sender: QueueSender) -> None:
        if not callable(sender):
            raise ValidationError("codex queue sender must be callable")
        self._sender = sender

    def validate(self, config: ChatTransportConfig) -> None:
        if not isinstance(config, ChatTransportConfig):
            raise ValidationError("delivery config must be a DeliveryConfig")
        for name in ("retry_base_seconds", "retry_cap_seconds"):
            value = getattr(config, name)
            if value <= 0 or not math.isfinite(value):
                raise ValidationError(f"delivery.{name} must be positive and finite")
        if config.max_attempts < 0:
            raise ValidationError("delivery.max_attempts must not be negative")

    def send(
        self, target: OrchestratorRef, notice: CompletionNotice
    ) -> DeliveryReceipt:
        if not isinstance(target, OrchestratorRef):
            raise ValidationError("target must be an OrchestratorRef")
        if not isinstance(notice, CompletionNotice):
            raise ValidationError("notice must be a CompletionNotice")
        if target.transport != self.name:
            raise DeliveryError(
                f"{self.name} cannot deliver to transport {target.transport!r}"
            )
        try:
            remote_message_id = self._sender(
                target.external_session_id, notice.render()
            )
        except TimeoutError as error:
            # The queue may have accepted the message. At-least-once: report
            # ambiguity so the dispatcher records it and retries.
            raise AmbiguousDeliveryError(
                f"codex queue acceptance is unknown for {notice.notification_id}"
            ) from error
        except LookupError as error:
            raise DeliveryError(
                "codex queue session is gone; agent-run never opens a replacement"
            ) from error
        except OSError as error:
            raise DeliveryError(f"codex queue is unreachable: {error}") from error
        if remote_message_id is not None and not isinstance(remote_message_id, str):
            raise DeliveryError("codex queue returned a non-string message id")
        return DeliveryReceipt(remote_message_id=remote_message_id, ambiguous=False)
