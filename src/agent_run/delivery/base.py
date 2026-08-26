"""Frozen chat delivery boundary: trusted completion notice and transport API."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from ..config import DeliveryConfig
from ..domain import (
    TERMINAL,
    AgentId,
    AgentStatus,
    OrchestratorRef,
    validate_agent_id,
)
from ..errors import AgentRunError, ValidationError


TRANSPORT_API_VERSION = 1
NOTICE_VERSION = 1
_MAX_ID_LENGTH = 512

#: Delivery has no config of its own beyond the `[delivery]` table.
ChatTransportConfig = DeliveryConfig


class DeliveryError(AgentRunError):
    """A transport refused or failed to deliver a completion notice."""


class AmbiguousDeliveryError(DeliveryError):
    """A transport neither confirmed nor refused; the notice may have arrived.

    Delivery is at-least-once: the dispatcher retries and the duplicate wake
    references the same agent, so it can never launch a replacement agent.
    """


def _trusted_id(name: str, value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{name} must be a nonblank string")
    if len(value) > _MAX_ID_LENGTH:
        raise ValidationError(f"{name} must be at most {_MAX_ID_LENGTH} characters")
    return value


@dataclass(frozen=True, slots=True)
class CompletionNotice:
    """The only payload a transport may send.

    It carries lifecycle facts alone. Task text, answer text, runtime error
    prose, and tool output never enter this type, so no untrusted string can
    reach an orchestration session through delivery.
    """

    notification_id: str
    agent_id: AgentId
    status: AgentStatus
    version: int = NOTICE_VERSION

    def __post_init__(self) -> None:
        _trusted_id("notification_id", self.notification_id)
        if isinstance(self.version, bool) or not isinstance(self.version, int):
            raise ValidationError(f"notice version must be {NOTICE_VERSION}")
        if self.version != NOTICE_VERSION:
            raise ValidationError(f"notice version must be {NOTICE_VERSION}")
        if not isinstance(self.status, AgentStatus) or self.status not in TERMINAL:
            raise ValidationError("completion notice status must be terminal")
        object.__setattr__(self, "agent_id", validate_agent_id(self.agent_id))

    def payload(self) -> dict[str, object]:
        return {
            "version": self.version,
            "notification_id": self.notification_id,
            "agent_id": str(self.agent_id),
            "status": self.status.value,
        }

    def render(self) -> str:
        """Render the fixed chat message; every part is derived from this notice."""

        return (
            f"agent-run: agent {self.agent_id} finished with status "
            f"{self.status.value}. Call summary({self.agent_id}) or "
            f"transcript({self.agent_id}) for details. Do not start a "
            f"replacement agent for this notification. "
            f"[notification {self.notification_id} v{self.version}]"
        )


@dataclass(frozen=True, slots=True)
class DeliveryReceipt:
    remote_message_id: str | None = None
    ambiguous: bool = False

    def __post_init__(self) -> None:
        if self.remote_message_id is not None:
            _trusted_id("remote_message_id", self.remote_message_id)
        if not isinstance(self.ambiguous, bool):
            raise ValidationError("ambiguous must be a bool")


class ChatTransport(Protocol):
    name: str
    api_version: int

    def validate(self, config: ChatTransportConfig) -> None: ...

    def send(
        self, target: OrchestratorRef, notice: CompletionNotice
    ) -> DeliveryReceipt: ...
