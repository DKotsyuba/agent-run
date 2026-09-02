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
_MAX_EVIDENCE_TAIL_BYTES = 4096


def _is_bool(value: object) -> bool:
    """Return whether an untrusted payload value is exactly boolean."""

    return isinstance(value, bool)


#: Delivery has no config of its own beyond the `[delivery]` table.
ChatTransportConfig = DeliveryConfig


@dataclass(frozen=True, slots=True)
class DeliveryAttemptEvidence:
    """Validated, bounded, secret-free facts for one queue subprocess attempt.

    The value contains only executable provenance, argument shape, outcome
    classification, timing, bounded output tails, and size flags. Callers must
    redact command values and credentials before construction. Instances are
    immutable and serialize through :meth:`payload` for durable storage.
    """

    classifier: str
    executable: str
    argv_shape: tuple[str, ...]
    duration_ms: int
    returncode: int | None = None
    spawn_errno: int | None = None
    error_class: str | None = None
    stdout_tail: str = ""
    stderr_tail: str = ""
    stdout_bytes: int = 0
    stderr_bytes: int = 0
    stdout_truncated: bool = False
    stderr_truncated: bool = False
    message_id_present: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.classifier, str) or not self.classifier.strip():
            raise ValidationError("invalid delivery attempt evidence")
        if not isinstance(self.executable, str) or not self.executable.strip():
            raise ValidationError("invalid delivery attempt evidence")
        if (
            not isinstance(self.argv_shape, tuple)
            or not self.argv_shape
            or any(not isinstance(item, str) or not item for item in self.argv_shape)
        ):
            raise ValidationError("invalid delivery attempt evidence")
        if type(self.duration_ms) is not int or self.duration_ms < 0:
            raise ValidationError("invalid delivery attempt evidence")
        for value in (self.returncode, self.spawn_errno):
            if value is not None and type(value) is not int:
                raise ValidationError("invalid delivery attempt evidence")
        if self.returncode is not None and self.spawn_errno is not None:
            raise ValidationError("invalid delivery attempt evidence")
        if self.error_class is not None and (
            not isinstance(self.error_class, str) or not self.error_class.strip()
        ):
            raise ValidationError("invalid delivery attempt evidence")
        for tail in (self.stdout_tail, self.stderr_tail):
            if not isinstance(tail, str):
                raise ValidationError("invalid delivery attempt evidence")
            if len(tail.encode("utf-8")) > _MAX_EVIDENCE_TAIL_BYTES:
                raise ValidationError("delivery evidence tail exceeds 4096 bytes")
        for count in (self.stdout_bytes, self.stderr_bytes):
            if type(count) is not int or count < 0:
                raise ValidationError("invalid delivery attempt evidence")
        for flag in (
            self.stdout_truncated,
            self.stderr_truncated,
            self.message_id_present,
        ):
            if not _is_bool(flag):
                raise ValidationError("invalid delivery attempt evidence")

    def payload(self) -> dict[str, object]:
        """Return the complete stable JSON-safe shape without private inputs."""

        return {
            "classifier": self.classifier,
            "executable": self.executable,
            "argv_shape": list(self.argv_shape),
            "duration_ms": self.duration_ms,
            "returncode": self.returncode,
            "spawn_errno": self.spawn_errno,
            "error_class": self.error_class,
            "stdout_tail": self.stdout_tail,
            "stderr_tail": self.stderr_tail,
            "stdout_bytes": self.stdout_bytes,
            "stderr_bytes": self.stderr_bytes,
            "stdout_truncated": self.stdout_truncated,
            "stderr_truncated": self.stderr_truncated,
            "message_id_present": self.message_id_present,
        }

    @classmethod
    def from_payload(cls, value: object) -> DeliveryAttemptEvidence:
        """Validate and restore one exact payload read from durable state.

        ``value`` must be an object with the complete current evidence shape;
        missing or unknown fields, invalid types, and oversized tails raise
        :class:`ValidationError` instead of exposing corrupt state.
        """

        expected = {
            "classifier", "executable", "argv_shape", "duration_ms",
            "returncode", "spawn_errno", "error_class", "stdout_tail",
            "stderr_tail", "stdout_bytes", "stderr_bytes",
            "stdout_truncated", "stderr_truncated", "message_id_present",
        }
        if not isinstance(value, dict) or set(value) != expected:
            raise ValidationError("invalid stored delivery attempt evidence")
        argv_shape = value["argv_shape"]
        if not isinstance(argv_shape, list):
            raise ValidationError("invalid stored delivery attempt evidence")
        return cls(
            classifier=value["classifier"],
            executable=value["executable"],
            argv_shape=tuple(argv_shape),
            duration_ms=value["duration_ms"],
            returncode=value["returncode"],
            spawn_errno=value["spawn_errno"],
            error_class=value["error_class"],
            stdout_tail=value["stdout_tail"],
            stderr_tail=value["stderr_tail"],
            stdout_bytes=value["stdout_bytes"],
            stderr_bytes=value["stderr_bytes"],
            stdout_truncated=value["stdout_truncated"],
            stderr_truncated=value["stderr_truncated"],
            message_id_present=value["message_id_present"],
        )


def _is_evidence(value: object) -> bool:
    """Return whether an untrusted payload is typed delivery evidence."""

    return isinstance(value, DeliveryAttemptEvidence)



class DeliveryError(AgentRunError):
    """A transport refused or failed to deliver a completion notice safely."""

    def __init__(self, message: str, *, evidence: DeliveryAttemptEvidence | None = None) -> None:
        """Create a safe delivery error with optional bounded attempt evidence."""

        if evidence is not None and not _is_evidence(evidence):
            raise ValidationError("evidence must be DeliveryAttemptEvidence or None")
        super().__init__(message)
        self.evidence = evidence


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
    """Successful transport result and optional evidence for its exact attempt."""

    remote_message_id: str | None = None
    ambiguous: bool = False
    evidence: DeliveryAttemptEvidence | None = None

    def __post_init__(self) -> None:
        if self.remote_message_id is not None:
            _trusted_id("remote_message_id", self.remote_message_id)
        if not isinstance(self.ambiguous, bool):
            raise ValidationError("ambiguous must be a bool")
        if self.evidence is not None and not isinstance(
            self.evidence, DeliveryAttemptEvidence
        ):
            raise ValidationError("evidence must be DeliveryAttemptEvidence or None")


class ChatTransport(Protocol):
    name: str
    api_version: int

    def validate(self, config: ChatTransportConfig) -> None: ...

    def send(
        self, target: OrchestratorRef, notice: CompletionNotice
    ) -> DeliveryReceipt: ...
