"""Trusted lifecycle notices emitted for terminal workflow runs."""

from __future__ import annotations

from dataclasses import dataclass

from ..errors import ValidationError

NOTICE_VERSION = 1
_TERMINAL = frozenset({"succeeded", "failed", "cancelled", "lost"})


@dataclass(frozen=True, slots=True)
class WorkflowNotice:
    """Describe one terminal workflow lifecycle event without result content.

    ``notification_id`` and ``run_id`` are durable trusted identifiers and
    ``status`` is one of the workflow terminal states.  The immutable notice
    can be passed to the existing chat transports because it exposes the same
    ``payload`` and ``render`` call shape as an agent completion notice.
    """

    notification_id: str
    run_id: str
    status: str
    version: int = NOTICE_VERSION

    def __post_init__(self) -> None:
        """Validate identifiers, protocol version, and terminal status."""

        for name, value in (("notification_id", self.notification_id), ("run_id", self.run_id)):
            if not isinstance(value, str) or not value.strip() or len(value) > 128:
                raise ValidationError(f"{name} must be a nonblank trusted identifier")
        if self.version != NOTICE_VERSION or isinstance(self.version, bool):
            raise ValidationError(f"notice version must be {NOTICE_VERSION}")
        if self.status not in _TERMINAL:
            raise ValidationError("workflow notice status must be terminal")

    def payload(self) -> dict[str, object]:
        """Return the lifecycle-only transport payload."""

        return {"version": self.version, "notification_id": self.notification_id,
                "run_id": self.run_id, "status": self.status}

    def render(self) -> str:
        """Render an attempt notice with explicit current-state lookup guidance.

        The fixed str includes no workflow-authored content. A resumed run may
        have advanced after this notice's terminal snapshot was recorded.
        """

        return (f"agent-run: workflow {self.run_id} finished with status {self.status}. "
                f"Call workflow_status({self.run_id}) for current state and "
                f"workflow_answer({self.run_id}) for the latest step result. "
                f"[notification {self.notification_id} v{self.version}]")
