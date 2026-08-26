"""Codex queue chat transport: fixed trusted text into an existing session."""

from __future__ import annotations

import math
import os
import re
import subprocess
from pathlib import Path
from typing import Callable, Mapping

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
#: `TimeoutError` when acceptance is unknown, `SessionGoneError` when the
#: session is gone, and `OSError` when the queue is unreachable.
QueueSender = Callable[[str, str], "str | None"]


# QueueSender implementations must raise only this error for an absent session.
class SessionGoneError(LookupError):
    """The queue target no longer exists; never create a replacement."""


class CodexQueueSender:
    """Run the proven ``codex queue`` command against one existing session."""

    _CLAUDE_AUTH = frozenset(
        {"CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"}
    )
    _SESSION_LIMIT = 512
    _MESSAGE_LIMIT = 4096
    _MESSAGE_ID_LIMIT = 512

    def __init__(
        self,
        executable: str | Path,
        *,
        timeout_seconds: float = 30.0,
        max_output_bytes: int = 4096,
        environment: Mapping[str, str] | None = None,
        runner: Callable[..., subprocess.CompletedProcess[object]] = subprocess.run,
    ) -> None:
        try:
            candidate = Path(executable)
        except TypeError as error:
            raise ValidationError("codex queue executable must be an absolute file") from error
        if not candidate.is_absolute():
            raise ValidationError("codex queue executable must be an absolute file")
        try:
            resolved = candidate.resolve(strict=True)
        except OSError as error:
            raise ValidationError("codex queue executable must be an absolute file") from error
        if not resolved.is_file() or not os.access(resolved, os.X_OK):
            raise ValidationError("codex queue executable must be executable")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not 0 < float(timeout_seconds) < float("inf")
        ):
            raise ValidationError("codex queue timeout must be positive and finite")
        if type(max_output_bytes) is not int or max_output_bytes < 1:
            raise ValidationError("codex queue output limit must be a positive integer")
        if not callable(runner):
            raise ValidationError("codex queue runner must be callable")
        inherited = os.environ if environment is None else environment
        if not isinstance(inherited, Mapping) or any(
            not isinstance(key, str) or not isinstance(value, str)
            for key, value in inherited.items()
        ):
            raise ValidationError("codex queue environment must map strings to strings")
        self._executable = str(resolved)
        self._timeout = float(timeout_seconds)
        self._max_output = max_output_bytes
        self._environment = {
            key: value for key, value in inherited.items() if key not in self._CLAUDE_AUTH
        }
        self._runner = runner

    def __call__(self, external_session_id: str, text: str) -> str | None:
        session = self._bounded_text(
            "external_session_id", external_session_id, self._SESSION_LIMIT
        )
        message = self._bounded_text("message", text, self._MESSAGE_LIMIT, bytes_limit=True)
        argv = [
            self._executable,
            "queue",
            "--thread",
            session,
            "--message",
            message,
        ]
        try:
            completed = self._runner(
                argv,
                capture_output=True,
                text=True,
                timeout=self._timeout,
                env=dict(self._environment),
                check=False,
                shell=False,
            )
        except subprocess.TimeoutExpired as error:
            raise TimeoutError("codex queue acceptance is unknown") from error
        except OSError:
            raise
        except subprocess.SubprocessError as error:
            raise OSError("codex queue subprocess failed") from error

        returncode = getattr(completed, "returncode", None)
        if type(returncode) is not int:
            raise DeliveryError("codex queue returned an invalid process result")
        output = getattr(completed, "stdout", None) or getattr(completed, "stderr", None) or ""
        if isinstance(output, bytes):
            output = output.decode("utf-8", "replace")
        if not isinstance(output, str):
            raise DeliveryError("codex queue returned invalid output")
        if len(output.encode("utf-8")) > self._max_output:
            raise DeliveryError("codex queue output exceeded its limit")
        output = output.strip()

        if returncode == 0:
            hit = re.search(r"Queued message (\S+)", output)
            if hit is None:
                return None
            message_id = hit.group(1).rstrip(".")
            if not message_id or len(message_id) > self._MESSAGE_ID_LIMIT:
                raise DeliveryError("codex queue returned an invalid message id")
            return message_id
        if self._session_is_gone(output, session):
            raise SessionGoneError("codex queue session is gone")
        raise DeliveryError(f"codex queue exited with status {returncode}")

    @staticmethod
    def _bounded_text(
        name: str, value: object, limit: int, *, bytes_limit: bool = False
    ) -> str:
        if not isinstance(value, str) or not value.strip() or "\x00" in value:
            raise ValidationError(f"{name} must be a nonblank bounded string")
        size = len(value.encode("utf-8")) if bytes_limit else len(value)
        if size > limit:
            raise ValidationError(f"{name} exceeds its size limit")
        return value

    @staticmethod
    def _session_is_gone(output: str, session: str) -> bool:
        lines = [line.strip() for line in output.splitlines() if line.strip()]
        if not lines:
            return False
        result = lines[-1]
        if result.startswith("Error: "):
            result = result[len("Error: ") :]
        return result in {
            f"thread not found: {session}",
            f"Session not found: {session}",
            f"no thread with id: {session}",
        }


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
        except SessionGoneError as error:
            raise DeliveryError(
                "codex queue session is gone; agent-run never opens a replacement"
            ) from error
        except OSError as error:
            raise DeliveryError(
                f"codex queue is unreachable ({type(error).__name__})"
            ) from error
        if remote_message_id is not None and not isinstance(remote_message_id, str):
            raise DeliveryError("codex queue returned a non-string message id")
        return DeliveryReceipt(remote_message_id=remote_message_id, ambiguous=False)
