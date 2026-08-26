"""Claude Code UDS chat transport: fixed trusted text into an existing session.

Mechanism
---------
A live Claude Code session publishes ``~/.claude/sessions/<pid>.json`` naming
its ``sessionId`` and a ``messagingSocketPath`` under ``/tmp/cc-socks``, plus a
paired mode-0600 ``<pid>.<digest>.key`` file holding the inbox ``peerToken``.
The session's own log documents the wire format::

    { echo '{"type":"auth","token":"..."}';
      echo '{"type":"user","message":{"role":"user","content":"hello"}}'; } \\
      | socat - UNIX-CONNECT:/tmp/cc-socks/<pid>.sock

Delivery resolves the session id in that registry, probe-connects the socket,
then writes the auth line followed by the user line as newline-delimited JSON.

Verified on claude 2.1.245 (2026-08-27)
---------------------------------------
* Headless sessions register too. A ``--print --input-format stream-json``
  child publishes a descriptor and a live socket exactly like a TUI session,
  so delivery is not limited to interactive orchestrators.
* Injection surfaces as a real user turn: the receiving session woke and
  answered the injected text.
* The registry is not feature-gated. Descriptor and socket appeared with
  ``CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS`` set to ``1``, set to ``0``, and
  absent entirely (with ``--setting-sources ""`` so user settings could not
  re-inject it). Absence of a descriptor therefore means "session gone", not
  "feature off".
* A clean exit removes the descriptor; ``SIGKILL`` leaves a stale descriptor
  *and* a stale socket file behind, which is why file presence is never
  trusted: only a probe-connect decides, and ``ECONNREFUSED`` means gone.

Known blind spot: the inbox sends no acknowledgement, so a clean write proves
the line was accepted by the peer, not that a human ever saw it. A message the
peer holds pending approval is invisible from this side.
"""

from __future__ import annotations

import json
import math
import socket
from pathlib import Path
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


TRANSPORT_NAME = "claude_uds"

#: Inject `text` into the Claude Code session named by `external_session_id`
#: and return the remote message id when one is reported. Raise `TimeoutError`
#: when acceptance is unknown, `SessionGoneError` when the session is gone, and
#: `OSError` when the socket is unreachable.
UdsSender = Callable[[str, str], "str | None"]


def default_registry() -> Path:
    """``~/.claude/sessions``, resolved per call so tests can move ``HOME``."""

    return Path.home() / ".claude" / "sessions"


# UdsSender implementations must raise only this error for an absent session.
class SessionGoneError(LookupError):
    """The session no longer exists; never create a replacement."""


class ClaudeSessionSender:
    """Write one trusted line into the inbox socket of one existing session."""

    _SESSION_LIMIT = 512
    _MESSAGE_LIMIT = 4096
    #: A registry scan must never stall a dispatcher on a pathological dir.
    _MAX_DESCRIPTORS = 512

    def __init__(
        self,
        registry_dir: str | Path | None = None,
        *,
        timeout_seconds: float = 5.0,
        probe_seconds: float = 1.0,
    ) -> None:
        directory = default_registry() if registry_dir is None else Path(registry_dir)
        if not directory.is_absolute():
            raise ValidationError("claude session registry must be an absolute directory")
        for name, value in (
            ("timeout", timeout_seconds),
            ("probe timeout", probe_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not 0 < float(value) < float("inf")
            ):
                raise ValidationError(f"claude uds {name} must be positive and finite")
        self._registry = directory
        self._timeout = float(timeout_seconds)
        self._probe = float(probe_seconds)

    def __call__(self, external_session_id: str, text: str) -> str | None:
        session = self._bounded_text(
            "external_session_id", external_session_id, self._SESSION_LIMIT
        )
        message = self._bounded_text("message", text, self._MESSAGE_LIMIT, bytes_limit=True)
        pid, socket_path = self._resolve(session)
        token = self._token(pid)
        connection = self._connect(socket_path)
        try:
            connection.settimeout(self._timeout)
            connection.sendall(
                _line({"type": "auth", "token": token})
                + _line({"type": "user", "message": {"role": "user", "content": message}})
            )
        except (socket.timeout, TimeoutError, BrokenPipeError) as error:
            # Bytes may already sit in the peer's buffer, or it hung up after
            # consuming part of them. At-least-once: report the ambiguity.
            raise TimeoutError("claude uds acceptance is unknown") from error
        finally:
            try:
                connection.close()
            except OSError:
                pass
        # The inbox never acknowledges, so there is no remote message id.
        return None

    def _resolve(self, session: str) -> tuple[int, str]:
        """Find the descriptor whose sessionId matches, or report it gone."""

        try:
            paths = sorted(self._registry.glob("*.json"))[: self._MAX_DESCRIPTORS]
        except OSError as error:
            raise SessionGoneError("claude session registry is unreadable") from error
        for path in paths:
            try:
                document = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError, UnicodeDecodeError):
                continue
            if not isinstance(document, dict) or document.get("sessionId") != session:
                continue
            socket_path = document.get("messagingSocketPath")
            pid = document.get("pid")
            if not isinstance(socket_path, str) or not socket_path:
                continue
            if isinstance(pid, bool) or not isinstance(pid, int):
                continue
            return pid, socket_path
        raise SessionGoneError(f"no claude session descriptor for {session}")

    def _token(self, pid: int) -> str:
        """Read the inbox auth token published beside the descriptor."""

        for path in sorted(self._registry.glob(f"{pid}.*.key")):
            try:
                document = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, ValueError, UnicodeDecodeError):
                continue
            token = document.get("peerToken") if isinstance(document, dict) else None
            if isinstance(token, str) and token:
                return token
        # Nothing was written yet, so this is a clean refusal, not an ambiguity.
        raise DeliveryError("claude session inbox auth key is missing or unreadable")

    def _connect(self, socket_path: str) -> socket.socket:
        """Probe-connect: a stale descriptor outlives its process, a socket cannot."""

        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        connection.settimeout(self._probe)
        try:
            connection.connect(socket_path)
        except (ConnectionRefusedError, FileNotFoundError, NotADirectoryError) as error:
            connection.close()
            raise SessionGoneError("claude session socket is dead") from error
        except (socket.timeout, TimeoutError) as error:
            connection.close()
            # The peer was never reached, so nothing can have been delivered.
            raise SessionGoneError("claude session socket did not accept") from error
        except OSError:
            connection.close()
            raise
        return connection

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


def _line(document: dict[str, object]) -> bytes:
    """One newline-delimited JSON frame; a raw newline would split the frame."""

    return json.dumps(document, ensure_ascii=True, sort_keys=True).encode("utf-8") + b"\n"


class ClaudeUdsTransport:
    """Deliver a completion notice by injecting one line into a live session.

    The transport has no session-open path by construction. A missing session
    is a hard failure: a completion wake never starts a replacement session and
    never starts a replacement agent.
    """

    name = TRANSPORT_NAME
    api_version = TRANSPORT_API_VERSION

    def __init__(self, sender: UdsSender) -> None:
        if not callable(sender):
            raise ValidationError("claude uds sender must be callable")
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
            # The inbox may have accepted the line. At-least-once: report
            # ambiguity so the dispatcher records it and retries.
            raise AmbiguousDeliveryError(
                f"claude uds acceptance is unknown for {notice.notification_id}"
            ) from error
        except SessionGoneError as error:
            raise DeliveryError(
                "claude session is gone; agent-run never opens a replacement"
            ) from error
        except OSError as error:
            raise DeliveryError(
                f"claude session inbox is unreachable ({type(error).__name__})"
            ) from error
        if remote_message_id is not None and not isinstance(remote_message_id, str):
            raise DeliveryError("claude uds returned a non-string message id")
        return DeliveryReceipt(remote_message_id=remote_message_id, ambiguous=False)
