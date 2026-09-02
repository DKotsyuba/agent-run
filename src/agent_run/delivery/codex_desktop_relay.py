"""Bounded client for the optional signed-Node Desktop completion relay."""
import json
import socket
import struct
import time
from pathlib import Path

from ..domain import OrchestratorRef
from ..errors import ValidationError
from .base import AmbiguousDeliveryError, CompletionNotice, DeliveryAttemptEvidence, DeliveryError

#: Local notices stay small; Node separately bounds the larger host inventory.
_MAX_FRAME = 8192
#: Total discovery budget in seconds, below the thirty-second delivery lease.
_TIMEOUT = 10.0
_MAX_RELAYS = 16


def _frame(value: object) -> bytes:
    """Encode a bounded JSON value as a uint32-LE frame; reject oversized data."""
    raw = json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    if not 0 < len(raw) <= _MAX_FRAME:
        raise ValidationError("relay frame exceeds its size limit")
    return struct.pack("<I", len(raw)) + raw


def _read_frame(connection: socket.socket, deadline: float | None = None) -> dict:
    """Read one bounded object before an optional monotonic deadline; raise on EOF."""
    def receive(size: int) -> bytes:
        """Collect exactly size bytes, limiting each read by the remaining deadline."""
        chunks = []
        while size:
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("relay deadline elapsed")
                connection.settimeout(remaining)
            chunk = connection.recv(size)
            if not chunk:
                raise EOFError("incomplete relay frame")
            chunks.append(chunk)
            size -= len(chunk)
        return b"".join(chunks)

    size = struct.unpack("<I", receive(4))[0]
    if not 0 < size <= _MAX_FRAME:
        raise ValueError("invalid relay frame size")
    value = json.loads(receive(size).decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("relay response must be an object")
    return value


def _evidence(classifier: str, started: float) -> DeliveryAttemptEvidence:
    """Return static transport facts and elapsed milliseconds, never private data."""
    return DeliveryAttemptEvidence(
        classifier=classifier, executable="codex_desktop_relay", argv_shape=("relay",),
        duration_ms=int((time.monotonic() - started) * 1000),
    )


class CodexDesktopRelayClient:
    """Try volatile local endpoints without owning state or changing delivery leases."""

    def __init__(self, home: Path) -> None:
        """Use home for relay discovery; last_evidence describes the latest send."""
        self._home = home
        self.last_evidence: DeliveryAttemptEvidence | None = None

    def send(self, target: OrchestratorRef, notice: CompletionNotice) -> bool:
        """Return True on acceptance or raise a retryable/ambiguous delivery error.

        Discovery takes at most ten seconds total across candidates. Any error
        once sendall is attempted is ambiguous; only rejection or a pre-write
        failure permits another relay. No queue or state/database access occurs.

        Args:
            target (OrchestratorRef): Existing session receiving the notice.
            notice (CompletionNotice): Validated terminal lifecycle facts.

        Returns:
            bool: True only for confirmed host acceptance.

        Raises:
            DeliveryError: No relay accepted; durable dispatch may retry.
            AmbiguousDeliveryError: Acceptance is unknown after a write attempt.
        """
        self.last_evidence = None
        started = time.monotonic()
        deadline = started + _TIMEOUT
        request = _frame({
            "version": 1, "op": "completion", "thread_id": target.external_session_id,
            "notification_id": notice.notification_id, "agent_id": str(notice.agent_id),
            "status": notice.status.value,
        })
        rejected = False
        for path in self._paths():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            written = False
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                    connection.settimeout(remaining)
                    connection.connect(str(path))
                    written = True
                    connection.sendall(request)
                    response = _read_frame(connection, deadline)
                if response == {"outcome": "accepted"}:
                    self.last_evidence = _evidence("relay_accepted", started)
                    return True
                if response == {"outcome": "rejected"}:
                    rejected = True
                    continue
                raise ValueError("unknown relay acceptance")
            except (EOFError, OSError, ValueError) as error:
                if written:
                    self.last_evidence = _evidence("relay_ambiguous", started)
                    raise AmbiguousDeliveryError(
                        "codex desktop relay acceptance is unknown", evidence=self.last_evidence
                    ) from error
                if isinstance(error, (ConnectionRefusedError, FileNotFoundError)):
                    try:
                        path.unlink()
                    except OSError:
                        pass
        classifier = "relay_rejected" if rejected else "relay_unavailable"
        self.last_evidence = _evidence(classifier, started)
        raise DeliveryError(
            "codex desktop relay did not accept completion", evidence=self.last_evidence
        )

    def _paths(self) -> tuple[Path, ...]:
        """Return at most sixteen local endpoints in sorted order; unreadable is empty."""
        try:
            return tuple(sorted(self._home.glob("ar-cdx-*.sock"))[:_MAX_RELAYS])
        except OSError:
            return ()
