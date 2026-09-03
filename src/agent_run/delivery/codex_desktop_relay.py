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
#: Socket names starting with this prefix advertise the rich local protocol
#: v2; every other ``ar-cdx-*.sock`` endpoint keeps the legacy six-key wire.
_V2_PREFIX = "ar-cdx-v2-"


def _request(path: Path, target: OrchestratorRef, notice: CompletionNotice) -> bytes:
    """Encode one endpoint's bounded request: legacy six keys, or rich v2 nine.

    Only the discovered socket's name selects the wire version, and the tag
    is advisory: a rich frame that reaches a stale legacy host is rejected
    before any send, so discovery safely continues to the next endpoint.

    Args:
        path (Path): Discovered relay socket path.
        target (OrchestratorRef): Existing session receiving the notice.
        notice (CompletionNotice): Validated terminal lifecycle facts; its
            optional launch metadata is included only on the rich wire.

    Returns:
        bytes: The uint32-LE framed JSON request for this endpoint.

    Raises:
        ValidationError: The encoded frame exceeds the local size limit,
            before any socket is opened.
    """

    request: dict[str, object] = {
        "version": 1, "op": "completion", "thread_id": target.external_session_id,
        "notification_id": notice.notification_id, "agent_id": str(notice.agent_id),
        "status": notice.status.value,
    }
    if path.name.startswith(_V2_PREFIX):
        request.update({
            "version": 2,
            "runtime": notice.runtime, "model": notice.model, "effort": notice.effort,
        })
    return _frame(request)


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

        Discovery takes at most ten seconds total across candidates. Each
        endpoint receives the wire its socket name advertises: the rich
        version 2 frame with launch metadata for ``ar-cdx-v2-*.sock`` paths,
        the unchanged legacy six-key version 1 frame for older hosts. Any
        error once sendall is attempted is ambiguous; only rejection or a
        pre-write failure permits another relay. No queue or state/database
        access occurs.

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
        rejected = False
        for path in self._paths():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            request = _request(path, target, notice)
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
        """Return at most sixteen endpoints, rich v2 sockets first; unreadable is empty.

        Ordering prefers ``ar-cdx-v2-*.sock`` endpoints so the rich wire is
        used whenever a capable host is live, inside the same sixteen
        endpoint discovery bound as before.
        """

        try:
            return tuple(
                sorted(
                    self._home.glob("ar-cdx-*.sock"),
                    key=lambda path: (not path.name.startswith(_V2_PREFIX), path.name),
                )[:_MAX_RELAYS]
            )
        except OSError:
            return ()
