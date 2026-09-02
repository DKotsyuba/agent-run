"""Volatile private relay between delivery dispatch and one Codex Desktop host."""

import json
import logging
import os
import socket
import struct
import threading
import time
import uuid
from pathlib import Path
from typing import Final

from ..domain import AgentStatus, OrchestratorRef
from ..errors import ValidationError
from .base import AmbiguousDeliveryError, CompletionNotice, DeliveryAttemptEvidence

_MAX_FRAME: Final = 8192
_SOCKET_PREFIX: Final = "ar-cdx-"
_SOCKET_SUFFIX: Final = ".sock"
_MAX_RELAYS: Final = 16
_TIMEOUT: Final = 2.0
#: Bounds an injected host pipe path; well under any OS/AF_UNIX path limit.
_MAX_PIPE_PATH: Final = 4096

_logger = logging.getLogger(__name__)


def _frame(value: object) -> bytes:
    """Encode one bounded JSON value as a little-endian length-prefixed frame."""

    raw = json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    if len(raw) > _MAX_FRAME:
        raise ValidationError("codex desktop relay frame exceeds its size limit")
    return struct.pack("<I", len(raw)) + raw


def _read_frame(connection: socket.socket) -> object:
    """Read and decode exactly one bounded framed JSON value from ``connection``."""

    def receive(size: int) -> bytes:
        """Read exactly ``size`` bytes or reject an incomplete frame."""

        chunks: list[bytes] = []
        remaining = size
        while remaining:
            chunk = connection.recv(remaining)
            if not chunk:
                raise EOFError("incomplete codex desktop relay frame")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    size = struct.unpack("<I", receive(4))[0]
    if not 0 < size <= _MAX_FRAME:
        raise ValueError("invalid codex desktop relay frame size")
    value = json.loads(receive(size).decode("utf-8"))
    if not isinstance(value, dict):
        raise ValueError("codex desktop relay frame must be an object")
    return value


def _evidence(classifier: str, started: float) -> DeliveryAttemptEvidence:
    """Return static, secret-free evidence for a relay terminal outcome."""

    return DeliveryAttemptEvidence(
        classifier=classifier,
        executable="codex_desktop_relay",
        argv_shape=("relay",),
        duration_ms=int((time.monotonic() - started) * 1000),
    )


def _valid_pipe_path(value: str) -> bool:
    """Return whether ``value`` is a bounded, absolute, NUL-free pipe path."""

    return (
        bool(value)
        and len(value) <= _MAX_PIPE_PATH
        and "\x00" not in value
        and Path(value).is_absolute()
    )


class CodexDesktopRelayServer:
    """Serve one process-private relay socket backed by one Desktop host pipe."""

    def __init__(self, home: Path, host_pipe_path: str) -> None:
        """Bind a mode-0600 socket under ``home`` for the supplied volatile pipe path."""

        self._home = home
        self._host_pipe_path = host_pipe_path
        self.path = home / (
            f"{_SOCKET_PREFIX}{os.getpid()}-{uuid.uuid4().hex[:8]}{_SOCKET_SUFFIX}"
        )
        self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._closed = threading.Event()

    @classmethod
    def start_from_environment(cls, home: Path) -> "CodexDesktopRelayServer | None":
        """Start a relay only when the host injected a valid, nonempty pipe path.

        An injected value that is empty is a normal, silent opt-out. An
        injected value that is nonempty but fails validation (relative,
        oversized, or containing a NUL byte) loudly disables the relay via a
        logged error and never binds or persists a relay socket.
        """

        value = os.environ.get("CODEX_APP_TOOLS_PIPE_PATH")
        if not value:
            return None
        if not _valid_pipe_path(value):
            _logger.error(
                "CODEX_APP_TOOLS_PIPE_PATH is invalid; the codex desktop relay is disabled"
            )
            return None
        server = cls(home, value)
        server.start()
        return server

    def start(self) -> None:
        """Bind and begin accepting short-lived private relay connections."""

        self._home.mkdir(parents=True, exist_ok=True)
        self._socket.bind(str(self.path))
        os.chmod(self.path, 0o600)
        self._socket.listen(8)
        self._thread.start()

    def close(self) -> None:
        """Stop the server and remove its volatile socket path.

        Safe to call before :meth:`start`: closing an unbound socket is a
        no-op, unlinking a path that was never created is tolerated, and the
        server thread is joined only when it was actually started.
        """

        if self._closed.is_set():
            return
        self._closed.set()
        try:
            self._socket.close()
        finally:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
        if self._thread.is_alive():
            self._thread.join(timeout=_TIMEOUT)

    def _serve(self) -> None:
        """Accept connections until closed; malformed callers receive no acceptance."""

        while not self._closed.is_set():
            try:
                connection, _ = self._socket.accept()
            except OSError:
                return
            with connection:
                connection.settimeout(_TIMEOUT)
                try:
                    request = _read_frame(connection)
                    outcome = self._deliver(request)
                    connection.sendall(_frame({"outcome": outcome}))
                except (EOFError, OSError, ValueError, ValidationError, json.JSONDecodeError):
                    continue

    def _deliver(self, request: object) -> str:
        """Validate one relay request and return only ``accepted`` or ``rejected``.

        A malformed *client* request (this method's own ``request`` argument)
        always raises immediately; that failure happens before any host
        socket is touched. Once a connection to the host pipe is open, a
        failure to discover the completion tool (unreachable pipe, malformed
        ``tools/list`` response, no matching tool) returns ``"rejected"`` so
        the caller falls back to the queue. From the moment the ``tools/call``
        request is sent onward, any failure to obtain a valid, matching-id
        response raises instead of returning a string, so the caller (an
        already-written client request) is never told anything more specific
        than "connection lost" and must treat the outcome as ambiguous.
        """

        if not isinstance(request, dict) or set(request) != {
            "version", "op", "thread_id", "notification_id", "agent_id", "status"
        }:
            raise ValidationError("invalid codex desktop relay request")
        if request["version"] != 1 or request["op"] != "completion":
            raise ValidationError("invalid codex desktop relay request")
        thread_id = request["thread_id"]
        if not isinstance(thread_id, str) or not thread_id or "\x00" in thread_id or len(thread_id) > 512:
            raise ValidationError("invalid codex desktop relay thread")
        raw_status = request["status"]
        if not isinstance(raw_status, str):
            raise ValidationError("invalid codex desktop relay status")
        try:
            status = AgentStatus(raw_status)
        except ValueError as error:
            raise ValidationError("invalid codex desktop relay status") from error
        notice = CompletionNotice(
            notification_id=request["notification_id"],
            agent_id=request["agent_id"],
            status=status,
        )

        call_sent = False
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as host:
                host.settimeout(_TIMEOUT)
                host.connect(self._host_pipe_path)
                list_id = uuid.uuid4().hex
                host.sendall(_frame({
                    "jsonrpc": "2.0",
                    "id": list_id,
                    "method": "tools/list",
                    "params": {"threadStartKind": "all"},
                }))
                listed = _read_frame(host)
                namespace = self._namespace(listed, list_id)
                if namespace is None:
                    return "rejected"
                call_id = uuid.uuid4().hex
                call_sent = True
                host.sendall(_frame({
                    "jsonrpc": "2.0",
                    "id": call_id,
                    "method": "tools/call",
                    "params": {
                        "arguments": {"threadId": thread_id, "prompt": notice.render()},
                        "callId": call_id,
                        "namespace": namespace,
                        "threadId": thread_id,
                        "tool": "send_message_to_thread",
                        "turnId": call_id,
                    },
                }))
                result = _read_frame(host)
                return self._classify_result(result, call_id)
        except (OSError, EOFError, ValueError, ValidationError, json.JSONDecodeError):
            if call_sent:
                raise
            return "rejected"

    @staticmethod
    def _namespace(response: object, expected_id: str) -> str | None:
        """Find the advertised ``send_message_to_thread`` namespace, if any.

        Returns ``None`` (a pre-call, fallback-safe outcome) for a response
        that is not an object, does not echo ``expected_id``, or does not
        advertise the exact tool.
        """

        if (
            not isinstance(response, dict)
            or response.get("jsonrpc") != "2.0"
            or response.get("id") != expected_id
        ):
            return None
        result = response.get("result")
        tools = result.get("tools") if isinstance(result, dict) else None
        if not isinstance(tools, list):
            return None
        for tool in tools:
            if isinstance(tool, dict) and tool.get("name") == "send_message_to_thread" and isinstance(tool.get("namespace"), str):
                return tool["namespace"]
        return None

    @staticmethod
    def _classify_result(result: object, expected_id: str) -> str:
        """Classify one ``tools/call`` host response, raising on any ambiguity.

        Only a response that echoes ``expected_id`` and carries a ``result``
        object with ``success`` exactly ``True`` or exactly ``False`` is
        explicit; every other shape (missing/mismatched id, an ``error``
        envelope, a malformed or missing ``result``, or a missing/unknown
        ``success`` value) raises :class:`ValidationError` so the caller
        treats the attempt as ambiguous rather than as an explicit rejection.
        """

        if (
            not isinstance(result, dict)
            or result.get("jsonrpc") != "2.0"
            or result.get("id") != expected_id
        ):
            raise ValidationError("codex desktop relay host response id mismatch")
        if "error" in result:
            raise ValidationError("codex desktop relay host reported an error")
        payload = result.get("result")
        if not isinstance(payload, dict):
            raise ValidationError("codex desktop relay host returned a malformed result")
        success = payload.get("success")
        if success is True:
            return "accepted"
        if success is False:
            return "rejected"
        raise ValidationError("codex desktop relay host returned an unknown success value")


class CodexDesktopRelayClient:
    """Try bounded local relay sockets before the normal Codex queue fallback."""

    def __init__(self, home: Path) -> None:
        """Create a client scanning only relay sockets below ``home``."""

        self._home = home
        self.last_evidence: DeliveryAttemptEvidence | None = None

    def send(self, target: OrchestratorRef, notice: CompletionNotice) -> bool:
        """Return acceptance, fall through on rejection, or raise on post-write ambiguity."""

        self.last_evidence = None
        for path in self._paths():
            started = time.monotonic()
            written = False
            try:
                with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                    connection.settimeout(_TIMEOUT)
                    connection.connect(str(path))
                    request = _frame({"version": 1, "op": "completion", "thread_id": target.external_session_id, "notification_id": notice.notification_id, "agent_id": str(notice.agent_id), "status": notice.status.value})
                    written = True
                    connection.sendall(request)
                    response = _read_frame(connection)
                if response == {"outcome": "accepted"}:
                    self.last_evidence = _evidence("relay_accepted", started)
                    return True
                if response == {"outcome": "rejected"}:
                    continue
                raise ValueError("unknown relay outcome")
            except (ConnectionRefusedError, FileNotFoundError):
                try:
                    path.unlink()
                except FileNotFoundError:
                    pass
                continue
            except (EOFError, OSError, ValueError, json.JSONDecodeError) as error:
                if written:
                    self.last_evidence = _evidence("relay_ambiguous", started)
                    raise AmbiguousDeliveryError("codex desktop relay acceptance is unknown", evidence=self.last_evidence) from error
        return False

    def _paths(self) -> tuple[Path, ...]:
        """Return a deterministic, bounded snapshot of candidate relay paths."""

        try:
            return tuple(sorted(self._home.glob(f"{_SOCKET_PREFIX}*{_SOCKET_SUFFIX}"))[:_MAX_RELAYS])
        except OSError:
            return ()
