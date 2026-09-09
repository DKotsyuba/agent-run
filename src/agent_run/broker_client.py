"""Minimal client for the resident agent-run Unix-socket API."""

from __future__ import annotations

import json
import math
import socket
import threading
from dataclasses import asdict
from pathlib import Path
from types import SimpleNamespace

from .domain import OrchestratorRef, StartRequest
from .errors import AgentRunError, BrokerUnavailable, ValidationError

MAX_LINE_BYTES = 1024 * 1024
_DEFAULT_TIMEOUT = 600.0
_BROKER_MESSAGE = (
    "agent-run broker is not running; start it with `agent-run api serve` "
    "or its launchd job"
)


class BrokerClient:
    """Lazily connect to the broker and preserve one API session per client."""

    def resume(
        self, agent_id: str, task: str, *, timeout_seconds: float | None = None,
        request_id: str | None = None, orchestrator: OrchestratorRef | None = None,
    ) -> SimpleNamespace:
        """Submit a continuation to the resident broker and return its new run ID.

        Parameters follow AgentService.resume; authority is inherited server-side.
        Connection/domain errors propagate, and malformed success replies raise
        AgentRunError. This client never starts a local preparation worker.
        """
        result = self.call("resume", {
            "agent_id": agent_id, "task": task, "timeout_seconds": timeout_seconds,
            "request_id": request_id,
            "orchestrator": None if orchestrator is None else asdict(orchestrator),
        })
        if not isinstance(result, dict) or not isinstance(result.get("agent_id"), str) or not isinstance(result.get("created"), bool):
            raise AgentRunError("broker returned an invalid resume result")
        return SimpleNamespace(**result)

    def __init__(self, socket_path: Path) -> None:
        self.socket_path = Path(socket_path)
        self._socket: socket.socket | None = None
        self._stream = None
        self._next_id = 1
        self._lock = threading.Lock()
        self._aborted = threading.Event()

    def _connect(self, timeout: float) -> None:
        """Open one abortable Unix-socket connection within ``timeout`` seconds.

        The socket is published before ``connect`` blocks so another thread may
        interrupt it through ``abort``. Cancellation raises ``ConnectionError``
        and leaves no reusable socket or stream.
        """
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        with self._lock:
            if self._aborted.is_set():
                sock.close()
                raise ConnectionError("broker call cancelled")
            self._socket = sock
        try:
            sock.connect(str(self.socket_path))
            with self._lock:
                if self._aborted.is_set() or self._socket is not sock:
                    raise ConnectionError("broker call cancelled")
                self._stream = sock.makefile("rb")
        except OSError:
            with self._lock:
                if self._socket is sock:
                    self._socket = None
            sock.close()
            raise

    def _close(self) -> None:
        with self._lock:
            stream, sock = self._stream, self._socket
            self._stream = None
            self._socket = None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        if stream is not None:
            stream.close()
        if sock is not None:
            sock.close()

    def abort(self) -> None:
        """Interrupt this caller's connect or response wait without cancelling broker work."""
        self._aborted.set()
        with self._lock:
            sock = self._socket
            self._socket = None
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            sock.close()

    def _request(self, method: str, params: dict | None, timeout: float) -> object:
        """Send one bounded frame and validate its matching response envelope."""

        if self._socket is None or self._stream is None:
            self._connect(timeout)
        request_id = self._next_id
        self._next_id += 1
        request = {"jsonrpc": "2.0", "id": request_id, "method": method}
        if params is not None:
            request["params"] = params
        encoded = json.dumps(request, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"
        if len(encoded) > MAX_LINE_BYTES:
            raise ValidationError("request exceeds maximum size")
        assert self._socket is not None and self._stream is not None
        self._socket.settimeout(timeout)
        self._socket.sendall(encoded)
        line = self._stream.readline(MAX_LINE_BYTES + 1)
        if not line:
            raise ConnectionError("broker closed the connection")
        if len(line) > MAX_LINE_BYTES:
            raise ConnectionError("broker response exceeds maximum size")
        try:
            response = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ConnectionError("broker returned invalid JSON") from error
        if not isinstance(response, dict):
            raise ConnectionError("broker returned an invalid response")
        if response.get("id") != request_id:
            raise ConnectionError("broker returned a mismatched response id")
        if "error" in response:
            error = response["error"]
            if not isinstance(error, dict):
                raise AgentRunError("broker returned an invalid error")
            message = str(error.get("message", "broker request failed"))
            code = error.get("code")
            if code == -32602:
                raise ValidationError(message)
            if code == -32000:
                data = error.get("data")
                mapped = AgentRunError(message)
                if isinstance(data, dict):
                    mapped.broker_error_code = data.get("code")
                    mapped.broker_error_data = data
                raise mapped
            raise AgentRunError(message)
        if "result" not in response:
            raise AgentRunError("broker returned an invalid response")
        return response["result"]

    def call(self, method: str, params: dict | None = None, timeout: float = _DEFAULT_TIMEOUT) -> object:
        """Forward one API request within a positive finite response deadline.

        ``method`` is nonblank, ``params`` is an optional object, and ``timeout``
        bounds connect, write, and response reads for each of at most two
        connection attempts. Validation errors are never retried; transport
        exhaustion raises :class:`BrokerUnavailable`.
        """

        if not isinstance(method, str) or not method:
            raise ValidationError("method must be a nonblank string")
        if params is not None and not isinstance(params, dict):
            raise ValidationError("params must be an object or null")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(timeout)
            or timeout <= 0
        ):
            raise ValidationError("timeout must be positive and finite")
        timeout = float(timeout)
        for attempt in range(2):
            try:
                return self._request(method, params, timeout)
            except (OSError, ConnectionError, TimeoutError):
                self._close()
                if self._aborted.is_set():
                    raise ConnectionError("broker call cancelled") from None
                if attempt == 0:
                    continue
                raise BrokerUnavailable(_BROKER_MESSAGE)

    def ping(self) -> bool:
        """Return whether the broker answered the protocol ping."""

        return self.call("ping") == {"ok": True}

    def start(self, request: StartRequest) -> SimpleNamespace:
        """Serialize and submit ``request`` to the resident broker asynchronously.

        Converts paths and the optional orchestrator reference to JSON-safe
        values, including sorted policy requirements, returns ``agent_id`` and
        ``created``, and leaves execution
        owned by the broker after this client closes. Raises ``ValidationError``
        for invalid input, ``AgentRunError`` for malformed results or domain
        failures, and ``BrokerUnavailable`` when the broker cannot be reached.
        """
        if not isinstance(request, StartRequest):
            raise ValidationError("request must be a StartRequest")
        params = {
            "runtime": request.runtime, "model": request.model,
            "profile": request.profile, "task": request.task,
            "workdir": str(request.workdir), "write": request.write,
            "effort": request.effort, "timeout_seconds": request.timeout_seconds,
            "read_roots": [str(path) for path in request.read_roots],
            "output_schema": request.output_schema,
            "orchestrator": (None if request.orchestrator is None else {
                "transport": request.orchestrator.transport,
                "external_session_id": request.orchestrator.external_session_id,
                "external_turn_id": request.orchestrator.external_turn_id,
            }),
            "request_id": request.request_id, "fast": request.fast,
            "account": request.account,
            "required_constraints": sorted(
                constraint.value for constraint in request.required_constraints
            ),
        }
        result = self.call("start", params)
        if (
            not isinstance(result, dict)
            or not isinstance(result.get("agent_id"), str)
            or not isinstance(result.get("created"), bool)
        ):
            raise AgentRunError("broker returned an invalid start result")
        return SimpleNamespace(**result)

    def close(self) -> None:
        self._close()
