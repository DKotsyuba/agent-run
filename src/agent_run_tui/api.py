"""Small JSON-RPC client for the resident agent-run API socket."""

from __future__ import annotations

import json
import os
from pathlib import Path
import socket
from typing import Any


_MAX_LINE = 1024 * 1024


class ApiUnavailable(Exception):
    """Raised when the resident API socket cannot be reached."""


class ApiError(Exception):
    """An RPC error, retaining the server's numeric ``code`` and ``data``."""

    def __init__(self, message: str, code: int, data: Any = None) -> None:
        """Create an error from the server message, code, and optional data."""
        super().__init__(message)
        self.code = code
        self.data = data


def default_socket_path() -> Path:
    """Return the API socket below ``AGENT_RUN_HOME`` or ``~/.agent-run``."""
    return Path(os.environ.get("AGENT_RUN_HOME", Path.home() / ".agent-run")) / "api.sock"


class ApiClient:
    """Persistent newline-delimited JSON-RPC client with safe pre-write retry."""

    def __init__(self, socket_path: Path, timeout: float = 20.0) -> None:
        """Use ``socket_path`` with ``timeout`` seconds for connect and reads."""
        self.socket_path, self.timeout = socket_path, timeout
        self._socket: socket.socket | None = None
        self._next_id = 1

    def _connect(self) -> socket.socket:
        """Open and retain a stream connection, translating availability failures."""
        if self._socket is None:
            try:
                connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                connection.settimeout(self.timeout)
                connection.connect(str(self.socket_path))
                self._socket = connection
            except OSError as error:
                self.close()
                raise ApiUnavailable("agent-run API is unavailable; run `agent-run api serve`") from error
        return self._socket

    def call(self, method: str, params: dict[str, Any] | None = None) -> Any:
        """Call ``method`` and retry only failures before its request is written.

        RPC errors become :class:`ApiError`, as does any reply that is not a
        JSON-RPC object carrying ``result`` or ``error``; transport failures,
        including an oversized frame, are :class:`ApiUnavailable` and are not
        resent because the server may already have performed the request.  A
        reply that times out after a complete send names a busy server, not a
        missing one.
        """
        request_id = self._next_id
        self._next_id += 1
        payload = (json.dumps({"jsonrpc": "2.0", "id": request_id, "method": method,
                               "params": params or {}}, separators=(",", ":")) + "\n").encode()
        for _attempt in range(2):
            sent = 0
            try:
                connection = self._connect()
                while sent < len(payload):
                    written = connection.send(payload[sent:])
                    if written <= 0:
                        raise ConnectionError("socket closed before request write")
                    sent += written
                chunks = bytearray()
                while not chunks.endswith(b"\n"):
                    chunk = connection.recv(65536)
                    if not chunk:
                        raise ConnectionError("socket closed")
                    chunks.extend(chunk)
                    if len(chunks) > _MAX_LINE:
                        raise ConnectionError("oversized response")
                try:
                    reply = json.loads(chunks.decode())
                except (UnicodeDecodeError, json.JSONDecodeError) as error:
                    raise ApiError("malformed RPC response", -32700) from error
                if not isinstance(reply, dict):
                    raise ApiError("malformed RPC response", -32700)
                if "error" in reply:
                    error = reply["error"]
                    if not isinstance(error, dict):
                        raise ApiError("malformed RPC error", -32700)
                    raise ApiError(str(error.get("message", "RPC error")), int(error.get("code", -32000)), error.get("data"))
                if "result" not in reply:
                    raise ApiError("malformed RPC response", -32700)
                return reply["result"]
            except ApiError:
                raise
            except (OSError, ConnectionError, ValueError, json.JSONDecodeError) as error:
                self.close()
                if sent == 0 and _attempt == 0:
                    continue
                if sent == len(payload) and isinstance(error, TimeoutError):
                    raise ApiUnavailable(f"agent-run API did not answer within {self.timeout:g}s (server busy?)") from error
                raise ApiUnavailable("agent-run API is unavailable; run `agent-run api serve`") from error
        raise AssertionError("unreachable")

    def close(self) -> None:
        """Close the retained socket; calling this more than once is harmless."""
        if self._socket is not None:
            self._socket.close()
            self._socket = None

    def __enter__(self) -> "ApiClient":
        """Return this client for a context-managed lifetime."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the client when its context exits."""
        self.close()
