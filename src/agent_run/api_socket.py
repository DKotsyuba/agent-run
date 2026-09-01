"""JSON-RPC API over a private Unix-domain socket.

SQLite connections are thread-affine (see StateStore.path), so the server
never shares the service across handler threads: one dedicated dispatcher
thread constructs the service and executes every tool call sequentially,
while ThreadingUnixStreamServer handlers only forward requests to it and
wait on a future. That also serializes dispatch, so no extra lock exists.
"""

from __future__ import annotations

import json
import math
import os
import queue
import signal
import socket
import socketserver
import stat
import threading
from concurrent.futures import Future
from pathlib import Path
from typing import IO, Callable

from .dispatch import (
    TOOL_NAMES,
    TOOLS,
    Session,
    _arguments,
    _bounded,
    _emit,
    _error,
    _jsonable,
    _string,
    _valid_id,
    call_tool,
)
from .errors import AgentRunError, ValidationError
from .launch_evidence import bootstrap_error_fields
from .wait import (
    WATCHER_TIMEOUT_EXIT,
    _workflow_status,
    wait_for_agent,
    wait_for_workflow,
)

MAX_LINE_BYTES = 1024 * 1024
_MISSING = object()
_DEFAULT_SOCKET = ".agent-run/api.sock"
METHOD_NAMES = TOOL_NAMES | {"tools", "ping", "wait", "workflow_wait"}


def default_socket_path() -> Path:
    return Path.home() / _DEFAULT_SOCKET


class _Dispatcher:
    """Owns the service and runs every tool call on one dedicated thread.

    The service is constructed inside the worker thread so its SQLite
    connection is created and only ever used there. Handler threads submit
    work and block on a future; exceptions propagate with their real type.
    """

    def __init__(self, service_factory: Callable[[], object]):
        self._queue: queue.Queue = queue.Queue()
        self._boot: Future = Future()
        self._thread = threading.Thread(
            target=self._run, args=(service_factory,), daemon=True
        )
        self._thread.start()
        self._boot.result()

    def _run(self, service_factory: Callable[[], object]) -> None:
        try:
            service = service_factory()
        except BaseException as error:
            self._boot.set_exception(error)
            return
        self._boot.set_result(None)
        while True:
            item = self._queue.get()
            if item is None:
                return
            method, params, session, future = item
            try:
                future.set_result(call_tool(service, method, params, session))
            except BaseException as error:
                future.set_exception(error)

    def call(self, method: str, params: dict, session: Session) -> object:
        future: Future = Future()
        self._queue.put((method, params, session, future))
        return future.result()

    def close(self) -> None:
        self._queue.put(None)


def _rpc_error(request_id: object, code: int, message: object, data: object = _MISSING) -> dict:
    error = {"code": code, "message": _bounded(message)}
    if data is not _MISSING:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}


def _agent_run_error(request_id: object, error: AgentRunError) -> dict:
    data = {
        "code": type(error).__name__,
        "message": _bounded(error),
        **bootstrap_error_fields(error),
    }
    return _rpc_error(request_id, -32000, error, data)


def _wait_timeout(params: dict) -> float:
    if "timeout_seconds" not in params:
        return 0.0
    value = params["timeout_seconds"]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError("timeout_seconds must be a positive finite number")
    if value <= 0 or not math.isfinite(value):
        raise ValidationError("timeout_seconds must be a positive finite number")
    return float(value)


def _wait_result(method: str, outcome) -> object:
    result = _jsonable(outcome.payload)
    if outcome.exit_code != WATCHER_TIMEOUT_EXIT:
        return result
    if not isinstance(result, dict):
        result = {"payload": result}
    result["timed_out"] = True
    if "status" not in result:
        result["status"] = _jsonable(
            _workflow_status(outcome.payload)
            if method == "workflow_wait"
            else getattr(outcome.payload, "status", "unknown")
        )
    return result


def _run_wait(method: str, params: dict, service_factory: Callable[[], object]) -> object:
    if method == "wait":
        agent_id = _string(_arguments(params, {"agent_id", "timeout_seconds"}, {"agent_id"}), "agent_id")
    else:
        run_id = _string(_arguments(params, {"run_id", "timeout_seconds"}, {"run_id"}), "run_id")
    timeout = _wait_timeout(params)
    service = service_factory()
    try:
        outcome = (
            wait_for_agent(service, agent_id, timeout=timeout)
            if method == "wait"
            else wait_for_workflow(service, run_id, timeout=timeout)
        )
        return _wait_result(method, outcome)
    finally:
        close = getattr(service, "close", None)
        if callable(close):
            close()


def _handle(
    dispatcher: _Dispatcher,
    request: object,
    session: Session,
    service_factory: Callable[[], object],
) -> dict | None:
    if isinstance(request, list):
        return _rpc_error(None, -32600, "batch requests are not supported")
    if not isinstance(request, dict):
        return _rpc_error(None, -32600, "invalid request")

    request_id = request.get("id", _MISSING)
    response_id = None if request_id is _MISSING else request_id
    if request_id is not _MISSING and not _valid_id(request_id):
        return _rpc_error(None, -32600, "invalid request id")
    if request.get("jsonrpc") != "2.0" or not isinstance(request.get("method"), str):
        return None if request_id is _MISSING else _rpc_error(response_id, -32600, "invalid request")

    method = request["method"]
    params = request.get("params", {})
    if not isinstance(params, dict):
        return None if request_id is _MISSING else _rpc_error(response_id, -32602, "params must be an object")

    try:
        if method == "ping":
            result = {"ok": True}
        elif method == "tools":
            result = _jsonable(TOOLS)
        elif method in {"wait", "workflow_wait"}:
            result = _run_wait(method, params, service_factory)
        elif method not in TOOL_NAMES:
            response = _rpc_error(response_id, -32601, "method not found")
            return None if request_id is _MISSING else response
        else:
            result = _jsonable(dispatcher.call(method, params, session))
    except ValidationError as error:
        response = _rpc_error(response_id, -32602, error)
        return None if request_id is _MISSING else response
    except AgentRunError as error:
        response = _agent_run_error(response_id, error)
        return None if request_id is _MISSING else response
    except Exception as error:
        response = _error(response_id, -32603, f"internal error: {_bounded(type(error).__name__)}")
        return None if request_id is _MISSING else response

    response = {"jsonrpc": "2.0", "id": response_id, "result": result}
    return None if request_id is _MISSING else response


class _SocketWriter:
    def __init__(self, stream: IO[bytes]):
        self.stream = stream

    def write(self, value: str) -> int:
        encoded = value.encode("utf-8")
        self.stream.write(encoded)
        return len(value)

    def flush(self) -> None:
        self.stream.flush()


class _Handler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        session = Session()
        writer = _SocketWriter(self.wfile)
        while True:
            line = self.rfile.readline(MAX_LINE_BYTES + 1)
            if not line:
                return
            if len(line) > MAX_LINE_BYTES:
                while line and not line.endswith(b"\n"):
                    line = self.rfile.readline(MAX_LINE_BYTES + 1)
                _emit(writer, _rpc_error(None, -32700, "request exceeds maximum size"))
                continue
            try:
                request = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                _emit(writer, _rpc_error(None, -32700, "parse error"))
                continue
            response = _handle(
                self.server.dispatcher, request, session, self.server.service_factory
            )
            if response is not None:
                _emit(writer, response)


class ApiServer(socketserver.ThreadingUnixStreamServer):
    allow_reuse_address = False
    daemon_threads = True

    def __init__(self, socket_path: str | Path, service_factory: Callable[[], object]):
        path = Path(socket_path).expanduser().resolve()
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        if path.exists():
            if not stat.S_ISSOCK(path.stat().st_mode):
                raise ValidationError(f"API socket path is not a socket: {path}")
            probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                probe.settimeout(0.5)
                probe.connect(str(path))
                probe.sendall(b'{"jsonrpc":"2.0","id":0,"method":"ping"}\n')
                reply = json.loads(probe.makefile("rb").readline())
                if reply.get("id") == 0 and reply.get("result") == {"ok": True}:
                    raise ValidationError(f"API socket is already in use: {path}")
                path.unlink()
            except ValidationError:
                raise
            except (OSError, ValueError, json.JSONDecodeError):
                path.unlink()
            finally:
                probe.close()
        self.socket_path = path
        self.service_factory = service_factory
        self.dispatcher = _Dispatcher(service_factory)
        super().__init__(str(path), _Handler, bind_and_activate=True)
        os.chmod(path, 0o600)
        bound = path.stat()
        self._socket_identity = (bound.st_dev, bound.st_ino)

    def release_socket_path(self) -> bool:
        """Unlink this server's socket path without deleting a replacement."""
        try:
            current = self.socket_path.stat()
        except FileNotFoundError:
            return False
        if (current.st_dev, current.st_ino) != self._socket_identity:
            return False
        self.socket_path.unlink()
        return True

    def server_close(self) -> None:
        super().server_close()
        self.dispatcher.close()


def serve(service_factory: Callable[[], object], socket_path: str | Path | None = None) -> int:
    """Serve JSON-RPC requests until SIGINT, SIGTERM, or server shutdown."""
    server = ApiServer(default_socket_path() if socket_path is None else socket_path, service_factory)
    previous: dict[int, object] = {}

    def stop(signum: int, _frame: object) -> None:
        server.release_socket_path()
        threading.Thread(target=server.shutdown, daemon=True).start()

    try:
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous[signum] = signal.signal(signum, stop)
        server.serve_forever()
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        server.server_close()
        server.release_socket_path()
    return 0
