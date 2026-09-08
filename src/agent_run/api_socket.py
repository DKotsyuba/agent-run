"""JSON-RPC API over a private Unix-domain socket.

SQLite connections are thread-affine (see StateStore.path), so the server
never shares the service across handler threads: one dedicated dispatcher
thread constructs the service and executes every tool call sequentially,
while ThreadingUnixStreamServer handlers only forward requests to it and
wait on a future. That also serializes dispatch, so no extra lock exists.
"""

from __future__ import annotations

import errno
import fcntl
import json
import math
import os
import queue
import signal
import socket
import socketserver
import stat
import threading
import time
from concurrent.futures import Future, TimeoutError as FutureTimeout
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
    wait_for_agent,
)

MAX_LINE_BYTES = 1024 * 1024
MAX_CONNECTIONS = 32
MAX_PENDING_REQUESTS = 64
REQUEST_DEADLINE_SECONDS = 30.0
IDLE_DEADLINE_SECONDS = 30.0
WRITE_DEADLINE_SECONDS = 5.0
SHUTDOWN_DEADLINE_SECONDS = 5.0
CONTROL_FRAME_DEADLINE_SECONDS = 0.5
_MISSING = object()
_DEFAULT_SOCKET = ".agent-run/api.sock"
METHOD_NAMES = TOOL_NAMES | {"tools", "ping", "wait"}
_CONTROL_METHODS = frozenset({"start", "resume", "cancel", "steer", "fast"})


class _Overloaded(RuntimeError):
    """The bounded dispatcher or connection pool has no free capacity."""


class _RequestDeadline(RuntimeError):
    """A request exceeded its server-side execution or shutdown deadline."""


class _DispatcherClosed(RuntimeError):
    """The owning dispatcher is closing and cannot accept or finish queued work."""


def default_socket_path() -> Path:
    return Path.home() / _DEFAULT_SOCKET


class _Dispatcher:
    """Own one service and a bounded request queue on one dedicated thread.

    The service is constructed inside the worker thread so its SQLite
    connection is created, called, and closed there. Handler threads submit up
    to ``max_pending`` calls and wait at most ``request_timeout`` seconds.
    Closing rejects new calls, resolves queued futures, and gives an executing
    call the caller-supplied shutdown budget before returning.
    """

    def __init__(
        self,
        service_factory: Callable[[], object],
        *,
        max_pending: int,
        request_timeout: float,
    ) -> None:
        """Start the owner thread and wait for its service to initialize."""

        self._queue: queue.Queue = queue.Queue(maxsize=max_pending)
        self._boot: Future = Future()
        self._closed = threading.Event()
        self._close_lock = threading.Lock()
        self._request_timeout = request_timeout
        self._thread = threading.Thread(
            target=self._run, args=(service_factory,), daemon=True
        )
        self._thread.start()
        self._boot.result(timeout=request_timeout)

    def _run(self, service_factory: Callable[[], object]) -> None:
        """Construct, exclusively use, and close the thread-affine service."""

        try:
            service = service_factory()
        except BaseException as error:
            self._boot.set_exception(error)
            return
        self._boot.set_result(None)
        try:
            while True:
                item = self._queue.get()
                if item is None:
                    return
                method, params, session, future = item
                if not future.set_running_or_notify_cancel():
                    continue
                try:
                    future.set_result(call_tool(service, method, params, session))
                except BaseException as error:
                    future.set_exception(error)
        finally:
            close = getattr(service, "close", None)
            if callable(close):
                close()

    def call(self, method: str, params: dict, session: Session) -> object:
        """Submit one call or raise an explicit overload/deadline/closed error."""

        future: Future = Future()
        with self._close_lock:
            if self._closed.is_set():
                raise _DispatcherClosed("API dispatcher is shutting down")
            try:
                self._queue.put_nowait((method, params, session, future))
            except queue.Full as error:
                raise _Overloaded("API request queue is full") from error
        try:
            return future.result(timeout=self._request_timeout)
        except FutureTimeout as error:
            future.cancel()
            raise _RequestDeadline("API request deadline exceeded") from error

    def close(self, timeout: float) -> None:
        """Reject submissions, fail queued calls, and boundedly join the owner."""

        with self._close_lock:
            if not self._closed.is_set():
                self._closed.set()
                while True:
                    try:
                        item = self._queue.get_nowait()
                    except queue.Empty:
                        break
                    if item is not None:
                        future = item[3]
                        if not future.done():
                            future.set_exception(
                                _DispatcherClosed("API dispatcher is shutting down")
                            )
                self._queue.put_nowait(None)
        self._thread.join(max(0.0, timeout))


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


def _wait_result(outcome) -> object:
    """Return one socket wait payload with watcher timeout metadata."""

    result = _jsonable(outcome.payload)
    if outcome.exit_code != WATCHER_TIMEOUT_EXIT:
        return result
    if not isinstance(result, dict):
        result = {"payload": result}
    result["timed_out"] = True
    if "status" not in result:
        result["status"] = _jsonable(getattr(outcome.payload, "status", "unknown"))
    return result


def _run_wait(
    params: dict,
    service_factory: Callable[[], object],
    shutdown: threading.Event,
) -> object:
    """Run one bounded-slot wait and interrupt its sleep during shutdown."""

    agent_id = _string(
        _arguments(params, {"agent_id", "timeout_seconds"}, {"agent_id"}),
        "agent_id",
    )
    timeout = _wait_timeout(params)
    service = service_factory()

    def sleep(seconds: float) -> None:
        """Sleep for polling unless server shutdown interrupts this wait."""

        if shutdown.wait(seconds):
            raise _DispatcherClosed("API server is shutting down")

    try:
        return _wait_result(
            wait_for_agent(service, agent_id, timeout=timeout, sleep=sleep)
        )
    finally:
        close = getattr(service, "close", None)
        if callable(close):
            close()


def _handle(server: ApiServer, request: object, session: Session) -> dict | None:
    """Validate and execute one decoded JSON-RPC request for ``server``."""

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
        elif method == "wait":
            result = _run_wait(params, server.service_factory, server.shutdown_event)
        elif method not in TOOL_NAMES:
            response = _rpc_error(response_id, -32601, "method not found")
            return None if request_id is _MISSING else response
        else:
            result = _jsonable(server.dispatcher_for(method).call(method, params, session))
    except _Overloaded as error:
        response = _rpc_error(response_id, -32001, error)
        return None if request_id is _MISSING else response
    except _RequestDeadline as error:
        response = _rpc_error(response_id, -32002, error)
        return None if request_id is _MISSING else response
    except _DispatcherClosed as error:
        response = _rpc_error(response_id, -32003, error)
        return None if request_id is _MISSING else response
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
    """Adapt a binary socket stream to the text writer used by dispatch."""

    def __init__(self, stream: IO[bytes]):
        """Wrap ``stream`` without assuming ownership of its lifetime."""

        self.stream = stream

    def write(self, value: str) -> int:
        """Encode and buffer one UTF-8 text fragment, returning characters."""

        encoded = value.encode("utf-8")
        self.stream.write(encoded)
        return len(value)

    def flush(self) -> None:
        """Flush buffered bytes through the socket's active write deadline."""

        self.stream.flush()


class _Handler(socketserver.StreamRequestHandler):
    """Serve one deadline-bound, session-isolated client connection."""

    def handle(self) -> None:
        """Read bounded newline frames until EOF, idle timeout, or write failure."""

        session = Session()
        writer = _SocketWriter(self.wfile)
        self.request.settimeout(
            min(self.server.idle_timeout, CONTROL_FRAME_DEADLINE_SECONDS)
            if self.server.control_connection_only(self.request)
            else self.server.idle_timeout
        )
        while True:
            try:
                line = self.rfile.readline(MAX_LINE_BYTES + 1)
            except (OSError, TimeoutError):
                return
            if not line:
                return
            if len(line) > MAX_LINE_BYTES:
                while line and not line.endswith(b"\n"):
                    line = self.rfile.readline(MAX_LINE_BYTES + 1)
                self.request.settimeout(self.server.write_timeout)
                try:
                    _emit(writer, _rpc_error(None, -32700, "request exceeds maximum size"))
                except (OSError, TimeoutError):
                    return
                self.request.settimeout(self.server.idle_timeout)
                continue
            try:
                request = json.loads(line.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                self.request.settimeout(self.server.write_timeout)
                try:
                    _emit(writer, _rpc_error(None, -32700, "parse error"))
                except (OSError, TimeoutError):
                    return
                self.request.settimeout(self.server.idle_timeout)
                continue
            if self.server.control_connection_only(self.request) and (
                not isinstance(request, dict)
                or request.get("method") not in _CONTROL_METHODS
            ):
                request_id = request.get("id") if isinstance(request, dict) else None
                self.request.settimeout(self.server.write_timeout)
                try:
                    _emit(
                        writer,
                        _rpc_error(
                            request_id,
                            -32001,
                            "API capacity is reserved for control requests",
                        ),
                    )
                except (OSError, TimeoutError):
                    pass
                return
            response = _handle(self.server, request, session)
            if response is not None:
                self.request.settimeout(self.server.write_timeout)
                try:
                    _emit(writer, response)
                except (OSError, TimeoutError):
                    return
            if self.server.control_connection_only(self.request):
                return
            self.request.settimeout(self.server.idle_timeout)


def _startup_lock(path: Path) -> int:
    """Acquire the lifetime flock that fences check, bind, and path ownership.

    The adjacent lock file is opened without following symlinks and must be a
    regular file owned by the current user. The returned descriptor holds the
    nonblocking exclusive lock until closed; contention raises
    :class:`ValidationError` without probing or changing the socket path.
    """

    lock_path = path.with_name(f".{path.name}.lock")
    flags = os.O_CREAT | os.O_RDWR | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as error:
        raise ValidationError(f"cannot open API startup lock: {lock_path}") from error
    try:
        info = os.fstat(descriptor)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
            raise ValidationError(f"API startup lock is not a private owned file: {lock_path}")
        os.fchmod(descriptor, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return descriptor
    except BlockingIOError as error:
        os.close(descriptor)
        raise ValidationError(f"API socket is already in use: {path}") from error
    except BaseException:
        os.close(descriptor)
        raise


def _reclaim_stale_socket(path: Path) -> None:
    """Remove ``path`` only after connection refusal and stable inode proof.

    A successful connection, timeout, permission denial, or any ambiguous
    result means an owner may still be live and is refused. Only
    ``ECONNREFUSED`` proves a socket inode has no listener; the inode is checked
    again immediately before unlinking so a replacement is never removed.
    """

    try:
        original = path.lstat()
    except FileNotFoundError:
        return
    if not stat.S_ISSOCK(original.st_mode):
        raise ValidationError(f"API socket path is not a socket: {path}")
    probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        probe.settimeout(0.5)
        probe.connect(str(path))
    except OSError as error:
        if error.errno != errno.ECONNREFUSED:
            raise ValidationError(f"API socket ownership is uncertain: {path}") from error
    else:
        raise ValidationError(f"API socket is already in use: {path}")
    finally:
        probe.close()
    try:
        current = path.lstat()
    except FileNotFoundError:
        return
    if (current.st_dev, current.st_ino) != (original.st_dev, original.st_ino):
        raise ValidationError(f"API socket changed during stale reclaim: {path}")
    path.unlink()


class ApiServer(socketserver.ThreadingUnixStreamServer):
    """Bounded private JSON-RPC server with two thread-affine service lanes."""

    allow_reuse_address = False
    daemon_threads = True

    def __init__(
        self,
        socket_path: str | Path,
        service_factory: Callable[[], object],
        *,
        max_connections: int = MAX_CONNECTIONS,
        max_pending_requests: int = MAX_PENDING_REQUESTS,
        request_timeout: float = REQUEST_DEADLINE_SECONDS,
        idle_timeout: float = IDLE_DEADLINE_SECONDS,
        write_timeout: float = WRITE_DEADLINE_SECONDS,
        shutdown_timeout: float = SHUTDOWN_DEADLINE_SECONDS,
    ) -> None:
        """Fence ``socket_path`` and initialize bounded control/read owners.

        Numeric limits must be positive and finite. ``service_factory`` is
        called once in each dispatcher thread and must return a fresh service
        with its own SQLite connection. Potentially slow read/probe methods use
        the read lane; durable admission, cancel, steer, and session-local fast
        settings use the control lane. Construction refuses ambiguous existing
        sockets and releases every acquired resource on failure.
        """

        if not callable(service_factory):
            raise ValidationError("service_factory must be callable")
        for name, value in (
            ("max_connections", max_connections),
            ("max_pending_requests", max_pending_requests),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValidationError(f"{name} must be a positive integer")
        for name, value in (
            ("request_timeout", request_timeout),
            ("idle_timeout", idle_timeout),
            ("write_timeout", write_timeout),
            ("shutdown_timeout", shutdown_timeout),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise ValidationError(f"{name} must be positive and finite")
        path = Path(socket_path).expanduser().resolve()
        path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.socket_path = path
        self.service_factory = service_factory
        self.idle_timeout = float(idle_timeout)
        self.write_timeout = float(write_timeout)
        self.shutdown_timeout = float(shutdown_timeout)
        self.shutdown_event = threading.Event()
        self._control_connection_slots = threading.BoundedSemaphore(
            1 if max_connections > 1 else 0
        )
        self._connection_slots = threading.BoundedSemaphore(
            max_connections - (1 if max_connections > 1 else 0)
        )
        self._connections: set[socket.socket] = set()
        self._control_connections: set[socket.socket] = set()
        self._connections_lock = threading.Lock()
        self._closed = False
        self._startup_lock_fd: int | None = _startup_lock(path)
        self._dispatchers: tuple[_Dispatcher, ...] = ()
        try:
            _reclaim_stale_socket(path)
            self._dispatchers += (
                _Dispatcher(
                    service_factory,
                    max_pending=max_pending_requests,
                    request_timeout=float(request_timeout),
                ),
            )
            self._dispatchers += (
                _Dispatcher(
                    service_factory,
                    max_pending=max_pending_requests,
                    request_timeout=float(request_timeout),
                ),
            )
            super().__init__(str(path), _Handler, bind_and_activate=True)
            os.chmod(path, 0o600)
            bound = path.stat()
            self._socket_identity = (bound.st_dev, bound.st_ino)
        except BaseException:
            for dispatcher in self._dispatchers:
                dispatcher.close(self.shutdown_timeout)
            if self._startup_lock_fd is not None:
                os.close(self._startup_lock_fd)
                self._startup_lock_fd = None
            raise

    def dispatcher_for(self, method: str) -> _Dispatcher:
        """Return the control owner for mutations, otherwise the read owner."""

        return self._dispatchers[0 if method in _CONTROL_METHODS else 1]

    def process_request(self, request: socket.socket, client_address: object) -> None:
        """Start a handler only when a bounded connection slot is available."""

        regular_slot = self._connection_slots.acquire(blocking=False)
        control_slot = False
        if not regular_slot:
            control_slot = self._control_connection_slots.acquire(blocking=False)
        if not regular_slot and not control_slot:
            try:
                request.settimeout(self.write_timeout)
                request.sendall(
                    (
                        json.dumps(
                            _rpc_error(None, -32001, "API connection limit reached"),
                            separators=(",", ":"),
                        )
                        + "\n"
                    ).encode("utf-8")
                )
            except OSError:
                pass
            self.shutdown_request(request)
            return
        with self._connections_lock:
            self._connections.add(request)
            if control_slot:
                self._control_connections.add(request)
        try:
            super().process_request(request, client_address)
        except BaseException:
            with self._connections_lock:
                self._connections.discard(request)
                self._control_connections.discard(request)
            (
                self._control_connection_slots
                if control_slot
                else self._connection_slots
            ).release()
            raise

    def control_connection_only(self, request: socket.socket) -> bool:
        """Return whether the request occupies the reserved control slot."""

        with self._connections_lock:
            return request in self._control_connections

    def process_request_thread(
        self, request: socket.socket, client_address: object
    ) -> None:
        """Run one handler and always return its connection slot."""

        try:
            super().process_request_thread(request, client_address)
        finally:
            with self._connections_lock:
                self._connections.discard(request)
                control_slot = request in self._control_connections
                self._control_connections.discard(request)
            (
                self._control_connection_slots
                if control_slot
                else self._connection_slots
            ).release()

    def release_socket_path(self) -> bool:
        """Unlink this server's socket path without deleting a replacement."""
        try:
            current = self.socket_path.stat()
        except FileNotFoundError:
            return False
        if (current.st_dev, current.st_ino) != self._socket_identity:
            return False
        self.socket_path.unlink()
        if self._startup_lock_fd is not None:
            os.close(self._startup_lock_fd)
            self._startup_lock_fd = None
        return True

    def server_close(self) -> None:
        """Close listener/connections and boundedly stop each service owner once."""

        if self._closed:
            return
        self._closed = True
        self.shutdown_event.set()
        super().server_close()
        with self._connections_lock:
            connections = tuple(self._connections)
        for connection in connections:
            try:
                connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
        deadline = time.monotonic() + self.shutdown_timeout
        for dispatcher in self._dispatchers:
            dispatcher.close(deadline - time.monotonic())
        if self._startup_lock_fd is not None:
            os.close(self._startup_lock_fd)
            self._startup_lock_fd = None


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
