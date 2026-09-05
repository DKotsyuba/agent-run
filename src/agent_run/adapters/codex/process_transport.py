"""Real Codex app-server subprocess transport and bounded diagnostics."""

from __future__ import annotations

import io
import json
import os
import queue
import select
import re
import subprocess
import threading
import time
from collections import deque
from typing import Mapping

from ...errors import ValidationError
from ..base import LaunchPlan
from ..claude.stderr import StderrTail

#: Sentinel placed on the inbound queue after the app-server closes stdout.
_STREAM_CLOSED = object()
#: Environment names whose literal values must never survive stderr capture.
_SENSITIVE_ENV = re.compile(r"(?:AUTH|CREDENTIAL|KEY|PASSWORD|SECRET|TOKEN)", re.I)


def _environment_secrets(environment: Mapping[str, str]) -> tuple[str, ...]:
    """Return nontrivial credential-shaped environment values for redaction.

    ``environment`` is the exact child environment. Values shorter than eight
    characters are ignored so harmless flags such as ``TOKENPIPE_*=1`` cannot
    redact every matching digit from diagnostics. The result is detached and
    contains no names or values from non-sensitive variables.
    """

    return tuple(
        value
        for name, value in environment.items()
        if _SENSITIVE_ENV.search(name) and len(value) >= 8
    )


class ProcessTransport:
    """Own one ``codex app-server`` subprocess and its bounded diagnostics.

    One stdout thread decodes JSON-RPC messages and one stderr thread retains a
    4096-byte redacted tail. Requests are serialized by the owning session;
    :meth:`terminate` owns final process and pipe cleanup.
    """

    def __init__(self, plan: LaunchPlan) -> None:
        """Launch ``plan`` and begin draining stdout plus secret-safe stderr.

        Process creation errors propagate from ``Popen``. Successful
        construction owns the child and all three standard pipes.
        """

        self._process = subprocess.Popen(
            list(plan.argv),
            cwd=str(plan.cwd),
            env=dict(plan.environment),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True,
        )
        if self._process.stdin is not None:
            os.set_blocking(self._process.stdin.fileno(), False)
        self._next_id = 1
        self._incoming: queue.Queue = queue.Queue()
        self._notifications: deque = deque()
        self._stream_closed = False
        self._reader = threading.Thread(
            target=self._read_stream, name="codex-app-server-reader", daemon=True
        )
        self._stderr_stream = (
            io.TextIOWrapper(self._process.stderr, encoding="utf-8", errors="replace")
            if self._process.stderr is not None
            else None
        )
        self._stderr = StderrTail(
            self._stderr_stream, _environment_secrets(plan.environment)
        )
        self._stderr_reader = threading.Thread(
            target=self._stderr.drain,
            name="codex-app-server-stderr",
            daemon=True,
        )
        self._reader.start()
        self._stderr_reader.start()

    @property
    def pid(self) -> int | None:
        """Return the owned child PID assigned by ``Popen``."""

        return self._process.pid

    def _read_stream(self) -> None:
        """Drain stdout, queue valid JSON objects, and signal end of stream."""

        stream = self._process.stdout
        try:
            if stream is not None:
                for line in iter(stream.readline, b""):
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        message = json.loads(text)
                    except ValueError:
                        continue
                    if isinstance(message, dict):
                        self._incoming.put(message)
        finally:
            self._incoming.put(_STREAM_CLOSED)

    def _take(self, timeout: float | None, waiting_for: str) -> Mapping[str, object] | None:
        """Return one parsed message, ``None`` on timeout, or fail on EOF."""

        if self._stream_closed:
            raise self._closed_error(waiting_for)
        try:
            if timeout is None:
                message = self._incoming.get()
            elif timeout <= 0:
                message = self._incoming.get_nowait()
            else:
                message = self._incoming.get(timeout=timeout)
        except queue.Empty:
            return None
        if message is _STREAM_CLOSED:
            self._stream_closed = True
            raise self._closed_error(waiting_for)
        return message

    def _write(
        self, payload: Mapping[str, object], method: str, deadline: float | None = None
    ) -> None:
        """Write one complete JSON-RPC frame before the deadline expires."""

        frame = json.dumps(dict(payload)).encode("utf-8") + b"\n"
        deadline = time.monotonic() + 30.0 if deadline is None else deadline
        stdin = self._process.stdin
        if stdin is None:
            raise ConnectionError(f"codex app-server has no stdin for {method}")
        try:
            fd = stdin.fileno()
        except ValueError as error:
            raise ConnectionError(f"codex app-server stdin is closed for {method}") from error
        sent = 0
        while sent < len(frame):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"codex app-server timed out writing {method}")
            try:
                _, writable, _ = select.select([], [fd], [], remaining)
            except (OSError, ValueError) as error:
                raise ConnectionError(f"codex app-server stdin is closed for {method}") from error
            if not writable:
                raise TimeoutError(f"codex app-server timed out writing {method}")
            try:
                count = os.write(fd, frame[sent:])
            except BlockingIOError:
                continue
            except OSError as error:
                raise ConnectionError(f"codex app-server stdin is closed for {method}") from error
            if count <= 0:
                raise ConnectionError(f"codex app-server stdin is closed for {method}")
            sent += count

    def request(
        self,
        method: str,
        params: Mapping[str, object],
        *,
        timeout_seconds: float = 30.0,
    ) -> Mapping[str, object]:
        """Send one JSON-RPC request or raise with bounded early-exit stderr.

        ``method`` is the app-server method and ``params`` its object payload.
        ``timeout_seconds`` is a positive finite deadline. A valid response
        mapping is returned; protocol errors raise ``ValidationError``, a live
        timeout raises ``TimeoutError``, and early process exit raises
        ``ConnectionError`` with exit/signal evidence and redacted stderr.
        """

        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not 0 < timeout_seconds < float("inf")
        ):
            raise ValidationError("timeout_seconds must be positive and finite")
        deadline = time.monotonic() + float(timeout_seconds)
        request_id = self._next_id
        self._next_id += 1
        self._write(
            {"jsonrpc": "2.0", "id": request_id, "method": method, "params": dict(params)},
            method,
            deadline,
        )
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"codex app-server timed out waiting for {method}")
            message = self._take(remaining, method)
            if message is None:
                if self._process.poll() is None:
                    raise TimeoutError(f"codex app-server timed out waiting for {method}")
                raise self._closed_error(method)
            if isinstance(message.get("method"), str):
                if "id" in message:
                    self._write(
                        {
                            "jsonrpc": "2.0",
                            "id": message["id"],
                            "error": {"code": -32601, "message": "Method not found"},
                        },
                        str(message["method"]),
                        deadline,
                    )
                else:
                    self._notifications.append(message)
                continue
            if type(message.get("id")) is int and message["id"] == request_id:
                if "error" in message:
                    raise ValidationError(f"codex app-server rejected {method}: {message['error']}")
                result = message.get("result", {})
                return result if isinstance(result, Mapping) else {}

    def notify(self, method: str, params: Mapping[str, object] | None = None) -> None:
        """Send a client notification that cannot authorize server actions."""

        self._write(
            {
                "jsonrpc": "2.0",
                "method": method,
                **({"params": dict(params)} if params is not None else {}),
            },
            method,
        )

    def poll_event(self, timeout: float | None) -> Mapping[str, object] | None:
        """Return one notification within ``timeout``, or ``None`` when absent."""

        if self._notifications:
            return self._notifications.popleft()
        deadline = None if timeout is None else time.monotonic() + max(float(timeout), 0.0)
        while True:
            remaining = None if deadline is None else max(deadline - time.monotonic(), 0.0)
            message = self._take(remaining, "event")
            if message is None:
                return None
            if isinstance(message.get("method"), str):
                return message
            if deadline is not None and time.monotonic() >= deadline:
                return None

    def terminate(self, grace_seconds: float) -> None:
        """Stop the child within ``grace_seconds`` and close every owned pipe.

        An already exited child is only reaped and drained. A live child gets
        SIGTERM, then SIGKILL when the nonnegative grace period expires.
        """

        try:
            if self._process.poll() is None:
                self._process.terminate()
                self._process.wait(timeout=grace_seconds)
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=max(grace_seconds, 1))
        finally:
            self._reader.join(timeout=1)
            self._stderr_reader.join(timeout=1)
            self._close_pipes()

    def close(self) -> None:
        """Close request input without terminating the owned child process."""

        if self._process.stdin:
            self._process.stdin.close()

    def _close_pipes(self) -> None:
        """Close the child pipes after their reader threads have settled."""

        for pipe in (self._process.stdin, self._process.stdout, self._stderr_stream):
            if pipe is not None:
                try:
                    pipe.close()
                except OSError:
                    pass

    def _closed_error(self, method: str) -> ConnectionError:
        """Describe an exited child using its exit/signal and redacted stderr."""

        self._stderr_reader.join(timeout=1)
        returncode = self._process.poll()
        if returncode is None:
            try:
                returncode = self._process.wait(timeout=0.1)
            except subprocess.TimeoutExpired:
                pass
        status = (
            f"signal {-returncode}"
            if returncode is not None and returncode < 0
            else f"exit code {returncode}" if returncode is not None else "stream"
        )
        detail = self._stderr.text()
        suffix = f": {detail}" if detail else ""
        return ConnectionError(
            f"codex app-server {status} closed the stream while waiting for {method}{suffix}"
        )
