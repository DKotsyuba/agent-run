"""Detach one accepted supervisor and wait only for its readiness proof."""

from __future__ import annotations

import json
import logging
import os
import select
import threading
import time
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager, suppress

from .errors import ValidationError
from .launch_evidence import (
    diagnose_bootstrap_failure,
    preflight_executable,
    write_exec_failure,
)

_logger = logging.getLogger("agent_run.launch")
from .lifecycle import (
    ReadyChannel,
    SystemProcessOps,
    VerifiedProcessGroup,
    terminate_process_group,
    verify_process_group,
)


# One budget covers exec, module imports, identity proof, config/store setup,
# and READY. Production launchd starts have needed 13.9s just for identity
# under routine collector load; 30s keeps failure bounded with useful headroom.
DEFAULT_READY_TIMEOUT_SECONDS = 30.0
DEFAULT_DISPATCH_TIMEOUT_SECONDS = 5.0
DEFAULT_REAP_TIMEOUT_SECONDS = 5.0
DEFAULT_CLEANUP_GRACE_SECONDS = 1.0
DEFAULT_CLEANUP_KILL_SECONDS = 1.0
#: Maximum default spawn-to-cleanup interval protected from reconciliation.
DEFAULT_STARTUP_HANDOFF_SECONDS = (
    DEFAULT_READY_TIMEOUT_SECONDS
    + DEFAULT_DISPATCH_TIMEOUT_SECONDS
    + 2 * (DEFAULT_CLEANUP_GRACE_SECONDS + DEFAULT_CLEANUP_KILL_SECONDS)
    + 0.5
)
SUPERVISOR_MODULE = "agent_run.supervisor_main"
_POLL_SECONDS = 0.01
_launch_context = threading.local()


@contextmanager
def launch_cancellation(cancel_requested: Callable[[], bool]) -> Iterator[None]:
    """Expose one worker's cancellation predicate to its nested launch call.

    Existing service launch callbacks keep their contract. The predicate is
    thread-local, applies only inside this context, and is restored afterward.
    """

    if not callable(cancel_requested):
        raise ValidationError("launch cancellation predicate must be callable")
    previous = getattr(_launch_context, "cancel_requested", None)
    _launch_context.cancel_requested = cancel_requested
    try:
        yield
    finally:
        if previous is None:
            with suppress(AttributeError):
                del _launch_context.cancel_requested
        else:
            _launch_context.cancel_requested = previous


class ChildReaper:
    """Reap only registered direct children for a long-lived launch owner.

    One daemon-owned instance may accept registrations from several threads.
    It polls exact child PIDs, preserving unrelated wait statuses and forwarding
    the raw wait status to each optional bounded callback.
    """

    def __init__(self) -> None:
        """Start the daemon reaper thread with no registered children."""

        self._children: dict[int, tuple[Callable[[int, int], object] | None, float]] = {}
        self._lock = threading.Lock()
        self._wake = threading.Event()
        self._closed = False
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def register(
        self,
        pid: int,
        callback: Callable[[int, int], object] | None = None,
        timeout_seconds: float = DEFAULT_REAP_TIMEOUT_SECONDS,
    ) -> None:
        """Register one direct child PID and its optional post-reap callback.

        ``pid`` must still be a direct child of this process. ``timeout_seconds``
        bounds only the callback; reaping itself continues until the child exits.
        Registration after :meth:`close` raises ``RuntimeError``.
        """

        with self._lock:
            if self._closed:
                raise RuntimeError("child reaper is closed")
            self._children[pid] = (callback, timeout_seconds)
        self._wake.set()

    def close(self, timeout_seconds: float = 1.0) -> None:
        """Stop accepting children and wait briefly for the reaper to quiesce.

        Existing live children remain registered. The daemon thread may outlive
        this bounded join and will exit after those children are reaped.
        """

        with self._lock:
            self._closed = True
        self._wake.set()
        self._thread.join(timeout_seconds)

    def _run(self) -> None:
        """Poll registered PIDs until closure and the child set are both empty."""

        while True:
            with self._lock:
                children = tuple(self._children.items())
                closed = self._closed
            if not children and closed:
                return
            for pid, (callback, timeout_seconds) in children:
                try:
                    waited, status = os.waitpid(pid, os.WNOHANG)
                except ChildProcessError:
                    with self._lock:
                        self._children.pop(pid, None)
                    continue
                except OSError:
                    continue
                if waited != pid:
                    continue
                with self._lock:
                    self._children.pop(pid, None)
                if callback is not None:
                    threading.Thread(
                        target=_invoke_reap_callback,
                        args=(callback, pid, status, timeout_seconds),
                        daemon=True,
                    ).start()
            self._wake.wait(_POLL_SECONDS if children else None)
            self._wake.clear()


def launch_detached(
    payload: Mapping[str, object],
    *,
    executable: str,
    module: str = SUPERVISOR_MODULE,
    readiness_timeout_seconds: float = DEFAULT_READY_TIMEOUT_SECONDS,
    post_terminal_timeout_seconds: float = DEFAULT_DISPATCH_TIMEOUT_SECONDS,
    post_reap: Callable[[int, int], object] | None = None,
    post_reap_timeout_seconds: float = DEFAULT_REAP_TIMEOUT_SECONDS,
    cleanup_grace_seconds: float = DEFAULT_CLEANUP_GRACE_SECONDS,
    cleanup_kill_seconds: float = DEFAULT_CLEANUP_KILL_SECONDS,
    child_reaper: ChildReaper | None = None,
    cancel_requested: Callable[[], bool] | None = None,
) -> int:
    """Spawn one session-leading supervisor and return its PID after READY.

    On supported systems, ``posix_spawn(..., setsid=True)`` creates the session
    and execs without running Python in the child, avoiding the unsafe
    post-fork window in the multithreaded API daemon.  ``payload`` carries live
    secrets only through a private pipe -- never argv or a file. The durable
    agent row must already exist; the child performs its own post-terminal
    dispatch. When ``child_reaper`` is supplied, that long-lived owner receives
    the exact PID instead of creating a short-lived waiter thread. The callback
    and wait-status contract is identical in both spawn modes.
    """

    if not isinstance(payload, Mapping):
        raise ValidationError("detached supervisor payload must be a mapping")
    if not isinstance(executable, str) or not executable.strip():
        raise ValidationError("detached supervisor executable must be a nonblank string")
    if not isinstance(module, str) or not module.strip():
        raise ValidationError("detached supervisor module must be a nonblank string")
    if post_reap is not None and not callable(post_reap):
        raise ValidationError("post-reap callback must be callable")
    if cancel_requested is None:
        cancel_requested = getattr(_launch_context, "cancel_requested", None)
    if cancel_requested is not None and not callable(cancel_requested):
        raise ValidationError("launch cancellation predicate must be callable")
    ready_timeout = _positive("readiness_timeout_seconds", readiness_timeout_seconds)
    dispatch_timeout = _positive(
        "post_terminal_timeout_seconds", post_terminal_timeout_seconds
    )
    reap_timeout = _positive("post_reap_timeout_seconds", post_reap_timeout_seconds)
    grace = _positive("cleanup_grace_seconds", cleanup_grace_seconds)
    kill_grace = _positive("cleanup_kill_seconds", cleanup_kill_seconds)
    # Refuses instantly (no fork, no evidence to lose) when this session's
    # own interpreter is already gone -- e.g. its release directory was
    # deleted while this MCP server kept running.
    preflight_executable(executable)

    ready = ReadyChannel.open()
    payload_read, payload_write = os.pipe()
    identity_read, identity_write = os.pipe()
    error_read, error_write = os.pipe()
    os.set_inheritable(payload_read, True)
    os.set_inheritable(identity_write, True)
    os.set_inheritable(error_write, True)
    argv = [
        executable,
        "-m",
        module,
        "--payload-fd",
        str(payload_read),
        "--ready-fd",
        str(ready.write_fd),
        "--identity-fd",
        str(identity_write),
        "--error-fd",
        str(error_write),
    ]
    try:
        # The child derives its own identity from `ps -o command=` on its own
        # pid (see supervisor_identity()): a parent-recorded argv can diverge
        # from what ps reports once exec'd (e.g. a venv python symlink
        # resolves to the real framework binary on macOS), so the parent
        # does not record one here.
        blob = json.dumps(
            {
                **dict(payload),
                "post_terminal_timeout_seconds": dispatch_timeout,
            }
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        for descriptor in (
            payload_read, payload_write, identity_read, identity_write,
            error_read, error_write,
        ):
            _close(descriptor)
        ready.close_read()
        ready.close_write()
        raise ValidationError(f"detached supervisor payload is not JSON: {error}") from error

    try:
        pid = _spawn_session_leader(executable, argv, error_write)
    except OSError as error:
        for descriptor in (
            payload_read, payload_write, identity_read, identity_write,
            error_read, error_write,
        ):
            _close(descriptor)
        ready.close_read()
        ready.close_write()
        raise ValidationError("cannot spawn detached supervisor") from error
    _logger.info("launch spawned pid=%d module=%s", pid, module)

    ready.close_write()
    _close(identity_write)
    _close(payload_read)
    _close(error_write)
    deadline = time.monotonic() + ready_timeout
    fork_started = time.monotonic()
    group: VerifiedProcessGroup | None = None
    try:
        _write_payload(payload_write, blob, deadline, cancel_requested)
        reported_pid = _read_identity(identity_read, deadline, pid=pid, error_fd=error_read)
        if reported_pid != pid:
            raise ValidationError("detached supervisor reported the wrong process identity")
        _logger.debug(
            "launch identity_proven pid=%d duration_ms=%.1f",
            pid, (time.monotonic() - fork_started) * 1000,
        )
        group = verify_process_group(SystemProcessOps(), pid)
        token = _wait_ready(ready, deadline, cancel_requested)
    except ValidationError as error:
        _logger.warning("launch failed pid=%d error_kind=%s", pid, type(error).__name__)
        ready.close_read()
        if time.monotonic() < deadline and _wait_child(pid, dispatch_timeout + 0.25):
            raise error
        if group is None:
            group = verify_process_group(SystemProcessOps(), pid)
        _cleanup(pid, group, grace, kill_grace)
        raise error
    finally:
        _close(identity_read)
        _close(error_read)

    ready.close_read()
    _logger.info(
        "launch ready pid=%d duration_ms=%.1f",
        pid, (time.monotonic() - fork_started) * 1000,
    )
    if child_reaper is None:
        threading.Thread(
            target=_reap_child, args=(pid, post_reap, reap_timeout), daemon=True
        ).start()
    else:
        child_reaper.register(pid, post_reap, reap_timeout)
    return pid


def _wait_ready(
    ready: ReadyChannel,
    deadline: float,
    cancel_requested: Callable[[], bool] | None,
) -> str:
    """Wait for READY while polling cooperative cancellation.

    Predicate failures and cancellation use the launcher's existing verified
    cleanup path, so a detached process group cannot be leaked.
    """

    while True:
        if cancel_requested is not None:
            try:
                cancelled = bool(cancel_requested())
            except Exception as error:
                raise ValidationError("launch cancellation predicate failed") from error
            if cancelled:
                raise ValidationError("detached launch cancelled before supervisor READY")
        remaining = _remaining(deadline)
        readable, _, _ = select.select(
            [ready.read_fd], [], [], min(remaining, _POLL_SECONDS)
        )
        if readable:
            return ready.wait(remaining)


def _spawn_session_leader(executable: str, argv: list[str], error_fd: int) -> int:
    """Spawn ``argv`` as a session leader without Python child execution when supported.

    The preferred ``posix_spawn`` path atomically creates a new session and
    execs the supervisor, avoiding the unsafe post-fork window in a
    multithreaded daemon.  Old Python/platform combinations that explicitly
    report ``setsid`` unsupported use the narrowly retained fork fallback;
    all other spawn errors propagate so an ambiguous creation is never retried.
    ``executable`` and ``argv`` were validated by :func:`launch_detached`;
    ``error_fd`` is its already-inherited bootstrap-evidence pipe.
    """

    spawn = getattr(os, "posix_spawn", None)
    if spawn is not None:
        try:
            return spawn(executable, argv, os.environ, setsid=True)
        except NotImplementedError:
            pass
        except TypeError as error:
            if "setsid" not in str(error):
                raise
    return _fork_session_leader(executable, argv, error_fd)


def _fork_session_leader(executable: str, argv: list[str], error_fd: int) -> int:
    """Use the legacy fork path only when ``posix_spawn(..., setsid=True)`` is unavailable.

    The child invokes only ``setsid``, ``execv``, bounded bootstrap-evidence
    writing to the already-known ``error_fd``, and immediate exit. Callers use
    this compatibility path only after :func:`_spawn_session_leader` has
    established that ``setsid`` spawn support is absent, never after a failed
    or ambiguous spawn attempt.
    """

    pid = os.fork()
    if pid != 0:
        return pid
    try:
        os.setsid()
        os.execv(executable, argv)
    except BaseException as error:
        write_exec_failure(error_fd, error)
        os._exit(1)


def _write_payload(
    fd: int,
    blob: bytes,
    deadline: float,
    cancel_requested: Callable[[], bool] | None,
) -> None:
    """Deliver ``blob`` before ``deadline`` while honoring cancellation.

    ``fd`` is switched to nonblocking mode and always closed. A child that does
    not read cannot hold the launcher past the shared READY deadline.
    """

    try:
        os.set_blocking(fd, False)
        offset = 0
        while offset < len(blob):
            if cancel_requested is not None:
                try:
                    cancelled = bool(cancel_requested())
                except Exception as error:
                    raise ValidationError(
                        "launch cancellation predicate failed"
                    ) from error
                if cancelled:
                    raise ValidationError(
                        "detached launch cancelled before supervisor READY"
                    )
            _, writable, _ = select.select(
                [], [fd], [], min(_remaining(deadline), _POLL_SECONDS)
            )
            if not writable:
                continue
            try:
                offset += os.write(fd, blob[offset:])
            except BlockingIOError:
                continue
    except ValidationError:
        raise
    except OSError as error:
        raise ValidationError(
            "detached supervisor closed the payload channel"
        ) from error
    finally:
        _close(fd)


def _read_identity(fd: int, deadline: float, *, pid: int, error_fd: int) -> int:
    buffer = b""
    while b"\n" not in buffer:
        readable, _, _ = select.select([fd], [], [], _remaining(deadline))
        if not readable:
            continue
        chunk = os.read(fd, 64)
        if not chunk:
            raise diagnose_bootstrap_failure(pid, error_fd)
        buffer += chunk
    raw = buffer.split(b"\n", 1)[0]
    try:
        pid = int(raw)
    except ValueError as error:
        raise ValidationError("detached supervisor reported an invalid process identity") from error
    if pid <= 1:
        raise ValidationError("detached supervisor reported an unsafe process identity")
    return pid


def _cleanup(
    pid: int,
    group: VerifiedProcessGroup | None,
    grace_seconds: float,
    kill_grace_seconds: float,
) -> None:
    if group is None:
        if _wait_child(pid, _POLL_SECONDS):
            return
        raise ValidationError("detached supervisor could not be verified for cleanup")
    result = terminate_process_group(
        SystemProcessOps(),
        group,
        grace_seconds=grace_seconds,
        kill_grace_seconds=kill_grace_seconds,
        poll_seconds=_POLL_SECONDS,
    )
    reaped = _wait_child(pid, grace_seconds + kill_grace_seconds + 0.25)
    if not result.group_gone or not reaped:
        raise ValidationError("detached supervisor process group survived cleanup")


def _wait_child(pid: int, timeout_seconds: float) -> bool:
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    while True:
        try:
            waited, _status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return True
        if waited == pid:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(_POLL_SECONDS)


def _reap_child(
    pid: int,
    callback: Callable[[int, int], object] | None = None,
    timeout_seconds: float = DEFAULT_REAP_TIMEOUT_SECONDS,
) -> None:
    try:
        waited, status = os.waitpid(pid, 0)
    except ChildProcessError:
        return
    if waited != pid:
        return
    if callback is None:
        return
    _invoke_reap_callback(callback, pid, status, timeout_seconds)


def _invoke_reap_callback(
    callback: Callable[[int, int], object],
    pid: int,
    status: int,
    timeout_seconds: float,
) -> None:
    """Run one post-reap callback in a bounded helper thread.

    Callback exceptions and callback timeouts are deliberately best-effort:
    the child is already reaped and its exact PID/status evidence must not be
    converted into a second lifecycle failure.
    """

    def invoke() -> None:
        with suppress(Exception):
            callback(pid, status)

    worker = threading.Thread(target=invoke, daemon=True)
    worker.start()
    worker.join(timeout_seconds)


def _positive(name: str, value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{name} must be positive and finite")
    number = float(value)
    if not 0 < number < float("inf"):
        raise ValidationError(f"{name} must be positive and finite")
    return number


def _remaining(deadline: float) -> float:
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise ValidationError("supervisor did not report ready in time")
    return remaining


def _close(fd: int) -> None:
    with suppress(OSError):
        os.close(fd)
