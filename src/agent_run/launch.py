"""Detach one accepted supervisor and wait only for its readiness proof."""

from __future__ import annotations

import json
import os
import select
import threading
import time
from collections.abc import Callable, Mapping
from contextlib import suppress

from .errors import ValidationError
from .lifecycle import (
    ReadyChannel,
    SystemProcessOps,
    VerifiedProcessGroup,
    terminate_process_group,
    verify_process_group,
)


DEFAULT_READY_TIMEOUT_SECONDS = 5.0
DEFAULT_DISPATCH_TIMEOUT_SECONDS = 5.0
DEFAULT_REAP_TIMEOUT_SECONDS = 5.0
DEFAULT_CLEANUP_GRACE_SECONDS = 1.0
DEFAULT_CLEANUP_KILL_SECONDS = 1.0
SUPERVISOR_MODULE = "agent_run.supervisor_main"
_POLL_SECONDS = 0.01


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
) -> int:
    """Fork one session leader, exec `module`, and return its pid after READY.

    The child execs a fresh interpreter before touching anything: on macOS a
    forked child of a process that has used SQLite segfaults inside
    ``sqlite3.connect`` roughly half the time, and the CLI always has
    ``state.db`` open here. ``payload`` carries live secrets, so it crosses only
    the private pipe -- never argv, never a file. The durable agent row must
    already exist; the child performs its own post-terminal dispatch.
    """

    if not isinstance(payload, Mapping):
        raise ValidationError("detached supervisor payload must be a mapping")
    if not isinstance(executable, str) or not executable.strip():
        raise ValidationError("detached supervisor executable must be a nonblank string")
    if not isinstance(module, str) or not module.strip():
        raise ValidationError("detached supervisor module must be a nonblank string")
    if post_reap is not None and not callable(post_reap):
        raise ValidationError("post-reap callback must be callable")
    ready_timeout = _positive("readiness_timeout_seconds", readiness_timeout_seconds)
    dispatch_timeout = _positive(
        "post_terminal_timeout_seconds", post_terminal_timeout_seconds
    )
    reap_timeout = _positive("post_reap_timeout_seconds", post_reap_timeout_seconds)
    grace = _positive("cleanup_grace_seconds", cleanup_grace_seconds)
    kill_grace = _positive("cleanup_kill_seconds", cleanup_kill_seconds)

    ready = ReadyChannel.open()
    payload_read, payload_write = os.pipe()
    identity_read, identity_write = os.pipe()
    os.set_inheritable(payload_read, True)
    os.set_inheritable(identity_write, True)
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
        for descriptor in (payload_read, payload_write, identity_read, identity_write):
            _close(descriptor)
        ready.close_read()
        ready.close_write()
        raise ValidationError(f"detached supervisor payload is not JSON: {error}") from error

    try:
        pid = os.fork()
    except OSError as error:
        for descriptor in (payload_read, payload_write, identity_read, identity_write):
            _close(descriptor)
        ready.close_read()
        ready.close_write()
        raise ValidationError("cannot fork detached supervisor") from error

    if pid == 0:
        # Nothing but setsid and execv may run here: any inherited library state
        # touched before the exec is exactly what crashes the child.
        try:
            os.setsid()
            os.execv(executable, argv)
        except BaseException:
            os._exit(1)

    ready.close_write()
    _close(identity_write)
    _close(payload_read)
    deadline = time.monotonic() + ready_timeout
    group: VerifiedProcessGroup | None = None
    try:
        _write_payload(payload_write, blob)
        reported_pid = _read_identity(identity_read, deadline)
        if reported_pid != pid:
            raise ValidationError("detached supervisor reported the wrong process identity")
        group = verify_process_group(SystemProcessOps(), pid)
        token = ready.wait(_remaining(deadline))
    except ValidationError as error:
        ready.close_read()
        if time.monotonic() < deadline and _wait_child(pid, dispatch_timeout + 0.25):
            raise ValidationError(str(error)) from error
        if group is None:
            group = verify_process_group(SystemProcessOps(), pid)
        _cleanup(pid, group, grace, kill_grace)
        raise ValidationError(str(error)) from error
    finally:
        _close(identity_read)

    ready.close_read()
    threading.Thread(
        target=_reap_child, args=(pid, post_reap, reap_timeout), daemon=True
    ).start()
    return pid


def _write_payload(fd: int, blob: bytes) -> None:
    """Hand the child its payload; a child that died before exec closes the pipe."""

    try:
        offset = 0
        while offset < len(blob):
            offset += os.write(fd, blob[offset:])
    except OSError as error:
        raise ValidationError("detached supervisor closed the payload channel") from error
    finally:
        _close(fd)


def _read_identity(fd: int, deadline: float) -> int:
    buffer = b""
    while b"\n" not in buffer:
        readable, _, _ = select.select([fd], [], [], _remaining(deadline))
        if not readable:
            continue
        chunk = os.read(fd, 64)
        if not chunk:
            raise ValidationError("detached supervisor exited before session proof")
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
