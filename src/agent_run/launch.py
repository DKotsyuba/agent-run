"""Detach one accepted supervisor and wait only for its readiness proof."""

from __future__ import annotations

import os
import select
import signal
import threading
import time
from collections.abc import Callable
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
DEFAULT_CLEANUP_GRACE_SECONDS = 1.0
DEFAULT_CLEANUP_KILL_SECONDS = 1.0
_POLL_SECONDS = 0.01


def launch_detached(
    child: Callable[[ReadyChannel], object],
    *,
    readiness_timeout_seconds: float = DEFAULT_READY_TIMEOUT_SECONDS,
    post_terminal: Callable[[], object] | None = None,
    post_terminal_timeout_seconds: float = DEFAULT_DISPATCH_TIMEOUT_SECONDS,
    cleanup_grace_seconds: float = DEFAULT_CLEANUP_GRACE_SECONDS,
    cleanup_kill_seconds: float = DEFAULT_CLEANUP_KILL_SECONDS,
) -> int:
    """Fork one session leader and return its pid after ``READY_TOKEN`` only.

    ``child`` owns construction and execution of the accepted Supervisor. The
    durable agent row must already exist. ``post_terminal`` is attempted once
    after the child returns or raises and is bounded independently.
    """

    if not callable(child):
        raise ValidationError("detached child callback must be callable")
    if post_terminal is not None and not callable(post_terminal):
        raise ValidationError("post-terminal callback must be callable")
    ready_timeout = _positive("readiness_timeout_seconds", readiness_timeout_seconds)
    dispatch_timeout = _positive(
        "post_terminal_timeout_seconds", post_terminal_timeout_seconds
    )
    grace = _positive("cleanup_grace_seconds", cleanup_grace_seconds)
    kill_grace = _positive("cleanup_kill_seconds", cleanup_kill_seconds)

    ready = ReadyChannel.open()
    identity_read, identity_write = os.pipe()
    try:
        pid = os.fork()
    except OSError as error:
        ready.close_read()
        ready.close_write()
        _close(identity_read)
        _close(identity_write)
        raise ValidationError("cannot fork detached supervisor") from error

    if pid == 0:
        _child_main(
            ready,
            identity_read,
            identity_write,
            child,
            post_terminal,
            dispatch_timeout,
        )

    ready.close_write()
    _close(identity_write)
    deadline = time.monotonic() + ready_timeout
    group: VerifiedProcessGroup | None = None
    try:
        reported_pid = _read_identity(identity_read, deadline)
        if reported_pid != pid:
            raise ValidationError("detached supervisor reported the wrong process identity")
        group = verify_process_group(SystemProcessOps(), pid)
        if group is None:
            raise ValidationError("detached supervisor exited before process-group proof")
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
    threading.Thread(target=_reap_child, args=(pid,), daemon=True).start()
    return pid


def _child_main(
    ready: ReadyChannel,
    identity_read: int,
    identity_write: int,
    child: Callable[[ReadyChannel], object],
    post_terminal: Callable[[], object] | None,
    dispatch_timeout: float,
) -> None:
    ready.close_read()
    _close(identity_read)
    try:
        os.setsid()
        pid = os.getpid()
        if os.getpgrp() != pid:
            raise ValidationError("detached child is not its process-group leader")
        os.write(identity_write, f"{pid}\n".encode("ascii"))
    except BaseException as error:
        _close(identity_write)
        ready.failed(_failure_reason(error))
        os._exit(1)
    _close(identity_write)

    exit_code = 0
    try:
        _redirect_standard_streams()
        child(ready)
    except BaseException as error:
        ready.failed(_failure_reason(error))
        exit_code = 1
    finally:
        ready.close_write()
        if post_terminal is not None:
            try:
                _bounded(post_terminal, dispatch_timeout)
            except BaseException:
                exit_code = 1
        os._exit(exit_code)


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


def _bounded(callback: Callable[[], object], timeout_seconds: float) -> None:
    def expired(_number: int, _frame: object) -> None:
        raise TimeoutError("post-terminal callback exceeded its deadline")

    previous = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        callback()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


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


def _reap_child(pid: int) -> None:
    with suppress(ChildProcessError):
        os.waitpid(pid, 0)


def _redirect_standard_streams() -> None:
    descriptor = os.open(os.devnull, os.O_RDWR)
    try:
        for target in (0, 1, 2):
            os.dup2(descriptor, target)
    finally:
        if descriptor > 2:
            os.close(descriptor)


def _failure_reason(error: BaseException) -> str:
    detail = str(error).strip()
    return (f"{type(error).__name__}: {detail}" if detail else type(error).__name__)[:300]


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
