"""Detached lifecycle primitives: readiness, deadlines, process groups."""

from __future__ import annotations

import math
import os
import select
import signal
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Mapping, Protocol

from .errors import ValidationError


READY_TOKEN = "ready"
FAILURE_PREFIX = "fail:"
STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def _positive(name: str, value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{name} must be positive and finite")
    if value <= 0 or not math.isfinite(value):
        raise ValidationError(f"{name} must be positive and finite")
    return float(value)


def _nonnegative(name: str, value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"{name} must be nonnegative and finite")
    if value < 0 or not math.isfinite(value):
        raise ValidationError(f"{name} must be nonnegative and finite")
    return float(value)


def checked_pgid(value: int) -> int:
    """Reject the wildcard and self-group ids that would signal our own tree."""

    if isinstance(value, bool) or not isinstance(value, int) or value <= 1:
        raise ValidationError("process group id must be an integer above 1")
    return value


class Phase(str, Enum):
    RUNNING = "running"
    WARNING = "warning"
    EXPIRED = "expired"


@dataclass(frozen=True)
class Deadline:
    """Monotonic budget with a single warning point before the hard stop."""

    started_at: float
    timeout_seconds: float
    warning_fraction: float = 0.90

    def __post_init__(self) -> None:
        _nonnegative("started_at", self.started_at)
        _positive("timeout_seconds", self.timeout_seconds)
        fraction = self.warning_fraction
        if isinstance(fraction, bool) or not isinstance(fraction, (int, float)):
            raise ValidationError("warning_fraction must be within (0, 1]")
        if not math.isfinite(fraction) or not 0 < fraction <= 1:
            raise ValidationError("warning_fraction must be within (0, 1]")

    @property
    def warning_at(self) -> float:
        return self.started_at + self.timeout_seconds * self.warning_fraction

    @property
    def expires_at(self) -> float:
        return self.started_at + self.timeout_seconds

    def remaining(self, now: float) -> float:
        return max(0.0, self.expires_at - now)

    def phase(self, now: float) -> Phase:
        if now >= self.expires_at:
            return Phase.EXPIRED
        if now >= self.warning_at:
            return Phase.WARNING
        return Phase.RUNNING


class ProcessOps(Protocol):
    """Injectable process and clock surface so supervision is testable."""

    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...

    def process_group(self, pid: int) -> int | None: ...

    def signal_group(self, pgid: int, signal_number: int) -> bool: ...

    def group_alive(self, pgid: int) -> bool: ...

    def reap(self, pid: int) -> int | None: ...


class SystemProcessOps:
    """Real clock and POSIX process-group control."""

    def monotonic(self) -> float:
        return time.monotonic()

    def sleep(self, seconds: float) -> None:
        time.sleep(max(0.0, seconds))

    def process_group(self, pid: int) -> int | None:
        try:
            return os.getpgid(checked_pgid(pid))
        except ProcessLookupError:
            return None
        except PermissionError as error:
            raise ValidationError(f"cannot inspect process group for {pid}") from error

    def signal_group(self, pgid: int, signal_number: int) -> bool:
        try:
            os.killpg(checked_pgid(pgid), signal_number)
        except ProcessLookupError:
            return False
        except PermissionError as error:
            raise ValidationError(f"cannot signal process group {pgid}") from error
        return True

    def group_alive(self, pgid: int) -> bool:
        try:
            os.killpg(checked_pgid(pgid), 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            # POSIX: signal 0 answering EPERM means the group exists but is
            # not ours to signal (e.g. the pid was reused by a foreign
            # process). For a liveness probe that is "exists"; raising here
            # let one stale agent row abort unrelated starts in the resident
            # daemon.
            return True
        return True

    def reap(self, pid: int) -> int | None:
        try:
            waited, status = os.waitpid(pid, os.WNOHANG)
        except (ChildProcessError, OSError):
            return None
        return None if waited == 0 else status


@dataclass(frozen=True)
class Termination:
    """Evidence that a process group was asked to stop and observed to be gone."""

    signals: tuple[str, ...]
    group_gone: bool
    waited_seconds: float


@dataclass(frozen=True)
class VerifiedProcessGroup:
    """A leader-to-group identity proved before any group signal is allowed."""

    leader_pid: int
    pgid: int


def verify_process_group(
    ops: ProcessOps, leader_pid: int
) -> VerifiedProcessGroup | None:
    leader_pid = checked_pgid(leader_pid)
    observed = ops.process_group(leader_pid)
    if observed is None:
        return None
    if observed != leader_pid:
        raise ValidationError(
            f"engine pid {leader_pid} is not its process group leader"
        )
    return VerifiedProcessGroup(leader_pid, observed)


def _await_group_exit(
    ops: ProcessOps,
    group: VerifiedProcessGroup,
    budget: float,
    poll_seconds: float,
) -> tuple[bool, float]:
    started = ops.monotonic()
    while True:
        ops.reap(group.leader_pid)
        if not ops.group_alive(group.pgid):
            return True, ops.monotonic() - started
        waited = ops.monotonic() - started
        if waited >= budget:
            return False, waited
        ops.sleep(min(poll_seconds, budget - waited))


def terminate_process_group(
    ops: ProcessOps,
    group: VerifiedProcessGroup | None,
    *,
    natural_grace_seconds: float = 0.0,
    owned_pid: int | None = None,
    grace_seconds: float = 10.0,
    kill_grace_seconds: float = 5.0,
    poll_seconds: float = 0.05,
) -> Termination:
    """TERM the whole group, escalate to KILL, and verify nothing survives.

    Signalling the group rather than the engine pid is what removes wrapper
    grandchildren; the returned evidence says whether the group is really gone.
    Without a verified group this function never signals. If ``owned_pid`` is
    supplied, ``gone`` requires both an absent leader PID and no live group at
    that PGID; with neither group nor owned PID there is no owned process to stop.

    ``ops`` supplies ProcessOps; ``group`` is a VerifiedProcessGroup or None,
    and ``owned_pid`` is an optional int leader PID. Duration floats are seconds:
    natural grace may be zero, while termination graces and polling must be
    positive. Returns Termination with sent signals, group absence and elapsed
    seconds. Invalid durations/PIDs raise ValidationError; process errors propagate.
    """

    natural_grace_seconds = _nonnegative(
        "natural_grace_seconds", natural_grace_seconds
    )
    grace_seconds = _positive("grace_seconds", grace_seconds)
    kill_grace_seconds = _positive("kill_grace_seconds", kill_grace_seconds)
    poll_seconds = _positive("poll_seconds", poll_seconds)
    started = ops.monotonic()
    if group is None:
        gone = owned_pid is None or (
            ops.process_group(checked_pgid(owned_pid)) is None
            and not ops.group_alive(checked_pgid(owned_pid))
        )
        return Termination((), gone, ops.monotonic() - started)

    gone, _ = _await_group_exit(
        ops, group, natural_grace_seconds, poll_seconds
    )
    if gone:
        return Termination((), True, ops.monotonic() - started)

    sent: list[str] = []
    if ops.signal_group(group.pgid, signal.SIGTERM):
        sent.append("SIGTERM")
    gone, _ = _await_group_exit(ops, group, grace_seconds, poll_seconds)
    if not gone:
        if ops.signal_group(group.pgid, signal.SIGKILL):
            sent.append("SIGKILL")
        gone, _ = _await_group_exit(
            ops, group, kill_grace_seconds, poll_seconds
        )
    return Termination(tuple(sent), gone, ops.monotonic() - started)


def install_signal_handlers(
    handler: Callable[[int], None],
    *,
    signals: tuple[int, ...] = STOP_SIGNALS,
) -> Mapping[int, object]:
    """Install stop handlers, returning the previous dispositions for restore.

    Off the main thread the interpreter refuses handlers; the caller still gets
    an empty mapping so a supervisor embedded in tests keeps working.
    """

    if threading.current_thread() is not threading.main_thread():
        return {}
    previous: dict[int, object] = {}
    for number in signals:
        previous[number] = signal.getsignal(number)
        signal.signal(number, lambda received, _frame: handler(received))
    return previous


def restore_signal_handlers(previous: Mapping[int, object]) -> None:
    if threading.current_thread() is not threading.main_thread():
        return
    for number, disposition in previous.items():
        if disposition is not None:
            signal.signal(number, disposition)


class ReadyChannel:
    """One-shot pipe a detached supervisor uses to prove it is supervising.

    The parent may return an agent id only after `wait` observes the token, so
    a cancel that arrives immediately afterwards can never race handler setup.
    """

    def __init__(self, read_fd: int, write_fd: int):
        self.read_fd = read_fd
        self.write_fd = write_fd
        self._reported = False

    @classmethod
    def open(cls) -> ReadyChannel:
        read_fd, write_fd = os.pipe()
        os.set_inheritable(write_fd, True)
        return cls(read_fd, write_fd)

    @classmethod
    def from_write_fd(cls, write_fd: int) -> ReadyChannel:
        """The exec'd supervisor inherits only the write end; the wire is the same."""

        if isinstance(write_fd, bool) or not isinstance(write_fd, int) or write_fd < 0:
            raise ValidationError("ready write fd must be a nonnegative integer")
        return cls(-1, write_fd)

    def ready(self) -> None:
        self._report(READY_TOKEN)

    def failed(self, reason: str) -> None:
        if not isinstance(reason, str) or not reason.strip():
            raise ValidationError("failure reason must be a nonblank string")
        self._report(f"{FAILURE_PREFIX}{reason.strip()}")

    def _report(self, token: str) -> None:
        if self._reported:
            return
        self._reported = True
        try:
            os.write(self.write_fd, f"{token}\n".encode("utf-8"))
        except OSError:
            pass
        finally:
            self.close_write()

    def close_write(self) -> None:
        try:
            os.close(self.write_fd)
        except OSError:
            pass

    def close_read(self) -> None:
        try:
            os.close(self.read_fd)
        except OSError:
            pass

    def wait(self, timeout_seconds: float) -> str:
        """Block until the child reports; raise when it fails, dies, or stalls."""

        timeout_seconds = _positive("timeout_seconds", timeout_seconds)
        deadline = time.monotonic() + timeout_seconds
        buffer = b""
        while b"\n" not in buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValidationError("supervisor did not report ready in time")
            readable, _, _ = select.select([self.read_fd], [], [], remaining)
            if not readable:
                continue
            chunk = os.read(self.read_fd, 256)
            if not chunk:
                raise ValidationError("supervisor exited before reporting ready")
            buffer += chunk
        token = buffer.split(b"\n", 1)[0].decode("utf-8", "replace")
        if token == READY_TOKEN:
            return token
        if token.startswith(FAILURE_PREFIX):
            raise ValidationError(f"supervisor failed to start: {token[len(FAILURE_PREFIX):]}")
        raise ValidationError(f"unexpected supervisor readiness token: {token!r}")
