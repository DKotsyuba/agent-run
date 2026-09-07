"""PID birth-time observations used to avoid acting on reused processes."""

from dataclasses import dataclass
from enum import StrEnum

import psutil


class ProcessState(StrEnum):
    """The result of comparing a PID with optional stored birth evidence."""

    ALIVE = "alive"
    DEAD = "dead"
    REUSED = "reused"
    UNKNOWN = "unknown"
    DENIED = "denied"


@dataclass(frozen=True)
class ProcessObservation:
    """A safe PID/birth-time verdict without command-line identity authority."""

    state: ProcessState
    create_time: float | None = None


def observe_process(pid: int, birth_time: float | None) -> ProcessObservation:
    """Compare ``pid`` to stored birth time without treating missing proof as death.

    ``NoSuchProcess`` means dead, access denial remains denied, and any unavailable
    birth observation remains unknown. A different observed creation time proves
    PID reuse; callers must never signal or reconcile that PID as their owner.
    """
    try:
        process = psutil.Process(pid)
        observed = process.create_time()
    except psutil.NoSuchProcess:
        return ProcessObservation(ProcessState.DEAD)
    except psutil.AccessDenied:
        return ProcessObservation(ProcessState.DENIED)
    except psutil.Error:
        return ProcessObservation(ProcessState.UNKNOWN)
    if birth_time is None:
        return ProcessObservation(ProcessState.UNKNOWN, observed)
    if observed == birth_time:
        return ProcessObservation(ProcessState.ALIVE, observed)
    return ProcessObservation(ProcessState.REUSED, observed)


def capture_process_birth(pid: int) -> float | None:
    """Return PID creation time when readable, otherwise no birth proof."""
    return observe_process(pid, None).create_time
