"""Bounded transient-thread coordination for accepted agent starts."""

from __future__ import annotations

import logging
import threading
from collections.abc import Callable
from pathlib import Path

from .domain import AgentId, validate_agent_id
from .errors import ValidationError
from .state.store import StateStore


_logger = logging.getLogger("agent_run.start")
StartWork = Callable[[StateStore, threading.Event], None]


class StartCoordinator:
    """Run accepted starts on bounded, per-agent transient threads.

    The coordinator retains the database path rather than a SQLite connection.
    Every worker opens and closes its own thread-affine :class:`StateStore`.
    Each agent id has at most one registered worker, while ``max_workers``
    bounds the number of live start threads. Closing signals every registered
    worker and waits a bounded interval for cooperative cancellation.
    """

    def __init__(
        self,
        database_path: str | Path,
        *,
        max_workers: int,
        close_timeout_seconds: float = 5.0,
    ) -> None:
        """Create a coordinator for one database and bounded worker count.

        ``database_path`` identifies the existing state database each worker
        opens on its own thread. ``max_workers`` must be positive.
        ``close_timeout_seconds`` is the maximum join time for each worker.
        """

        if type(max_workers) is not int or max_workers < 1:
            raise ValidationError("max_workers must be a positive integer")
        if (
            isinstance(close_timeout_seconds, bool)
            or not isinstance(close_timeout_seconds, (int, float))
            or close_timeout_seconds <= 0
        ):
            raise ValidationError("close_timeout_seconds must be positive")
        self._database_path = Path(database_path)
        self._max_workers = max_workers
        self._close_timeout_seconds = float(close_timeout_seconds)
        self._lock = threading.Lock()
        self._workers: dict[AgentId, tuple[threading.Thread, threading.Event]] = {}
        self._closed = False

    def submit(self, agent_id: str | AgentId, work: StartWork) -> bool:
        """Start ``work`` once and report whether it was newly registered.

        ``work`` receives a worker-owned store and the per-agent cancellation
        event. Duplicate live registrations return ``False``. Closed or
        saturated coordinators raise :class:`ValidationError`.
        """

        checked = validate_agent_id(agent_id)
        if not callable(work):
            raise ValidationError("start work must be callable")
        with self._lock:
            if self._closed:
                raise ValidationError("start coordinator is closed")
            if checked in self._workers:
                return False
            if len(self._workers) >= self._max_workers:
                raise ValidationError("start coordinator worker limit reached")
            cancelled = threading.Event()
            thread = threading.Thread(
                target=self._run,
                args=(checked, cancelled, work),
                name=f"agent-start-{checked}",
                daemon=True,
            )
            self._workers[checked] = (thread, cancelled)
            thread.start()
        return True

    def cancel(self, agent_id: str | AgentId) -> bool:
        """Signal a registered pre-ownership worker, if one still exists.

        The return value is ``True`` when a live registration was signalled and
        ``False`` after ownership transferred or before registration.
        """

        checked = validate_agent_id(agent_id)
        with self._lock:
            worker = self._workers.get(checked)
            if worker is None:
                return False
            worker[1].set()
            return True

    def close(self) -> None:
        """Reject new work, signal current workers, and join them boundedly."""

        with self._lock:
            self._closed = True
            workers = tuple(self._workers.values())
            for _thread, cancelled in workers:
                cancelled.set()
        for thread, _cancelled in workers:
            thread.join(self._close_timeout_seconds)

    def _run(
        self,
        agent_id: AgentId,
        cancelled: threading.Event,
        work: StartWork,
    ) -> None:
        """Open the thread-owned store, execute one start, and unregister it."""

        store: StateStore | None = None
        try:
            store = StateStore.open(self._database_path)
            work(store, cancelled)
        except BaseException:
            _logger.exception("agent_id=%s asynchronous start worker crashed", agent_id)
        finally:
            if store is not None:
                store.close()
            with self._lock:
                current = self._workers.get(agent_id)
                if current is not None and current[0] is threading.current_thread():
                    self._workers.pop(agent_id, None)
