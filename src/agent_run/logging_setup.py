"""Central, dense, process-wide logging.

Owner directive: until agent-run is debugged, logs must be dense and cover
everything happening in the system, because the pre-existing per-agent and
per-worker logs are not enough to reconstruct an incident timeline.  Every
entrypoint calls :func:`configure_logging` once; every module below it logs
through ``logging.getLogger("agent_run.<area>")`` at DEBUG (detail), INFO
(lifecycle), or WARNING/ERROR (failure).
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

#: Rotation policy: dense-by-default logging fills files fast, so keep a
#: bounded, predictable footprint per component rather than growing forever.
_MAX_BYTES = 10 * 1024 * 1024
_BACKUP_COUNT = 5
_ENV_LEVEL = "AGENT_RUN_LOG_LEVEL"
_DEFAULT_LEVEL = "DEBUG"
_FORMAT = "%(asctime)s %(levelname)s %(component)s %(process)d %(name)s %(message)s"

_configured = False


class _UtcFormatter(logging.Formatter):
    """Formatter that renders ``record.created`` as ISO-8601 UTC with millisecond precision."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        moment = datetime.fromtimestamp(record.created, tz=timezone.utc)
        return moment.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(record.msecs):03d}Z"


class _ComponentFilter(logging.Filter):
    """Attaches a fixed ``component`` field to every record passing through the handler."""

    def __init__(self, component: str) -> None:
        super().__init__()
        self._component = component

    def filter(self, record: logging.LogRecord) -> bool:
        record.component = self._component
        return True


def _resolve_level() -> int:
    """Read :data:`_ENV_LEVEL` and fall back to :data:`_DEFAULT_LEVEL` for anything unrecognized."""

    name = os.environ.get(_ENV_LEVEL, _DEFAULT_LEVEL).strip().upper()
    level = logging.getLevelName(name)
    return level if isinstance(level, int) else logging.DEBUG


def _build_handler(home: str | Path, component: str) -> logging.Handler:
    """Open the rotating file handler for ``component``, or a stderr fallback on any OSError.

    A log directory that cannot be created or opened (permissions, a full
    disk, ``home`` not yet existing) must never stop agent-run from running;
    it only means this process logs to stderr instead of the central file.
    """

    try:
        log_dir = Path(home) / "logs"
        log_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
        return logging.handlers.RotatingFileHandler(
            log_dir / f"{component}.log",
            maxBytes=_MAX_BYTES,
            backupCount=_BACKUP_COUNT,
            encoding="utf-8",
        )
    except OSError:
        return logging.StreamHandler(sys.stderr)


def configure_logging(home: str | Path, component: str) -> logging.Logger:
    """Attach one dense, rotating handler to the process root logger.

    ``home`` is the resolved agent-run home; the handler writes to
    ``<home>/logs/<component>.log`` (created if missing) unless that fails,
    in which case it silently falls back to stderr. The level comes from
    :data:`_ENV_LEVEL` (``AGENT_RUN_LOG_LEVEL``), defaulting to ``DEBUG`` so
    logging is dense until the operator narrows it. Safe to call more than
    once per process: only the first call attaches a handler, so repeated
    calls (e.g. from shared code paths) are idempotent and do not duplicate
    log lines or clobber an already-configured level.

    Returns the process root :class:`logging.Logger`.
    """

    global _configured
    root = logging.getLogger()
    if _configured:
        return root
    handler = _build_handler(home, component)
    handler.setFormatter(_UtcFormatter(_FORMAT))
    handler.addFilter(_ComponentFilter(component))
    root.addHandler(handler)
    root.setLevel(_resolve_level())
    _configured = True
    return root


def _reset_for_tests() -> None:
    """Undo :func:`configure_logging`'s process-wide state so tests can reconfigure it.

    Not part of the public contract; only :mod:`tests.test_logging_setup`
    should call this, between cases that each need a fresh handler.
    """

    global _configured
    root = logging.getLogger()
    for handler in list(root.handlers):
        root.removeHandler(handler)
        handler.close()
    root.setLevel(logging.WARNING)
    _configured = False
