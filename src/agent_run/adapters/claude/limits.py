"""Live rate-limit sampling for the ``claude`` runtime.

The claude CLI never exposes a quota endpoint, so the only evidence of a
usage window is the ``rate_limit_event`` line it emits on its own stdout.
``ClaudeSession`` persists every sanitized stdout line to the agent's
``runtime.jsonl``; this module reads the newest of those back.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path

from ..base import LimitSample

__all__ = ["agent_rate_limit_samples"]


_LIMITS_STALE_SECONDS = 900
_AGENT_RUNTIME_FILES = 24
_RUNTIME_TAIL_BYTES = 262_144
_RUNTIME_TAIL_LINES = 2_048


def _timestamp(value: object) -> datetime | None:
    """Convert an epoch second to UTC; anything unrepresentable is unknown."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value):
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _tail_lines(path: Path) -> tuple[str, ...]:
    """Read a bounded complete-line tail from one agent's runtime stream."""

    with path.open("rb") as stream:
        stream.seek(0, 2)
        end = stream.tell()
        stream.seek(max(0, end - _RUNTIME_TAIL_BYTES))
        data = stream.read(_RUNTIME_TAIL_BYTES)
    if end > _RUNTIME_TAIL_BYTES:
        data = data.split(b"\n", 1)[-1]
    return tuple(data.decode("utf-8").splitlines()[-_RUNTIME_TAIL_LINES:])


def agent_rate_limit_samples(home: Path, now: float) -> tuple[LimitSample, ...]:
    """Return the newest ``rate_limit_event`` samples from sibling agent dirs.

    In stream-json mode the claude CLI emits a ``rate_limit_event`` line on
    stdout; ``ClaudeSession._read_stdout`` already persists every sanitized
    stdout line verbatim to each agent's ``runtime.jsonl`` below the shared
    agent-run home (``<agent_run_home>/agents/<agent_id>/runtime.jsonl``),
    three levels above this adapter's own generated
    ``<agent_run_home>/runtimes/claude/home``. This scans the newest such
    files for the latest event; anything unreadable, malformed, or older
    than ``_LIMITS_STALE_SECONDS`` yields ``unknown`` per the M009 contract.
    """

    try:
        agents_root = Path(home).resolve(strict=True).parents[2] / "agents"
        if agents_root.is_symlink():
            return ()
        agents_root = agents_root.resolve(strict=True)
        paths = agents_root.glob("*/runtime.jsonl")
    except (IndexError, OSError, RuntimeError, ValueError):
        return ()

    newest: list[tuple[float, str, Path]] = []
    try:
        for path in paths:
            try:
                if path.is_symlink():
                    continue
                resolved = path.resolve(strict=True)
                resolved.relative_to(agents_root)
                if not resolved.is_file():
                    continue
                candidate = (resolved.stat().st_mtime, str(resolved), resolved)
                newest = sorted((*newest, candidate), reverse=True)[:_AGENT_RUNTIME_FILES]
            except (OSError, RuntimeError, ValueError):
                continue
    except OSError:
        pass

    for mtime, _name, path in newest:
        try:
            lines = _tail_lines(path)
        except (OSError, UnicodeError, ValueError):
            continue
        stale = now - mtime > _LIMITS_STALE_SECONDS
        observed_at = _timestamp(mtime)
        for line in reversed(lines):
            if '"rate_limit_event"' not in line:
                continue
            try:
                event = json.loads(line)
            except (TypeError, ValueError):
                continue
            if not isinstance(event, dict) or event.get("type") != "rate_limit_event":
                continue
            info = event.get("rate_limit_info")
            windows = info.get("unifiedWindows") if isinstance(info, dict) else None
            if not isinstance(windows, dict):
                continue
            samples = []
            for window_name in sorted(windows):
                window = windows[window_name]
                if not isinstance(window, dict):
                    continue
                utilization = window.get("utilization")
                if (
                    isinstance(utilization, bool)
                    or not isinstance(utilization, (int, float))
                    or not math.isfinite(utilization)
                ):
                    continue
                samples.append(
                    LimitSample(
                        lane="usage",
                        window=window_name,
                        remaining_percent=None
                        if stale
                        else max(0.0, min(100.0, (1.0 - float(utilization)) * 100.0)),
                        reset_at=_timestamp(window.get("resetsAt")),
                        observed_at=observed_at,
                        source="unknown" if stale else "runtime_stream_evidence",
                    )
                )
            if samples:
                return tuple(samples)
    return ()
