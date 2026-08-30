"""The OmniRoute account pool's quota, read from OmniRoute's own store.

Every model the OmniRoute-served runtimes (opencode, qwen) can reach is
served by OmniRoute out of one ``opencode-go`` account pool, so the pool's
quota *is* each such runtime's quota.

That store is the only real source there is. Probed live against the local
server (2026-08-27):

* the OpenAI-compatible surface answers an explicit 404 ``Unknown API route``
  for ``/v1/usage``, ``/v1/quota``, ``/v1/limits`` and
  ``/v1/organization/usage``;
* ``GET /v1/models`` (200) and a real ``POST /v1/chat/completions`` both come
  back with no ``x-ratelimit-*``, ``x-quota-*`` or ``retry-after`` header at
  all -- only ``x-omniroute-route-class``, ``-selected-connection-id`` and
  ``-session-id``;
* the whole ``/api/*`` admin surface answers 401 ``AUTH_001`` for *every*
  path, invented ones included, so a 401 there is a catch-all and is no
  evidence that anything exists behind it;
* agent transcripts carry no opencode provider rate-limit evidence either:
  the ``rate_limit_info`` events under ``<home>/agents/*/runtime.jsonl`` are
  the claude runtime's own.

What the server does keep is sqlite: ``quota_snapshots`` holds
``remaining_percentage`` and ``next_reset_at`` per connection per window,
refreshed by OmniRoute's own provider sync (roughly every 70 minutes, so a
sample is routinely older than the M009 freshness bound and is then reported
as unknown -- which is the honest answer, not a missing feature).

OmniRoute now runs inside a docker container (OrbStack) whose volume is not
readable from the host, so the store is queried in place: the query runs
inside the container against ``/app/data/storage.sqlite`` via the sqlite
driver OmniRoute itself ships with (verified live 2026-08-28: the old host
copy ``~/.omniroute/storage.sqlite`` is a stale pre-docker snapshot).
"""

from __future__ import annotations

import json
import logging
import math
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Iterable, Mapping

from .base import LimitSample


DOCKER = Path("/Users/pluto/.orbstack/bin/docker")
CONTAINER = "omniroute"
PROVIDER = "opencode-go"
#: OmniRoute's own provider sync refreshes quota_snapshots roughly every 70
#: minutes, so a 15-minute freshness bound reported the pool as unknown for
#: most of every hour (owner order 30.08.2026: limits must run like
#: clockwork, nothing hidden). 90 minutes covers the sync cadence with
#: margin; pool quota moves slowly enough that an hour-old percentage is
#: still an honest answer, and a died sync still decays to unknown.
LIMITS_STALE_SECONDS = 5400
#: How long to wait before retrying a first failed quota read.
_DOCKER_RETRY_DELAY_SECONDS = 2.0
#: Bound on the stderr fragment carried into a failure log line.
_DOCKER_ERROR_TAIL_CHARS = 200

#: Warnings about unreadable quota sources surface on the capacity logger, so
#: a flaky collection tick is diagnosable next to the collect log lines.
_logger = logging.getLogger("agent_run.capacity")


def _bounded_error_tail(stderr: object) -> str:
    """Collapse a child's stderr into one bounded line for failure logs.

    Accepts the ``stderr`` of a ``subprocess.CompletedProcess`` or of a
    ``subprocess.TimeoutExpired``, or ``None`` when the child never ran or
    produced nothing. Returns at most ``_DOCKER_ERROR_TAIL_CHARS`` characters
    with all whitespace flattened to single spaces, so the result never
    contains a newline; returns ``""`` when there is nothing to report.
    """

    if isinstance(stderr, bytes):
        stderr = stderr.decode("utf-8", "replace")
    if not isinstance(stderr, str) or not stderr:
        return ""
    return " ".join(stderr.split())[:_DOCKER_ERROR_TAIL_CHARS]
#: OmniRoute's window keys in this runtime's vocabulary. A key absent here is
#: one we have never seen and cannot name, so it is not reported.
WINDOWS = MappingProxyType(
    {"session": "session_5h", "weekly": "weekly", "mcp_monthly": "mcp_monthly"}
)
#: Newest snapshot per (connection, window), restricted to connections that are
#: both active and quota-visible -- a pool member OmniRoute cannot see quota for
#: would otherwise average in as if it were full. Bounded: the pool is two
#: accounts over three windows, and the cap is a guard, not a page size.
_QUERY = """
SELECT s.window_key, s.remaining_percentage, s.next_reset_at, s.created_at
FROM quota_snapshots AS s
JOIN provider_connections AS c ON c.id = s.connection_id
WHERE s.provider = ?
  AND c.is_active = 1
  AND c.quota_visible = 1
  AND s.id IN (
      SELECT MAX(id) FROM quota_snapshots
      WHERE provider = ? GROUP BY connection_id, window_key)
LIMIT 64
"""
#: Runs the same SQL the old host-side sqlite reader ran, but inside the
#: container where the live database actually is, using the driver OmniRoute
#: already bundles. Rows arrive on stdout as one JSON array.
_DOCKER_SCRIPT = (
    'const Database = require("/app/node_modules/better-sqlite3");'
    'const db = new Database("/app/data/storage.sqlite", {readonly: true});'
    f"const rows = db.prepare({json.dumps(_QUERY)}).all({json.dumps(PROVIDER)}, {json.dumps(PROVIDER)});"
    "console.log(JSON.stringify(rows));"
)


def _timestamp(value: object) -> datetime | None:
    """Parse one OmniRoute ISO-8601 stamp; anything else is no evidence."""

    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _docker_rows() -> list | None:
    """Read the snapshot rows from the container; any failure is no evidence.

    The in-container read is flaky in place -- one tick returns three samples,
    the next returns none -- so a first failure is retried once after a short
    delay. A second failure still yields ``None``, which callers read as "no
    evidence" rather than "empty pool", but it is logged as a warning naming
    the failure kind: a lane that silently reports zero samples is
    indistinguishable from a healthy one from the outside.

    Returns the parsed row list on success (possibly empty), or ``None`` when
    the source could not be read. The success path stays silent.
    """

    kind, stderr = "", None
    for attempt in (0, 1):
        kind, stderr, rows = _docker_rows_once()
        if rows is not None:
            return rows
        if attempt == 0:
            time.sleep(_DOCKER_RETRY_DELAY_SECONDS)
    _logger.warning(
        "omniroute quota rows unavailable kind=%s stderr=%s",
        kind,
        _bounded_error_tail(stderr),
    )
    return None


def _docker_rows_once() -> tuple[str, object, list | None]:
    """One docker exec attempt against the container's quota store.

    Returns a ``(kind, stderr, rows)`` triple. On failure ``rows`` is ``None``
    and ``kind`` names what went wrong -- ``"timeout"``, ``"os_error"``,
    ``"exit_code"``, ``"unparseable"`` or ``"not_a_list"`` -- with ``stderr``
    carrying the child's raw stderr for the caller to log. On success ``kind``
    is ``""`` and ``rows`` is the parsed list, which may legitimately be
    empty.
    """

    try:
        result = subprocess.run(
            [str(DOCKER), "exec", CONTAINER, "node", "-e", _DOCKER_SCRIPT],
            capture_output=True,
            text=True,
            timeout=10.0,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        return "timeout", getattr(error, "stderr", None), None
    except OSError:
        return "os_error", None, None
    if result.returncode != 0:
        return "exit_code", result.stderr, None
    try:
        rows = json.loads(result.stdout)
    except ValueError:
        return "unparseable", result.stderr, None
    if not isinstance(rows, list):
        return "not_a_list", result.stderr, None
    return "", result.stderr, rows


def pool_samples(
    now: float,
    fetch_rows: Callable[[], Iterable[Mapping[str, object]]] | None = None,
) -> tuple[LimitSample, ...]:
    """Report the OmniRoute account pool's quota under the M009 contract.

    Equal pool accounts are averaged into one 0-100 figure and the *soonest*
    reset in the pool is the one reported, because that is the one that bites
    first. A window whose newest observation is older than
    ``LIMITS_STALE_SECONDS`` keeps its reset and its timestamp but reports
    ``source="unknown"`` with no percentage: never a fabricated zero. A
    failing or unparsable row source is simply no evidence.

    ``fetch_rows`` overrides the row source (tests); by default the newest
    snapshots are read out of the OmniRoute container. Local and read-only:
    one docker exec, so the collector's cadence is the only cost and no agent
    start ever reaches the network for it.
    """

    fetch = _docker_rows if fetch_rows is None else fetch_rows
    try:
        rows = fetch()
    except Exception:
        return ()
    if not isinstance(rows, (list, tuple)):
        return ()

    pooled: dict[str, list[tuple[float, datetime | None, datetime]]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        window = WINDOWS.get(row.get("window_key"))
        remaining = row.get("remaining_percentage")
        observed_at = _timestamp(row.get("created_at"))
        if (
            window is None
            or observed_at is None
            or isinstance(remaining, bool)
            or not isinstance(remaining, (int, float))
            or not math.isfinite(remaining)
        ):
            continue
        pooled.setdefault(window, []).append(
            (float(remaining), _timestamp(row.get("next_reset_at")), observed_at)
        )

    samples = []
    for window in sorted(pooled):
        members = pooled[window]
        observed_at = max(observed for _r, _reset, observed in members)
        resets = [reset for _r, reset, _o in members if reset is not None]
        stale = now - observed_at.timestamp() > LIMITS_STALE_SECONDS
        mean = sum(remaining for remaining, _reset, _o in members) / len(members)
        samples.append(
            LimitSample(
                lane="pool",
                window=window,
                remaining_percent=None if stale else max(0.0, min(100.0, mean)),
                reset_at=min(resets) if resets else None,
                observed_at=observed_at,
                source="unknown" if stale else "omniroute_quota_pool",
                target=f"{PROVIDER}:pool",
                valid_for_seconds=LIMITS_STALE_SECONDS,
            )
        )
    return tuple(samples)
