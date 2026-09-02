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

What the server does keep is sqlite: the ``key_value`` table holds one row per
connection under namespace ``providerLimitsCache`` (key = connection id),
whose JSON value carries the *current* quota reading -- ``quotas`` (per
window: ``remainingPercentage``, ``resetAt``), ``fetchedAt`` (when that
reading was taken) and ``source`` (``"manual"`` or ``"scheduled"``).
``quota_snapshots`` is an older history table that OmniRoute's own sync can
leave unchanged for well over an hour; reading it instead of the current
cache is what previously made a fresh percentage look stale. A cache read
older than the freshness bound below is reported as unknown -- the honest
answer, not a missing feature. A failed OmniRoute refresh does not always
drop the previous reading: OmniRoute's own merge (``mergeProviderLimitsCacheEntry``)
can retain the last good cache entry with its old ``fetchedAt`` rather than
clearing it, so an unresponsive collector shows up here as a cache that
keeps getting older, not as an empty one -- which is exactly what the
staleness bound below is for. A row this runtime cannot parse into a usable
``quotas`` object is rejected outright rather than silently dropped, so a
member's read failure surfaces as an explicit source error instead of
quietly shrinking the pool.

OmniRoute now runs inside a docker container (OrbStack) whose volume is not
readable from the host, so the store is queried in place: the query runs
inside the container against ``/app/data/storage.sqlite`` via the sqlite
driver OmniRoute itself ships with (verified live 2026-08-28: the old host
copy ``~/.omniroute/storage.sqlite`` is a stale pre-docker snapshot).
"""

from __future__ import annotations

from ..errors import CapacitySourceError

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
#: A 15-minute freshness bound reported the pool as unknown for most of every
#: hour, because OmniRoute's own collection does not run that often. 90
#: minutes covers a slow collection cycle with margin; pool quota moves
#: slowly enough that an hour-old percentage is still an honest answer, and a
#: died collector still decays to unknown.
LIMITS_STALE_SECONDS = 5400
#: How long to wait before retrying a first failed quota read.
_DOCKER_RETRY_DELAY_SECONDS = 2.0

#: Warnings about unreadable quota sources surface on the capacity logger, so
#: a flaky collection tick is diagnosable next to the collect log lines.
_logger = logging.getLogger("agent_run.capacity")


#: OmniRoute's window keys in this runtime's vocabulary. A key absent here is
#: one we have never seen and cannot name, so it is not reported.
WINDOWS = MappingProxyType(
    {"session": "session_5h", "weekly": "weekly", "mcp_monthly": "mcp_monthly"}
)
#: One row per connection that is both active and quota-visible -- a pool
#: member OmniRoute cannot see quota for would otherwise silently drop out of
#: the average as if it did not exist. The LEFT JOIN keeps such a member in
#: the result with a null cache rather than inner-joining it away. ``LIMIT``
#: is a guard against an unbounded pool, not a page size, and matches the
#: 64-row emission cap in ``_DOCKER_SCRIPT`` with one row of headroom for the
#: overflow sentinel. Only the cache JSON is selected; the connection id is
#: used solely to join and is never itself part of the result.
_MEMBERS_QUERY = """
SELECT kv.value AS cache_json
FROM provider_connections AS c
LEFT JOIN key_value AS kv
  ON kv.namespace = ? AND kv.key = c.id
WHERE c.provider = ?
  AND c.is_active = 1
  AND c.quota_visible = 1
LIMIT 65
"""
#: The current-quota cache OmniRoute itself maintains: one ``key_value`` row
#: per connection, namespace ``providerLimitsCache``, key = connection id.
_CACHE_NAMESPACE = "providerLimitsCache"
#: The most rows ``_DOCKER_SCRIPT`` will ever emit before it stops and appends
#: one overflow sentinel row instead of continuing -- kept in lockstep with
#: the ``len(rows) > 64`` overflow check in ``pool_samples``.
_MAX_EMITTED_ROWS = 64
#: Runs the members query inside the container where the live database
#: actually is, using the driver OmniRoute already bundles, then projects
#: each member's cached quota JSON down to sanitized, type-checked fields --
#: never the connection id, raw cache JSON, message text, plan, or any other
#: cache field, and never an arbitrary nested value smuggled through
#: unexamined -- across stdout. ``remaining_percentage`` is emitted only as a
#: finite non-boolean number or ``null``. ``fetched_at`` (the observed-at
#: reading) is emitted only as a bounded ISO-8601 string or ``null``; there is
#: no legitimate-but-missing case for it, so anything unparsable collapses to
#: ``null`` and the caller's own "no evidence" handling takes it from there.
#: ``next_reset_at`` is different: a reset that is absent (key missing or
#: explicitly ``null`` in the source JSON) is legitimate and stays ``null``,
#: but a reset that is *present* and fails validation (an object, an array, a
#: malformed or oversized string) is replaced with a fixed, content-free
#: poison marker rather than ``null`` -- collapsing it to ``null`` would make
#: a malformed reset indistinguishable from a genuinely absent one and let
#: the caller revive the window as healthy. ``quotas`` and each per-window
#: quota value are rejected as unusable when they are an array (arrays are
#: not valid cache/quota objects even though ``typeof [] === "object"``). A
#: member with no cache row, an unparseable cache value, or a cache value
#: with no usable ``quotas`` object still emits one row per known window with
#: a null percentage, so that member's failure surfaces as data (caught by
#: the caller's own validation) instead of silently vanishing from the pool.
#: Total emitted rows are capped at ``_MAX_EMITTED_ROWS``: once that many real
#: rows have been pushed, one overflow sentinel row is appended and emission
#: stops immediately, rather than continuing to build every member's full set
#: of window rows only to have the caller discard them. Rows arrive on
#: stdout as one JSON array.
_DOCKER_SCRIPT = (
    'const Database = require("/app/node_modules/better-sqlite3");'
    'const db = new Database("/app/data/storage.sqlite", {readonly: true});'
    f"const members = db.prepare({json.dumps(_MEMBERS_QUERY)})"
    f".all({json.dumps(_CACHE_NAMESPACE)}, {json.dumps(PROVIDER)});"
    f"const WINDOW_KEYS = {json.dumps(list(WINDOWS.keys()))};"
    f"const MAX_ROWS = {json.dumps(_MAX_EMITTED_ROWS)};"
    "const ISO_RE = /^\\d{4}-\\d{2}-\\d{2}[T ]\\d{2}:\\d{2}:\\d{2}"
    "(\\.\\d{1,6})?(Z|[+-]\\d{2}:?\\d{2})?$/;"
    "function isFiniteNumber(v) {"
    "  return typeof v === \"number\" && !isNaN(v) && isFinite(v);"
    "}"
    "function isBoundedIsoString(v) {"
    "  return typeof v === \"string\" && v.length <= 40 && ISO_RE.test(v);"
    "}"
    "function sanitizeObservedAt(v) {"
    "  return isBoundedIsoString(v) ? v : null;"
    "}"
    "function sanitizeResetAt(v) {"
    "  if (v === undefined || v === null) return null;"
    "  return isBoundedIsoString(v) ? v : \"__unparseable_reset__\";"
    "}"
    "function isPlainObject(v) {"
    "  return v !== null && typeof v === \"object\" && !Array.isArray(v);"
    "}"
    "const rows = [];"
    "memberLoop:"
    "for (const member of members) {"
    "  let quotas = null, fetchedAt = null;"
    "  if (member.cache_json) {"
    "    try {"
    "      const parsed = JSON.parse(member.cache_json);"
    "      if (isPlainObject(parsed) && isPlainObject(parsed.quotas)) {"
    "        quotas = parsed.quotas;"
    "        fetchedAt = sanitizeObservedAt(parsed.fetchedAt);"
    "      }"
    "    } catch (e) {}"
    "  }"
    "  for (const key of WINDOW_KEYS) {"
    "    if (rows.length >= MAX_ROWS) {"
    "      rows.push({window_key: \"__overflow__\", remaining_percentage: null,"
    "                 next_reset_at: null, fetched_at: null});"
    "      break memberLoop;"
    "    }"
    "    const q = quotas ? quotas[key] : null;"
    "    if (isPlainObject(q)) {"
    "      rows.push({"
    "        window_key: key,"
    "        remaining_percentage: isFiniteNumber(q.remainingPercentage)"
    "          ? q.remainingPercentage : null,"
    "        next_reset_at: sanitizeResetAt(q.resetAt),"
    "        fetched_at: fetchedAt"
    "      });"
    "    } else {"
    "      rows.push({window_key: key, remaining_percentage: null, next_reset_at: null, fetched_at: null});"
    "    }"
    "  }"
    "}"
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


def _docker_rows() -> list:
    """Read quota rows with one retry and fail safely after two failures.

    Each attempt has a ten-second timeout and the attempts are separated by
    ``_DOCKER_RETRY_DELAY_SECONDS``. An empty list is successful empty data;
    exhausted reads raise ``CapacitySourceError``. The warning contains only a
    safe kind and optional integer return code, never stderr.
    """

    kind, returncode = "", None
    for attempt in (0, 1):
        kind, returncode, rows = _docker_rows_once()
        if rows is not None:
            return rows
        if attempt == 0:
            time.sleep(_DOCKER_RETRY_DELAY_SECONDS)
    _logger.warning("omniroute quota rows unavailable kind=%s rc=%s", kind, returncode)
    raise CapacitySourceError("omniroute_unavailable")


def _docker_rows_once() -> tuple[str, int | None, list | None]:
    """One docker exec attempt against the container's quota store.

    Returns a ``(kind, returncode, rows)`` triple. On failure ``rows`` is ``None``
    and ``kind`` names what went wrong -- ``"timeout"``, ``"os_error"``,
    ``"exit_code"``, ``"unparseable"`` or ``"not_a_list"`` -- with only an
    integer process status when available. Raw stderr is never returned.
    On success ``kind`` is ``""`` and ``rows`` may legitimately be empty.
    """

    try:
        result = subprocess.run(
            [str(DOCKER), "exec", CONTAINER, "node", "-e", _DOCKER_SCRIPT],
            capture_output=True,
            text=True,
            timeout=10.0,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return "timeout", None, None
    except OSError:
        return "os_error", None, None
    if result.returncode != 0:
        return "exit_code", result.returncode, None
    try:
        rows = json.loads(result.stdout)
    except ValueError:
        return "unparseable", result.returncode, None
    if not isinstance(rows, list):
        return "not_a_list", result.returncode, None
    return "", result.returncode, rows


def pool_samples(
    now: float,
    fetch_rows: Callable[[], Iterable[Mapping[str, object]]] | None = None,
) -> tuple[LimitSample, ...]:
    """Report the OmniRoute account pool's quota as one figure per window.

    Equal pool accounts are averaged into one 0-100 figure and the *soonest*
    reset in the pool is the one reported, because that is the one that bites
    first. A window whose oldest included observation is older than
    ``LIMITS_STALE_SECONDS`` keeps its reset and its timestamp but reports
    ``source="unknown"`` with no percentage: never a fabricated zero. A
    future observation or an expired reset also makes the whole window
    unknown. Failed or malformed sources raise ``CapacitySourceError``;
    a successfully read empty pool returns an empty tuple.

    ``fetch_rows`` overrides the row source (tests); by default each active,
    quota-visible pool member's *current* quota cache is read out of the
    OmniRoute container, keyed by that member's ``fetched_at`` -- never the
    older, more slowly refreshed history table. Local and read-only: one
    docker exec, so the collector's cadence is the only cost and no agent
    start ever reaches the network for it.
    """

    fetch = _docker_rows if fetch_rows is None else fetch_rows
    try:
        rows = fetch()
    except CapacitySourceError:
        raise
    except Exception:
        raise CapacitySourceError("omniroute_unavailable") from None
    if not isinstance(rows, (list, tuple)):
        raise CapacitySourceError("omniroute_unavailable")
    if len(rows) > 64:
        raise CapacitySourceError("omniroute_result_overflow")

    pooled: dict[str, list[tuple[float, datetime | None, datetime]]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise CapacitySourceError("omniroute_malformed_data")
        window_key = row.get("window_key")
        window = WINDOWS.get(window_key) if isinstance(window_key, str) else None
        if window is None:
            continue
        remaining = row.get("remaining_percentage")
        observed_at = _timestamp(row.get("fetched_at"))
        reset_value = row.get("next_reset_at")
        reset_at = _timestamp(reset_value) if reset_value is not None else None
        if (
            observed_at is None
            or isinstance(remaining, bool)
            or not isinstance(remaining, (int, float))
            or not math.isfinite(remaining)
            or (reset_value is not None and reset_at is None)
        ):
            raise CapacitySourceError("omniroute_malformed_data")
        pooled.setdefault(window, []).append(
            (float(remaining), reset_at, observed_at)
        )

    samples = []
    for window in sorted(pooled):
        members = pooled[window]
        observed_at = min(observed for _r, _reset, observed in members)
        resets = [reset for _r, reset, _o in members if reset is not None]
        future = any(observed.timestamp() > now for _r, _reset, observed in members)
        expired = any(reset.timestamp() <= now for reset in resets)
        stale = now - observed_at.timestamp() > LIMITS_STALE_SECONDS
        mean = sum(remaining for remaining, _reset, _o in members) / len(members)
        unknown = future or expired or stale
        samples.append(
            LimitSample(
                lane="pool",
                window=window,
                remaining_percent=None if unknown else max(0.0, min(100.0, mean)),
                reset_at=min(resets) if resets else None,
                observed_at=observed_at,
                source="unknown" if unknown else "omniroute_quota_pool",
                target=f"{PROVIDER}:pool",
                valid_for_seconds=LIMITS_STALE_SECONDS,
            )
        )
    return tuple(samples)
