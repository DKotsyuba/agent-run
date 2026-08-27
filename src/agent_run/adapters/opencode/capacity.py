"""The OmniRoute account pool's quota, read from OmniRoute's own store.

Every model this runtime can reach is served by OmniRoute out of one
``opencode-go`` account pool, so the pool's quota *is* this runtime's quota.

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
"""

from __future__ import annotations

import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType

from ..base import LimitSample


OMNIROUTE_STORAGE = Path.home() / ".omniroute" / "storage.sqlite"
PROVIDER = "opencode-go"
LIMITS_STALE_SECONDS = 900
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


def _timestamp(value: object) -> datetime | None:
    """Parse one OmniRoute ISO-8601 stamp; anything else is no evidence."""

    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def pool_samples(now: float, database: Path | None = None) -> tuple[LimitSample, ...]:
    """Report the OmniRoute account pool's quota under the M009 contract.

    Equal pool accounts are averaged into one 0-100 figure and the *soonest*
    reset in the pool is the one reported, because that is the one that bites
    first. A window whose newest observation is older than
    ``LIMITS_STALE_SECONDS`` keeps its reset and its timestamp but reports
    ``source="unknown"`` with no percentage: never a fabricated zero. An
    unreadable, missing, or schema-drifted store is simply no evidence.

    Read-only and local: this opens a file, so the collector's cadence is the
    only cost and no agent start ever reaches the network for it.
    """

    path = OMNIROUTE_STORAGE if database is None else Path(database)
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=2.0)
    except sqlite3.Error:
        return ()
    try:
        rows = connection.execute(_QUERY, (PROVIDER, PROVIDER)).fetchall()
    except sqlite3.Error:  # a drifted or quarantined store is no evidence
        return ()
    finally:
        connection.close()

    pooled: dict[str, list[tuple[float, datetime | None, datetime]]] = {}
    for window_key, remaining, reset_raw, observed_raw in rows:
        window = WINDOWS.get(window_key)
        observed_at = _timestamp(observed_raw)
        if (
            window is None
            or observed_at is None
            or isinstance(remaining, bool)
            or not isinstance(remaining, (int, float))
            or not math.isfinite(remaining)
        ):
            continue
        pooled.setdefault(window, []).append(
            (float(remaining), _timestamp(reset_raw), observed_at)
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
            )
        )
    return tuple(samples)
