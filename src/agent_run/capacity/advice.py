"""Advisory recommendations and a noise-tolerant semantic capacity key.

Advice is always advisory: an explicit owner choice wins regardless of risk.
``advice_key`` intentionally drops high-resolution timestamps so it changes
only on material capacity state, not on every collection tick.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Iterable

from .forecast import RISK_HIGH, RISK_MEDIUM, RISK_UNKNOWN, CapacityForecast
from .history import CapacityKey


_REMAINING_BUCKET_PERCENT = 5.0
_RESET_BUCKET_SECONDS = 300


@dataclass(frozen=True)
class CapacityAdvice:
    key: CapacityKey
    known: bool
    remaining_percent: float | None
    reset_at: float | None
    warmup: bool
    risk: str
    recommendations: tuple[str, ...]


def build_advice(forecasts: Iterable[CapacityForecast]) -> tuple[CapacityAdvice, ...]:
    return tuple(
        CapacityAdvice(
            key=forecast.key,
            known=forecast.known,
            remaining_percent=forecast.remaining_percent,
            reset_at=forecast.reset_at,
            warmup=forecast.warmup,
            risk=forecast.risk,
            recommendations=_recommendations(forecast),
        )
        for forecast in forecasts
    )


def _recommendations(forecast: CapacityForecast) -> tuple[str, ...]:
    label = f"{forecast.key.runtime}/{forecast.key.lane} {forecast.key.window}"
    if forecast.risk == RISK_UNKNOWN:
        return (f"{label} capacity is unknown; treat limits as unverified.",)
    if forecast.risk == RISK_HIGH:
        return (f"{label} is near exhaustion; avoid starting new {forecast.key.lane} work before reset.",)
    if forecast.risk == RISK_MEDIUM:
        return (f"{label} is trending toward exhaustion; pace new requests.",)
    return ()


def _bucketed_remaining(remaining: float | None) -> float | None:
    if remaining is None:
        return None
    return round(remaining / _REMAINING_BUCKET_PERCENT) * _REMAINING_BUCKET_PERCENT


def _bucketed_reset(reset_at: float | None) -> int | None:
    if reset_at is None:
        return None
    return int(reset_at // _RESET_BUCKET_SECONDS)


def _sort_key(advice: CapacityAdvice) -> tuple[str, str, str, str, str]:
    key = advice.key
    return (key.runtime, key.lane, key.window, key.target or "", key.source)


def advice_key(items: Iterable[CapacityAdvice]) -> str:
    ordered = sorted(items, key=_sort_key)
    parts = [
        "|".join(
            (
                key.runtime,
                key.lane,
                key.window,
                key.target or "",
                key.source,
                item.risk,
                "warmup" if item.warmup else "steady",
                str(_bucketed_remaining(item.remaining_percent)),
                str(_bucketed_reset(item.reset_at)),
            )
        )
        for item, key in ((item, item.key) for item in ordered)
    ]
    digest = hashlib.sha256("::".join(parts).encode("utf-8")).hexdigest()
    return digest[:32]
