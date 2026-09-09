"""Capacity intelligence: adapter-limit collection, forecast, and advice."""

from .advice import CapacityAdvice, advice_key, build_advice
from .collect import CollectionReport, CollectResult, collect_once
from .forecast import CapacityForecast, build_forecasts
from .history import CapacityKey, CapacitySeries, NormalizedSample, load_series
from .launchd import LaunchdJob, build_job, render_plist

__all__ = [
    "CapacityAdvice",
    "CapacityForecast",
    "CapacityKey",
    "CapacitySeries",
    "CollectResult",
    "CollectionReport",
    "LaunchdJob",
    "NormalizedSample",
    "advice_key",
    "build_advice",
    "build_forecasts",
    "build_job",
    "collect_once",
    "load_series",
    "render_plist",
]
