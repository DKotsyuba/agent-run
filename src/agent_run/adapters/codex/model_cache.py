"""Refresh and persist the isolated Codex model roster cache."""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Mapping

from ...config import RuntimeConfig
from ..base import LaunchPlan
from . import app_server, environment


_MODEL_CACHE_MAX_AGE_SECONDS = 24 * 60 * 60
_MODEL_REFRESH_TIMEOUT_SECONDS = 20.0
_MODEL_CACHE_REL = "cache/models.json"


def _write_model_cache(path: Path, models: tuple[Mapping[str, object], ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps({"models": [dict(model) for model in models]}, separators=(",", ":"))
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _cache_is_fresh(path: Path, now: float) -> bool:
    try:
        return now - path.stat().st_mtime <= _MODEL_CACHE_MAX_AGE_SECONDS
    except OSError:
        return False


def refresh_models(config: RuntimeConfig, home: Path) -> None:
    """Refresh ``home``'s isolated model roster cache from the real app-server.

    ``config`` supplies the Codex binary and ``home`` the runtime home whose
    ``cache/models.json`` is rewritten.  The child is launched with the shared
    :func:`environment.build_environment` mapping, so the packaged interpreter
    resolves even under the collector's minimal PATH.  Any failure -- spawn,
    handshake, or cache write -- is swallowed and leaves any existing cache in
    place, so a refresh problem never turns into an agent-start failure.
    """

    plan = LaunchPlan(
        argv=(str(config.binary), "app-server"),
        cwd=Path(home),
        environment=environment.build_environment(config.binary, home),
        initial_input=None,
        runtime_stream_path=Path(home) / ".model-refresh.jsonl",
        adapter_state={},
    )
    try:
        models = app_server.fetch_models(plan, timeout_seconds=_MODEL_REFRESH_TIMEOUT_SECONDS)
        _write_model_cache(Path(home) / _MODEL_CACHE_REL, models)
    except Exception:
        return


def is_fresh(path: Path, now: float) -> bool:
    return _cache_is_fresh(path, now)
