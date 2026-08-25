"""Shared foundation for agent-run."""

from __future__ import annotations

import sys
from typing import Sequence


def _require_supported_python(version_info: Sequence[int] | None = None) -> None:
    version = tuple((version_info or sys.version_info)[:2])
    if version < (3, 11):
        raise RuntimeError(
            f"agent-run requires Python 3.11 or newer; found {version[0]}.{version[1]}"
        )


_require_supported_python()
