"""Shared foundation for agent-run."""

from __future__ import annotations

import sys
from typing import Sequence


def _require_supported_python(version_info: Sequence[int] | None = None) -> None:
    """Reject interpreters below Python 3.14 before importing runtime features.

    ``version_info`` is an optional two-or-more-integer sequence used by tests;
    when omitted, the running interpreter's ``sys.version_info`` is checked.
    The function returns ``None`` for Python 3.14 and newer and raises
    ``RuntimeError`` for older versions without changing process state.
    """
    version = tuple((version_info or sys.version_info)[:2])
    if version < (3, 14):
        raise RuntimeError(
            f"agent-run requires Python 3.14 or newer; found {version[0]}.{version[1]}"
        )


_require_supported_python()
