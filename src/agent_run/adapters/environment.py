"""Host-environment inheritance shared by runtime adapters."""

from __future__ import annotations

import os
import re
from collections.abc import Iterable, Mapping


_SECRET_NAME = re.compile(r"(key|token|secret|password|credential)", re.IGNORECASE)


def is_secret_env_name(name: str) -> bool:
    """Return whether ``name`` is shaped like a credential variable."""

    return _SECRET_NAME.search(name) is not None


def host_environment(
    overrides: Mapping[str, str], *, allowed_secret_names: Iterable[str] = ()
) -> dict[str, str]:
    """Return the host environment with runtime-owned overrides.

    PATH, locale, SDK, compiler and toolchain variables are inherited unchanged.
    Ambient credential-shaped variables are omitted unless the resolved runtime
    auth or MCP contract explicitly names them. ``overrides`` always wins and
    remains process-memory-only with the resulting launch plan.
    """

    allowed = frozenset(allowed_secret_names)
    environment = {
        name: value
        for name, value in os.environ.items()
        if not is_secret_env_name(name) or name in allowed
    }
    environment.update(overrides)
    return environment
