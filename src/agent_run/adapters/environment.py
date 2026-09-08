"""Host-environment inheritance shared by runtime adapters."""

from __future__ import annotations

import os
import re
from collections.abc import Iterable, Mapping


_SECRET_NAME = re.compile(r"(key|token|secret|password|credential)", re.IGNORECASE)
_CREDENTIAL_CARRIERS = frozenset(
    {
        "AWS_PROFILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AZURE_CONFIG_DIR",
        "CLOUDSDK_CONFIG",
        "DOCKER_CONFIG",
        "GH_CONFIG_DIR",
        "GIT_ASKPASS",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "KUBECONFIG",
        "NETRC",
        "NPM_CONFIG_USERCONFIG",
        "PGPASSFILE",
        "SSH_ASKPASS",
        "SSH_AUTH_SOCK",
    }
)
_SAFE_HOST_NAMES = frozenset(
    {
        "AGENT_RUN_HOME",
        "AR",
        "ARCHFLAGS",
        "BUN_INSTALL",
        "CC",
        "CFLAGS",
        "COLORTERM",
        "CPATH",
        "CPPFLAGS",
        "CXX",
        "CXXFLAGS",
        "DEVELOPER_DIR",
        "GOCACHE",
        "GOENV",
        "GOFLAGS",
        "GOMODCACHE",
        "GOPATH",
        "GOROOT",
        "GOTOOLCHAIN",
        "JAVA_HOME",
        "LANG",
        "LANGUAGE",
        "LD",
        "LDFLAGS",
        "LIBRARY_PATH",
        "LOGNAME",
        "MACOSX_DEPLOYMENT_TARGET",
        "MAKEFLAGS",
        "NVM_DIR",
        "PATH",
        "PKG_CONFIG_PATH",
        "PNPM_HOME",
        "PYENV_ROOT",
        "RUSTUP_HOME",
        "SDKROOT",
        "SHELL",
        "TERM",
        "TERM_PROGRAM",
        "TMPDIR",
        "USER",
        "UV_PYTHON_INSTALL_DIR",
        "VIRTUAL_ENV",
    }
)
_SAFE_HOST_PREFIXES = ("CCACHE_", "CMAKE_", "LC_", "NODE_", "PYTHON", "RUST")


def is_secret_env_name(name: str) -> bool:
    """Return whether ``name`` is shaped like a credential variable."""

    return _SECRET_NAME.search(name) is not None


def _safe_host_name(name: str) -> bool:
    """Return whether a non-credential host variable is a build/runtime input."""

    return name in _SAFE_HOST_NAMES or name.startswith(_SAFE_HOST_PREFIXES)


def host_environment(
    overrides: Mapping[str, str], *, allowed_secret_names: Iterable[str] = ()
) -> dict[str, str]:
    """Return the host environment with runtime-owned overrides.

    PATH, locale, SDK, compiler and toolchain variables on the small safe list
    are inherited unchanged. Credential-shaped variables and known credential
    carriers are omitted unless the resolved runtime auth or MCP contract names
    them explicitly. ``overrides`` always wins and remains process-memory-only.
    """

    allowed = frozenset(allowed_secret_names)
    environment = {
        name: value
        for name, value in os.environ.items()
        if name in allowed
        or (
            name not in _CREDENTIAL_CARRIERS
            and not is_secret_env_name(name)
            and _safe_host_name(name)
        )
    }
    environment.update(overrides)
    return environment
