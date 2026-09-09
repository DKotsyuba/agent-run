"""Host-environment inheritance shared by runtime adapters."""

from __future__ import annotations

import os
import re
from collections.abc import Iterable, Mapping
from pathlib import Path


_SECRET_NAME = re.compile(r"(key|token|secret|password|credential)", re.IGNORECASE)
_CREDENTIAL_CARRIERS = frozenset(
    {
        "AWS_PROFILE",
        "AWS_CONFIG_FILE",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AZURE_CONFIG_DIR",
        "BOTO_CONFIG",
        "BUNDLE_USER_CONFIG",
        "CLOUDSDK_CONFIG",
        "DOCKER_CONFIG",
        "GH_CONFIG_DIR",
        "GIT_ASKPASS",
        "GIT_CONFIG_GLOBAL",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "GNUPGHOME",
        "KRB5CCNAME",
        "KUBECONFIG",
        "NETRC",
        "NPM_CONFIG_USERCONFIG",
        "PIP_CONFIG_FILE",
        "PGPASSFILE",
        "SSH_ASKPASS",
        "SSH_AUTH_SOCK",
        "TF_CLI_CONFIG_FILE",
    }
)


def is_secret_env_name(name: str) -> bool:
    """Return whether ``name`` is shaped like a credential variable."""

    return _SECRET_NAME.search(name) is not None


def host_environment(
    overrides: Mapping[str, str], *, allowed_secret_names: Iterable[str] = ()
) -> dict[str, str]:
    """Return the host environment with runtime-owned overrides.

    Ordinary variables are inherited unchanged. Credential-shaped names and a
    compact set of known credential/config carriers are subtracted unless the
    resolved runtime auth or MCP contract names them. Explicit Rust toolchain
    homes are preserved; when absent, existing `.rustup` and `.cargo` directories
    beneath the original host ``HOME`` are inherited before a runtime replaces
    ``HOME``. ``overrides`` always wins and remains process-memory-only.
    """

    allowed = frozenset(allowed_secret_names)
    environment = {
        name: value
        for name, value in os.environ.items()
        if name in allowed
        or (
            name not in _CREDENTIAL_CARRIERS
            and not is_secret_env_name(name)
        )
    }
    host_home = os.environ.get("HOME")
    if host_home:
        for name, directory in (
            ("RUSTUP_HOME", ".rustup"),
            ("CARGO_HOME", ".cargo"),
        ):
            candidate = Path(host_home) / directory
            if name not in environment and candidate.is_dir():
                environment[name] = str(candidate)
    environment.update(overrides)
    return environment
