"""Credential environment and durable Claude Code state paths.

Claude Code is the sole owner of its OAuth credential and refresh lifecycle.
This module deliberately never reads a global Keychain item, extracts an
access token, or invokes a refresh probe. Each launch receives a private,
account-scoped ``CLAUDE_CONFIG_DIR`` below the service-selected durable runtime
home, so the real CLI can refresh its own state without falling back to a
global Claude account.
"""

from __future__ import annotations

import os
from pathlib import Path

from ...config import RuntimeConfig

__all__ = ["AUTH_ENV_NAMES", "TOKEN_ENV_NAME", "auth_environment", "claude_config_dir"]

TOKEN_ENV_NAME = "CLAUDE_CODE_OAUTH_TOKEN"

#: Explicit caller credentials remain authoritative. When none is declared
#: and exported, Claude Code reads and refreshes only its scoped config state.
AUTH_ENV_NAMES = ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")
_CONFIG_DIR_NAME = "claude-config"


def claude_config_dir(config: RuntimeConfig) -> Path:
    """Create and return Claude Code's durable, account-scoped config directory.

    ``config.credential_state_home`` is set by :class:`AgentService` before it
    swaps the materialized runtime home for a per-agent snapshot; it therefore
    identifies either the configured base runtime home or its selected account
    sibling. Direct adapter callers that do not go through the service use
    ``config.home``. The returned ``claude-config`` directory is private (mode
    ``0700``), persists refresh state between launches, and is never derived
    from ambient ``CLAUDE_CONFIG_DIR`` or ``HOME``.

    :param config: Effective runtime configuration with an optional durable
        credential-state home.
    :returns: The existing private config directory for the real Claude child.
    :raises OSError: If the durable directory cannot be created or protected.
    """

    directory = (config.credential_state_home or config.home) / _CONFIG_DIR_NAME
    directory.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory.chmod(0o700)
    return directory


def auth_environment(auth_names: tuple[str, ...]) -> dict[str, str]:
    """Return only explicitly declared and exported Claude auth variables.

    ``auth_names`` is the configured allow-list. A nonempty ambient value for
    one of those names is copied unchanged, preserving explicit API-key or
    OAuth-token precedence. With no such value this returns ``{}``: the child
    CLI is responsible for reading, refreshing, or rejecting its own scoped
    credential state. Undeclared ambient values are never copied.

    :param auth_names: Configured environment variable names allowed into the
        child.
    :returns: The nonempty explicitly exported subset of ``auth_names``.
    """

    return {name: value for name in auth_names if (value := os.environ.get(name))}
