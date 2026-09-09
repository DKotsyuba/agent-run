"""Claude credential environment and optional account-scoped state paths."""

from __future__ import annotations

import os
from pathlib import Path

from ...config import RuntimeConfig

__all__ = [
    "AUTH_ENV_NAMES",
    "TOKEN_ENV_NAME",
    "auth_environment",
    "claude_config_dir",
    "claude_login_environment",
]

TOKEN_ENV_NAME = "CLAUDE_CODE_OAUTH_TOKEN"

#: Explicit caller credentials remain authoritative. When none is declared
#: and exported, Claude Code reads and refreshes only its scoped config state.
AUTH_ENV_NAMES = ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")
#: ``str`` leaf directory name beneath each durable Claude credential-state home.
_CONFIG_DIR_NAME = "claude-config"


def _native_home() -> Path:
    """Resolve the host login home for native Claude credential lookup.

    ``Path.home()`` follows the current process ``HOME`` environment, which is
    intentionally isolated for Claude runtime launches. The account-keyed fallback
    path must remain anchored to the real user home instead.
    """

    try:
        import pwd

        return Path(pwd.getpwuid(os.getuid()).pw_dir).resolve()
    except (AttributeError, KeyError, ImportError, OSError):
        configured_home = os.environ.get("HOME")
        return Path(configured_home).expanduser() if configured_home else Path.home()


def claude_config_dir(config: RuntimeConfig) -> Path:
    """Return native global state or create one explicit account directory.

    An absent ``credential_state_home`` selects the host CLI's native
    ``CLAUDE_CONFIG_DIR`` or ``~/.claude`` without creating or copying it. An
    explicit service-selected state home receives a private ``claude-config``
    child shared by login and launches for that account label.

    :param RuntimeConfig config: Effective runtime configuration with an
        optional durable credential-state home.
    :returns Path: The existing private config directory for the real Claude
        child.
    :raises OSError: If the durable directory cannot be created or protected.
    """

    if config.credential_state_home is None:
        configured = os.environ.get("CLAUDE_CONFIG_DIR")
        return (
            Path(configured).expanduser()
            if configured
            else _native_home() / ".claude"
        )
    directory = config.credential_state_home / _CONFIG_DIR_NAME
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

    :param tuple[str, ...] auth_names: Configured environment variable names
        allowed into the child.
    :returns dict[str, str]: The nonempty explicitly exported subset of
        ``auth_names``.
    """

    return {name: value for name in auth_names if (value := os.environ.get(name))}


def claude_login_environment(config: RuntimeConfig) -> dict[str, str]:
    """Build the minimal interactive-login environment for one scoped runtime.

    ``config`` must carry the selected durable credential-state home, normally
    installed by the service or CLI with ``credential_state_home``. Only
    ``PATH`` and ``HOME`` needed to locate the configured Claude executable and
    launch its browser flow are retained from the parent; all explicit Claude
    credential variables are omitted. ``CLAUDE_CONFIG_DIR`` is always the
    private directory returned by :func:`claude_config_dir`, so the interactive
    login and a later agent child share one account state without consulting a
    global Claude configuration.

    :param RuntimeConfig config: Effective Claude runtime configuration for one
        account.
    :returns dict[str, str]: Private environment for ``claude auth login`` and
        status.
    :raises OSError: If the durable scoped config directory cannot be prepared.
    """

    environment = {key: os.environ[key] for key in ("PATH", "HOME") if key in os.environ}
    environment["CLAUDE_CONFIG_DIR"] = str(claude_config_dir(config))
    return environment
