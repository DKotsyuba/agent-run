"""Keychain fallback for Qwen's OpenAI-compatible provider credentials.

Qwen Code authenticates through ``OPENAI_API_KEY`` and talks to an
OpenAI-compatible endpoint named by ``OPENAI_BASE_URL``. A child launched
without those exported into the process environment falls back to the same
managed OmniRoute keychain item the OpenCode server password helper reads
(mirrors :mod:`agent_run.adapters.opencode.password`), so the operator does
not have to export a secret by hand into every shell that starts a qwen
child. The process environment always wins when both a value and a keychain
entry exist.
"""

from __future__ import annotations

from functools import lru_cache
import os
import pwd
import subprocess

__all__ = ["DEFAULT_BASE_URL", "keychain_omniroute_api_key"]

#: Loopback OmniRoute router address used when ``OPENAI_BASE_URL`` is unset.
DEFAULT_BASE_URL = "http://127.0.0.1:20128/v1"

# The login keychain must be named explicitly: ``security``'s default search
# list is resolved from the process's real passwd-entry home, which a
# generated child ``HOME`` (or a sandboxed development shell) does not share,
# so an unqualified lookup silently misses the item instead of finding it.
_KEYCHAIN_PATH = os.path.join(
    pwd.getpwuid(os.getuid()).pw_dir, "Library", "Keychains", "login.keychain-db"
)
_SECURITY_COMMAND = (
    "/usr/bin/security",
    "find-generic-password",
    "-a",
    "OMNIROUTE_API_KEY",
    "-s",
    "com.pluto.agent-run.opencode.omniroute",
    "-w",
    _KEYCHAIN_PATH,
)
_KEYCHAIN_TIMEOUT_SECONDS = 3


@lru_cache(maxsize=1)
def keychain_omniroute_api_key() -> str | None:
    """Return the nonblank OmniRoute API key from Keychain, or ``None`` on failure.

    :returns: The trimmed key text, or ``None`` when the lookup command fails,
        times out, exits nonzero, or returns blank output. Cached for the
        process so a missing export does not repeatedly launch the Keychain
        command. Command output is deliberately never included in an
        exception or log message, so the key can never be printed.
    """

    try:
        result = subprocess.run(
            _SECURITY_COMMAND,
            capture_output=True,
            check=False,
            text=True,
            timeout=_KEYCHAIN_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    return value or None
