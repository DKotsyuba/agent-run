"""Keychain fallback for the GLM Coding Plan's Anthropic-protocol credential.

The Z.ai GLM Coding Plan exposes an Anthropic Messages endpoint, so the glm
runtime launches the claude CLI against ``ANTHROPIC_BASE_URL`` with
``ANTHROPIC_AUTH_TOKEN`` as the key. A child launched without the token
exported falls back to the managed login-keychain item the owner provisions
out of band (mirrors :mod:`agent_run.adapters.qwen.auth`), so the secret
never has to be exported into every shell that starts a glm child. The
process environment always wins when both a value and a keychain entry
exist.
"""

from __future__ import annotations

from functools import lru_cache
import os
import pwd
import subprocess

__all__ = [
    "DEFAULT_BASE_URL",
    "KEYCHAIN_ACCOUNT",
    "KEYCHAIN_SERVICE",
    "keychain_glm_key",
]

#: Anthropic Messages endpoint of the Z.ai GLM Coding Plan, used when
#: ``ANTHROPIC_BASE_URL`` is unset.
DEFAULT_BASE_URL = "https://api.z.ai/api/anthropic"

KEYCHAIN_SERVICE = "com.pluto.agent-run.glm"
KEYCHAIN_ACCOUNT = "GLM_CODING_KEY"

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
    KEYCHAIN_ACCOUNT,
    "-s",
    KEYCHAIN_SERVICE,
    "-w",
    _KEYCHAIN_PATH,
)
_KEYCHAIN_TIMEOUT_SECONDS = 3


@lru_cache(maxsize=1)
def keychain_glm_key() -> str | None:
    """Return the nonblank GLM Coding Plan key from Keychain, or ``None`` on failure.

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
