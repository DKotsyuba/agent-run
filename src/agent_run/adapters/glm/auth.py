"""Keychain fallback for the GLM Coding Plan's Anthropic-protocol credential.

The Z.ai GLM Coding Plan exposes an Anthropic Messages endpoint, so the glm
runtime launches the claude CLI against ``ANTHROPIC_BASE_URL`` with
``ANTHROPIC_AUTH_TOKEN`` as the key. A child launched without the token
exported falls back to the managed login-keychain item the owner provisions
out of band (mirrors :mod:`agent_run.adapters.qwen.auth`), so the secret
never has to be exported into every shell that starts a glm child.

Precedence is the keychain's, not the environment's: an inherited
``ANTHROPIC_AUTH_TOKEN`` is the orchestrator's own Anthropic credential, which
this endpoint rejects, so :func:`agent_run.adapters.glm.adapter._glm_environment`
consults the keychain first and falls back to the exported value only when the
keychain has nothing.
"""

from __future__ import annotations

import os
import pwd
import subprocess
import threading
import time

__all__ = [
    "DEFAULT_BASE_URL",
    "KEYCHAIN_ACCOUNT",
    "KEYCHAIN_SERVICE",
    "keychain_glm_key",
    "reset_keychain_cache",
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
#: How long one successful lookup is reused. The ``security`` call is slow
#: enough (measured ~1.5s wall on a miss) to be worth caching, but the daemon
#: outlives a key rotation, so the cache expires instead of being permanent.
#: A failed lookup is never cached, so provisioning the item is picked up at
#: once rather than after a restart.
_KEY_CACHE_TTL_SECONDS = 300.0
#: ``(monotonic timestamp, key)`` of the last successful lookup, or ``None``.
#: Module-global rather than an :func:`functools.lru_cache`, whose previous use
#: here cached a ``None`` result for the life of the process.
_cached_key: tuple[float, str] | None = None
#: Serializes cache refresh/reset so a slower stale lookup cannot overwrite a
#: newer key observed concurrently during rotation.
_cache_lock = threading.Lock()


def reset_keychain_cache() -> None:
    """Forget any cached key so the next lookup re-runs ``security``.

    Exists for tests and for a caller that has just provisioned or rotated the
    keychain item; it never touches the item itself.
    """

    global _cached_key
    with _cache_lock:
        _cached_key = None


def keychain_glm_key() -> str | None:
    """Return the nonblank GLM Coding Plan key from Keychain, or ``None`` on failure.

    :returns: The trimmed key text, or ``None`` when the lookup command fails,
        times out, exits nonzero, or returns blank output. Command output is
        deliberately never included in an exception or log message, so the key
        can never be printed.

    A successful result is reused for ``_KEY_CACHE_TTL_SECONDS`` of monotonic
    time; a failure is not cached at all. So a rotated key is observed within
    the TTL, and a newly provisioned one immediately. Refresh and reset are
    serialized so concurrent callers cannot overwrite a newer observation.
    """

    global _cached_key
    with _cache_lock:
        now = time.monotonic()
        cached = _cached_key
        if cached is not None and now - cached[0] < _KEY_CACHE_TTL_SECONDS:
            return cached[1]
        value = _lookup_keychain_key()
        _cached_key = None if value is None else (now, value)
        return value


def _lookup_keychain_key() -> str | None:
    """Run the ``security`` lookup once, uncached.

    :returns: The trimmed key, or ``None`` when the command cannot run, times
        out after ``_KEYCHAIN_TIMEOUT_SECONDS``, exits nonzero (item absent or
        keychain locked), or prints only whitespace. Output is never logged.
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
