"""Read the managed OpenCode server password without exposing its value."""

from __future__ import annotations

from functools import lru_cache
import subprocess


_SECURITY_COMMAND = (
    "/usr/bin/security",
    "find-generic-password",
    "-a",
    "OPENCODE_SERVER_PASSWORD",
    "-s",
    "com.pluto.agent-run.opencode.service",
    "-w",
)
_KEYCHAIN_TIMEOUT_SECONDS = 3


@lru_cache(maxsize=1)
def keychain_server_password() -> str | None:
    """Return the nonblank managed password from Keychain, or ``None`` on failure.

    The lookup result is cached for the process to avoid repeatedly launching
    the Keychain command. Command output is deliberately not propagated so a
    password can never appear in an exception or log message.
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
