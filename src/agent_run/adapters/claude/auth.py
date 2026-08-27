"""OAuth credential acquisition for the ``claude`` runtime.

An explicitly exported auth variable always wins. Otherwise the adapter
reads the macOS Keychain entry the Claude Code CLI maintains for itself,
and when that entry is missing or expired it runs the bare CLI exactly
once with every auth variable stripped from the child's environment --
which is what makes the CLI refresh the Keychain instead of trusting an
inherited value. At most one refresh per launch: a stale credential is a
hard launch failure, never a retry loop.

No function here returns, raises, or logs anything derived from a token
value; the token is handed straight to the caller's environment mapping,
whose name is already registered as a secret for stream sanitization.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path

from ...errors import AuthError

__all__ = ["TOKEN_ENV_NAME", "keychain_token", "refresh_keychain", "resolve_token"]

TOKEN_ENV_NAME = "CLAUDE_CODE_OAUTH_TOKEN"

#: Auth variables that must not reach the refresh child: with any one of
#: them set the CLI uses it verbatim and never touches the Keychain.
AUTH_ENV_NAMES = ("CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN")

_SECURITY_BIN = "/usr/bin/security"
_KEYCHAIN_SERVICE = "Claude Code-credentials"
_READ_TIMEOUT_SECONDS = 15.0
_REFRESH_TIMEOUT_SECONDS = 60.0
# Treat a token that expires within the margin as already stale: one that
# dies mid-run is no more useful than one that died before it.
_EXPIRY_MARGIN_SECONDS = 300.0


def keychain_token(now: float) -> str | None:
    """Return a live access token from the Keychain, or ``None``.

    Every failure mode -- no entry, a locked keychain, malformed JSON, a
    past ``expiresAt`` -- collapses to ``None``, meaning "no usable
    token"; the caller decides whether to refresh. ``stderr`` is captured
    rather than inherited so a Keychain diagnostic cannot land in a log.
    """

    try:
        completed = subprocess.run(
            [_SECURITY_BIN, "find-generic-password", "-s", _KEYCHAIN_SERVICE, "-w"],
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_READ_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    try:
        payload = json.loads(completed.stdout)
    except (TypeError, ValueError):
        return None
    oauth = payload.get("claudeAiOauth") if isinstance(payload, dict) else None
    if not isinstance(oauth, dict):
        return None
    token = oauth.get("accessToken")
    if not isinstance(token, str) or not token.strip():
        return None
    expires_at = oauth.get("expiresAt")
    if isinstance(expires_at, (int, float)) and not isinstance(expires_at, bool):
        # The CLI stores milliseconds since the epoch.
        if expires_at / 1000.0 - _EXPIRY_MARGIN_SECONDS <= now:
            return None
    return token


def refresh_keychain(binary: Path) -> None:
    """Run the bare CLI once so it refreshes the Keychain entry itself.

    Bounded by a hard timeout and never inspected for output: whether the
    refresh worked is decided by re-reading the Keychain, not by trusting
    the child's exit status.
    """

    # ponytail: no cross-process lock, so N launches racing on the same
    # stale token each spend one refresh. Bounded and idempotent -- add a
    # lock under <home>/locks only if that ever shows up as real cost.
    environment = {
        name: value for name, value in os.environ.items() if name not in AUTH_ENV_NAMES
    }
    try:
        subprocess.run(
            [str(binary), "--print", "ping"],
            env=environment,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=_REFRESH_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.SubprocessError) as error:
        # Only the exception class, never its message: a subprocess error
        # can quote the child's own output back at us.
        raise AuthError(
            "claude runtime could not run the credential refresh: "
            f"{type(error).__name__}"
        ) from None


def resolve_token(binary: Path, *, now: float | None = None) -> str:
    """Return a live OAuth token, refreshing the Keychain at most once."""

    at = time.time() if now is None else now
    token = keychain_token(at)
    if token is not None:
        return token
    refresh_keychain(binary)
    token = keychain_token(time.time() if now is None else now)
    if token is None:
        raise AuthError(
            "claude runtime has no usable credential: the macOS Keychain entry "
            f"{_KEYCHAIN_SERVICE!r} is missing or expired and the bare-CLI "
            "refresh did not renew it"
        )
    return token
