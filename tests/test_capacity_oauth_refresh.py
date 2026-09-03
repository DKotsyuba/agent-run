"""Mocked coverage for Claude OAuth renewal during capacity collection."""

from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest import mock

from agent_run.capacity import sources
from agent_run.config import RuntimeAuthConfig, RuntimeConfig
from agent_run.errors import AuthError, CapacitySourceError


def _runtime_config(*, auth: RuntimeAuthConfig | None = None) -> RuntimeConfig:
    """Build the smallest Claude runtime configuration used by these tests.

    ``auth`` is the optional declared runtime auth bridge.  The fictitious
    binary is never executed because every refresh is mocked.
    """

    return RuntimeConfig(
        enabled=True,
        adapter="claude:ADAPTER",
        binary=Path("/usr/bin/fake-claude"),
        home=Path("/tmp/fake-claude-home"),
        models=("model-a",),
        limits_source="native",
        auth=auth,
    )


class ClaudeCapacityOAuthRefreshTests(unittest.TestCase):
    """Exercise the collection-only Keychain renewal boundary."""

    def test_healthy_keychain_bypasses_refresh(self) -> None:
        """A validated Keychain token is returned without invoking the CLI."""

        with mock.patch.object(sources, "keychain_token", return_value="live-token") as read, mock.patch.object(
            sources, "refresh_keychain"
        ) as refresh:
            self.assertEqual(sources._claude_oauth_token(_runtime_config(), 100.0), "live-token")
        read.assert_called_once_with(100.0)
        refresh.assert_not_called()

    def test_missing_keychain_refreshes_once_then_rereads(self) -> None:
        """An unusable entry gets one bounded adapter refresh and a fresh read."""

        with mock.patch.object(
            sources, "keychain_token", side_effect=(None, "renewed-token")
        ) as read, mock.patch.object(sources, "refresh_keychain") as refresh:
            self.assertEqual(sources._claude_oauth_token(_runtime_config(), 100.0), "renewed-token")
        refresh.assert_called_once_with(Path("/usr/bin/fake-claude"))
        self.assertEqual(read.call_args_list, [mock.call(100.0), mock.call(100.0)])

    def test_refreshed_token_is_used_for_native_request(self) -> None:
        """The reread token becomes the usage request bearer credential."""

        captured: dict[str, str] = {}

        class Response:
            """Minimal successful HTTP response for the capacity reader."""

            def __enter__(self) -> "Response":
                """Return this response for the request context manager."""

                return self

            def __exit__(self, *_args: object) -> None:
                """Close no resources because this response owns none."""

                del _args

            def read(self) -> bytes:
                """Return an empty, valid native Claude limits payload."""

                return b'{"limits": []}'

        def urlopen(request: object, timeout: float, **_kwargs: object) -> Response:
            """Capture the request auth header without performing I/O."""

            del timeout, _kwargs
            captured["auth"] = request.headers["Authorization"]  # type: ignore[attr-defined]
            return Response()

        with mock.patch.object(sources, "keychain_token", side_effect=(None, "renewed-token")), mock.patch.object(
            sources, "refresh_keychain"
        ) as refresh, mock.patch.object(sources.urllib.request, "urlopen", side_effect=urlopen):
            self.assertEqual(sources._claude_native_samples(_runtime_config()), ())
        refresh.assert_called_once_with(Path("/usr/bin/fake-claude"))
        self.assertEqual(captured["auth"], "Bearer renewed-token")

    def test_refresh_failure_or_still_missing_maps_to_safe_static_code(self) -> None:
        """Refresh failure details and credential values do not escape collection."""

        secret = "oauth-secret-value"
        for refresh_side_effect, reads in ((AuthError("provider output " + secret), (None,)), (None, (None, None))):
            with self.subTest(refresh_side_effect=type(refresh_side_effect).__name__), mock.patch.object(
                sources, "keychain_token", side_effect=reads
            ), mock.patch.object(sources, "refresh_keychain", side_effect=refresh_side_effect), self.assertLogs(
                "agent_run.capacity", level="WARNING"
            ) as logs:
                with self.assertRaisesRegex(CapacitySourceError, "^claude_token_missing$") as raised:
                    sources._claude_native_samples(_runtime_config())
            self.assertNotIn(secret, str(raised.exception))
            self.assertNotIn(secret, "\n".join(logs.output))

    def test_declared_oauth_env_bypasses_keychain_and_refresh(self) -> None:
        """A declared nonempty OAuth environment value remains authoritative."""

        auth = RuntimeAuthConfig("env", names=("CLAUDE_CODE_OAUTH_TOKEN",))
        with mock.patch.dict(os.environ, {"CLAUDE_CODE_OAUTH_TOKEN": "env-token"}, clear=True), mock.patch.object(
            sources, "keychain_token"
        ) as read, mock.patch.object(sources, "refresh_keychain") as refresh:
            self.assertEqual(sources._claude_oauth_token(_runtime_config(auth=auth), 100.0), "env-token")
        read.assert_not_called()
        refresh.assert_not_called()

    def test_undeclared_oauth_and_api_key_do_not_bypass_keychain_refresh(self) -> None:
        """Only the declared OAuth variable can suppress the renewal path."""

        environment = {"CLAUDE_CODE_OAUTH_TOKEN": "undeclared", "ANTHROPIC_API_KEY": "api-key"}
        with mock.patch.dict(os.environ, environment, clear=True), mock.patch.object(
            sources, "keychain_token", side_effect=(None, "renewed-token")
        ), mock.patch.object(sources, "refresh_keychain") as refresh:
            self.assertEqual(sources._claude_oauth_token(_runtime_config(), 100.0), "renewed-token")
        refresh.assert_called_once_with(Path("/usr/bin/fake-claude"))
