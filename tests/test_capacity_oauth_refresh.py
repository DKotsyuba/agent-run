"""Coverage for Claude native-capacity credential isolation."""

from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest import mock

from agent_run.capacity import sources
from agent_run.config import RuntimeAuthConfig, RuntimeConfig
from agent_run.errors import CapacitySourceError


def _runtime_config(*, auth: RuntimeAuthConfig | None = None) -> RuntimeConfig:
    """Build the minimal Claude native-capacity runtime used by these tests.

    ``auth`` is the optional declared runtime environment bridge. The binary
    and home are placeholders because native capacity must not spawn Claude or
    inspect its scoped credential storage.
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
    """Ensure native capacity never reaches outside explicit OAuth auth."""

    def test_absent_explicit_oauth_is_unknown_without_cli_or_keychain_access(self) -> None:
        """Scoped CLI state is opaque rather than a global-token fallback."""

        self.assertIsNone(sources._claude_oauth_token(_runtime_config(), 100.0))

    def test_declared_oauth_environment_remains_authoritative(self) -> None:
        """A declared nonempty OAuth environment value can power native usage."""

        auth = RuntimeAuthConfig("environment", names=("CLAUDE_CODE_OAUTH_TOKEN",))
        with mock.patch.dict(os.environ, {"CLAUDE_CODE_OAUTH_TOKEN": "env-token"}, clear=True):
            self.assertEqual(sources._claude_oauth_token(_runtime_config(auth=auth), 100.0), "env-token")

    def test_native_capacity_reports_a_safe_fixed_failure_without_explicit_oauth(self) -> None:
        """No scoped credential read is attempted when native OAuth is unavailable."""

        with self.assertRaisesRegex(CapacitySourceError, "^claude_token_missing$"):
            sources._claude_native_samples(_runtime_config())
