"""Contract checks for runtime host-environment inheritance."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.environment import host_environment


class HostEnvironmentTests(unittest.TestCase):
    """Verify toolchain inheritance without unrelated credential export."""

    def test_inherits_host_tools_and_only_selected_secrets(self) -> None:
        """Preserve host tooling while filtering unselected credential values."""

        parent = {
            "PATH": "/host/bin",
            "LANG": "en_US.UTF-8",
            "RUSTUP_HOME": "/host/rustup",
            "UNRELATED_TOKEN": "drop-me",
            "SELECTED_API_KEY": "keep-me",
            "SSH_AUTH_SOCK": "/tmp/agent.sock",
            "KUBECONFIG": "/home/user/.kube/config",
            "AWS_PROFILE": "production",
            "NPM_CONFIG_USERCONFIG": "/home/user/.npmrc",
            "SDKROOT": "/host/sdk",
        }
        with patch.dict(os.environ, parent, clear=True):
            environment = host_environment(
                {"HOME": "/generated/home"},
                allowed_secret_names=("SELECTED_API_KEY",),
            )

        self.assertEqual(environment["PATH"], "/host/bin")
        self.assertEqual(environment["RUSTUP_HOME"], "/host/rustup")
        self.assertEqual(environment["SDKROOT"], "/host/sdk")
        self.assertEqual(environment["HOME"], "/generated/home")
        self.assertEqual(environment["SELECTED_API_KEY"], "keep-me")
        self.assertNotIn("UNRELATED_TOKEN", environment)
        for carrier in (
            "SSH_AUTH_SOCK", "KUBECONFIG", "AWS_PROFILE", "NPM_CONFIG_USERCONFIG"
        ):
            self.assertNotIn(carrier, environment)

    def test_selected_credential_carrier_is_forwarded(self) -> None:
        """Allow an otherwise filtered carrier only when the contract names it."""

        with patch.dict(os.environ, {"SSH_AUTH_SOCK": "/tmp/selected.sock"}, clear=True):
            environment = host_environment(
                {}, allowed_secret_names=("SSH_AUTH_SOCK",)
            )
        self.assertEqual(environment["SSH_AUTH_SOCK"], "/tmp/selected.sock")


if __name__ == "__main__":
    unittest.main()
