"""Regression tests for OpenCode Keychain password fallback."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest import mock

from agent_run.adapters.opencode import password, service


class OpenCodeKeychainPasswordTests(unittest.TestCase):
    """Exercise password resolution through the proven service attachment path."""

    def setUp(self) -> None:
        """Reset the process-local fallback cache before each independent test."""

        password.keychain_server_password.cache_clear()

    def _proven_service(self, home: Path) -> Path:
        """Create a descriptor and generated config that the adapter will attach to."""

        config_path = home / "opencode.json"
        config_path.write_text('{"generated": true}\n', encoding="utf-8")
        config_home, data_home = service.service_home_paths(home)
        service.write_service_descriptor(
            home,
            service.ServiceDescriptor(
                host=service.SERVICE_HOST,
                port=41777,
                config_home=config_home,
                data_home=data_home,
                pid=os.getpid(),
                config_hash=service.config_file_hash(config_path),
                version="2.1.0",
            ),
        )
        return config_path

    def test_environment_password_wins_without_keychain_lookup(self) -> None:
        """Attach with an environment password without starting a subprocess."""

        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            config_path = self._proven_service(home)
            with mock.patch.dict(os.environ, {service.PASSWORD_ENV: "environment-secret"}, clear=True):
                with mock.patch.object(password.subprocess, "run") as run:
                    self.assertEqual(service.attach_service(home, config_path).pid, os.getpid())
            run.assert_not_called()

    def test_blank_environment_falls_back_to_keychain_for_attach(self) -> None:
        """Attach when a blank environment password is replaced by Keychain."""

        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            config_path = self._proven_service(home)
            completed = subprocess.CompletedProcess([], 0, stdout="keychain-secret\n")
            with mock.patch.dict(os.environ, {service.PASSWORD_ENV: "   "}, clear=True):
                with mock.patch.object(password.subprocess, "run", return_value=completed) as run:
                    self.assertEqual(service.attach_service(home, config_path).pid, os.getpid())
            self.assertEqual(run.call_count, 1)
            self.assertEqual(run.call_args.args[0], password._SECURITY_COMMAND)

    def test_failed_keychain_fallback_keeps_original_secret_safe_error(self) -> None:
        """Reject failed fallback without disclosing command output in the error."""

        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary)
            config_path = self._proven_service(home)
            completed = subprocess.CompletedProcess([], 1, stdout="do-not-leak-this-secret")
            with mock.patch.dict(os.environ, {service.PASSWORD_ENV: "   "}, clear=True):
                with mock.patch.object(password.subprocess, "run", return_value=completed):
                    with self.assertRaises(service.ServiceIsolationError) as caught:
                        service.attach_service(home, config_path)
            self.assertEqual(
                str(caught.exception),
                f"{service.PASSWORD_ENV} must be set to a nonblank value",
            )
            self.assertNotIn("do-not-leak-this-secret", str(caught.exception))


if __name__ == "__main__":
    unittest.main()
