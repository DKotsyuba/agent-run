"""Regression coverage for opt-in Rust environment provisioning."""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.rust import rust_environment
from agent_run.config import RuntimeConfig, RustConfig
from agent_run.errors import ValidationError


class RustProvisioningTests(unittest.TestCase):
    """Verify provisioning remains explicit, isolated, and non-installing."""

    def setUp(self) -> None:
        """Create one isolated lexical Cargo-bin proxy directory per test."""

        self._temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary_directory.cleanup)
        self.root = Path(self._temporary_directory.name).resolve()
        self.workdir = self.root / "work"
        self.workdir.mkdir()
        self.rustup_home = self.root / "rustup"
        self.rustup_home.mkdir()
        self.cargo_bin = self.root / "cargo-bin"
        self.cargo_bin.mkdir()
        for name in ("cargo", "rustc", "rust-analyzer"):
            self._proxy(name, "#!/bin/sh\nexit 0\n")
        self._proxy(
            "rustup",
            "#!/bin/sh\n"
            "if [ \"$1\" = --version ]; then echo 'rustup 1.28.1'; exit 0; fi\n"
            "if [ \"$1\" = show ]; then echo '1.98.1 (active)'; exit 0; fi\n"
            "printf '%s\\n' rust-src rust-analyzer\n",
        )

    def _proxy(self, name: str, text: str) -> None:
        """Write one executable lexical proxy used by the bounded fake rustup."""

        path = self.cargo_bin / name
        path.write_text(text, encoding="utf-8")
        path.chmod(0o755)

    def config(self, rust: RustConfig | None) -> RuntimeConfig:
        """Build the minimal runtime value required by the shared helper."""

        return RuntimeConfig(
            enabled=True,
            adapter="agent_run.adapters.claude.adapter:ADAPTER",
            binary=Path("/bin/echo"),
            home=self.root / "home",
            models=("test",),
            rust=rust,
        )

    def test_provisioning_precedes_ambient_paths_without_creating_cargo_home(self) -> None:
        """Configured roots win over ambient values while preparation creates no cache."""

        environment = rust_environment(
            {"HOME": str(self.root / "home"), "PATH": "/ambient/bin", "RUSTUP_HOME": "/stale"},
            self.config(RustConfig(self.rustup_home, self.cargo_bin)),
            self.workdir,
        )
        self.assertEqual(environment["RUSTUP_HOME"], str(self.rustup_home))
        self.assertEqual(environment["CARGO_HOME"], str(self.workdir / ".cargo-home"))
        self.assertEqual(environment["RUSTUP_AUTO_INSTALL"], "0")
        self.assertEqual(environment["PATH"], str(self.cargo_bin) + os.pathsep + "/ambient/bin")
        self.assertFalse((self.workdir / ".cargo-home").exists())

    def test_empty_declaration_leaves_environment_unchanged(self) -> None:
        """Absent Rust configuration preserves the pre-existing child environment."""

        original = {"HOME": str(self.root / "home"), "PATH": "/ambient/bin"}
        self.assertEqual(rust_environment(original, self.config(None), self.workdir), original)

    def test_symlinked_cargo_home_cannot_escape_workdir(self) -> None:
        """An existing cache symlink outside the request root fails before probing Rustup."""

        outside = self.root / "outside"
        outside.mkdir()
        (self.workdir / ".cargo-home").symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ValidationError, "escapes workdir"):
            rust_environment({}, self.config(RustConfig(self.rustup_home, self.cargo_bin)), self.workdir)

    def test_missing_analyzer_component_fails_without_an_install(self) -> None:
        """A selected toolchain without analyzer support reports a local preflight failure."""

        self._proxy(
            "rustup",
            "#!/bin/sh\n"
            "if [ \"$1\" = --version ]; then echo 'rustup 1.28.1'; exit 0; fi\n"
            "if [ \"$1\" = show ]; then echo active; exit 0; fi\n"
            "printf '%s\\n' rust-src\n",
        )
        with self.assertRaisesRegex(ValidationError, "rust-analyzer"):
            rust_environment({}, self.config(RustConfig(self.rustup_home, self.cargo_bin)), self.workdir)

    def test_old_rustup_is_rejected_before_toolchain_resolution(self) -> None:
        """Rustup releases that may ignore AUTO_INSTALL fail before the active-toolchain query."""

        self._proxy("rustup", "#!/bin/sh\necho 'rustup 1.28.0'\n")
        with self.assertRaisesRegex(ValidationError, "rustup >= 1.28.1"):
            rust_environment({}, self.config(RustConfig(self.rustup_home, self.cargo_bin)), self.workdir)

    def test_probe_failures_use_actual_tool_operation_labels(self) -> None:
        """Public provisioning diagnostics identify each failing Rust probe."""

        cases = (
            ("rustup", "rustup --version", "if [ \"$1\" = --version ]; then exit 1; fi\n"),
            ("rustup", "rustup show", "if [ \"$1\" = --version ]; then echo 'rustup 1.28.1'; exit 0; fi\nif [ \"$1\" = show ]; then exit 1; fi\n"),
            ("rustup", "rustup component", "if [ \"$1\" = --version ]; then echo 'rustup 1.28.1'; exit 0; fi\nif [ \"$1\" = show ]; then echo active; exit 0; fi\nif [ \"$1\" = component ]; then exit 1; fi\n"),
            ("cargo", "cargo --version", "exit 1\n"),
            ("rustc", "rustc --version", "exit 1\n"),
            ("rust-analyzer", "rust-analyzer --version", "exit 1\n"),
        )
        for name, label, failure in cases:
            with self.subTest(label=label):
                for executable in ("cargo", "rustc", "rust-analyzer"):
                    self._proxy(executable, "#!/bin/sh\nexit 0\n")
                self._proxy(
                    "rustup",
                    "#!/bin/sh\n"
                    "if [ \"$1\" = --version ]; then echo 'rustup 1.28.1'; exit 0; fi\n"
                    "if [ \"$1\" = show ]; then echo active; exit 0; fi\n"
                    "printf '%s\\n' rust-src rust-analyzer\n",
                )
                self._proxy(name, "#!/bin/sh\n" + failure)
                with self.assertRaisesRegex(ValidationError, rf"cannot {label}:"):
                    rust_environment({}, self.config(RustConfig(self.rustup_home, self.cargo_bin)), self.workdir)

    def test_probe_timeout_and_oserror_use_actual_tool_operation_label(self) -> None:
        """Public provisioning maps timeout and spawn failures to fixed labels."""

        with mock.patch(
            "agent_run.adapters.rust.subprocess.run",
            side_effect=subprocess.TimeoutExpired(["rustup", "--version"], 5),
        ), self.assertRaisesRegex(ValidationError, r"timed out running rustup --version after 5s"):
            rust_environment({}, self.config(RustConfig(self.rustup_home, self.cargo_bin)), self.workdir)
        with mock.patch(
            "agent_run.adapters.rust.subprocess.run", side_effect=OSError("spawn failed")
        ), self.assertRaisesRegex(ValidationError, r"cannot execute rustup --version:"):
            rust_environment({}, self.config(RustConfig(self.rustup_home, self.cargo_bin)), self.workdir)
