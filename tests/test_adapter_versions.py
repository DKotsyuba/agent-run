"""Focused checks for bounded configured-binary version observation."""

from __future__ import annotations

from pathlib import Path

import pytest

from agent_run.adapters import version as version_module
from agent_run.adapters.version import observe_binary_version


def _executable(tmp_path: Path, body: str) -> Path:
    """Create one executable shell fixture with ``body`` after its shebang."""

    path = tmp_path / "runtime"
    path.write_text(f"#!/bin/sh\n{body}\n", encoding="utf-8")
    path.chmod(0o700)
    return path


def test_version_is_fresh_and_not_cached(tmp_path: Path) -> None:
    """Each call executes the configured binary and observes changed output."""

    binary = _executable(tmp_path, "printf 'runtime 1.0\\n'")
    assert observe_binary_version(binary, tmp_path) == ("runtime 1.0", None)
    binary.write_text("#!/bin/sh\nprintf 'runtime 2.0\\n'\n", encoding="utf-8")
    assert observe_binary_version(binary, tmp_path) == ("runtime 2.0", None)


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        ("exit 7", "status 7"),
        ("printf 'stderr-only\\n' >&2", "no version"),
        ("i=0; while [ $i -lt 5000 ]; do printf x; i=$((i + 1)); done", "exceeds 4096"),
    ],
)
def test_version_failures_are_bounded(tmp_path: Path, body: str, reason: str) -> None:
    """Nonzero, stderr-only, and oversized results return bounded diagnostics."""

    version, diagnostic = observe_binary_version(_executable(tmp_path, body), tmp_path)
    assert version is None
    assert diagnostic is not None and reason in diagnostic


def test_missing_and_timed_out_commands_are_reaped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Missing and stalled commands return after owned process cleanup."""

    assert observe_binary_version(tmp_path / "missing", tmp_path) == (
        None,
        "version command could not be started",
    )
    monkeypatch.setattr(version_module, "_VERSION_TIMEOUT_SECONDS", 0.05)
    version, diagnostic = observe_binary_version(
        _executable(tmp_path, "sleep 60"), tmp_path
    )
    assert version is None
    assert diagnostic == "version command timed out after 0.05 seconds"


def test_version_command_receives_no_ambient_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The child environment contains only isolated HOME and PATH values."""

    monkeypatch.setenv("AGENT_RUN_TEST_SECRET", "sk-do-not-pass")
    binary = _executable(
        tmp_path,
        "if [ -n \"$AGENT_RUN_TEST_SECRET\" ]; then printf leaked; else printf 'clean 1\\n'; fi",
    )
    assert observe_binary_version(binary, tmp_path) == ("clean 1", None)
