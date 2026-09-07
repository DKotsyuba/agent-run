"""Explicit, non-installing Rust launch provisioning for compatible adapters."""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Mapping
from pathlib import Path

from ..config import RuntimeConfig
from ..errors import ValidationError


RUST_ENVIRONMENT_NAMES = frozenset(
    {"PATH", "RUSTUP_HOME", "CARGO_HOME", "RUSTUP_AUTO_INSTALL", "RUSTUP_TOOLCHAIN"}
)
_REQUIRED_EXECUTABLES = ("cargo", "rustc", "rustup", "rust-analyzer")
_REQUIRED_COMPONENTS = frozenset({"rust-analyzer", "rust-src"})
_PROBE_TIMEOUT_SECONDS = 5
_MIN_RUSTUP_VERSION = (1, 28, 1)


def rust_environment(
    environment: Mapping[str, str], config: RuntimeConfig, workdir: Path
) -> dict[str, str]:
    """Return a provisioned Rust environment for one configured launch.

    ``environment`` is the isolated string mapping built by the adapter,
    ``config`` supplies optional ``RustConfig``, and ``workdir`` is the launch
    root. Returns a new string mapping unchanged for absent provisioning;
    otherwise it preserves ``HOME``, prepends lexical ``cargo_bin`` to PATH,
    sets ``RUSTUP_HOME``, and sets ``CARGO_HOME`` to ``workdir/.cargo-home``.
    It sets ``RUSTUP_AUTO_INSTALL=0`` and runs bounded read-only probes only;
    it never creates the cache, downloads tools, selects a default toolchain,
    or overrides directory ``rust-toolchain`` pins. Invalid paths, proxy
    executables, rustup version, active toolchain, or components raise
    ``ValidationError`` with a bounded diagnostic.
    """

    if config.rust is None:
        return dict(environment)

    rustup_home = config.rust.rustup_home
    cargo_bin = config.rust.cargo_bin
    if not rustup_home.is_dir():
        raise ValidationError(f"rustup_home is not a directory: {rustup_home}")
    if not cargo_bin.is_dir():
        raise ValidationError(f"cargo_bin is not a directory: {cargo_bin}")
    missing = [name for name in _REQUIRED_EXECUTABLES if not _is_executable(cargo_bin / name)]
    if missing:
        raise ValidationError(f"cargo_bin lacks executable Rust proxies: {', '.join(missing)}")

    resolved_workdir = workdir.resolve()
    cargo_home = resolved_workdir / ".cargo-home"
    if cargo_home.is_symlink() or cargo_home.exists():
        if not cargo_home.resolve().is_relative_to(resolved_workdir):
            raise ValidationError(f"CARGO_HOME escapes workdir through symlink: {cargo_home}")
        if cargo_home.exists() and not cargo_home.is_dir():
            raise ValidationError(f"CARGO_HOME is not a directory: {cargo_home}")

    result = dict(environment)
    result.update(
        {
            "RUSTUP_HOME": str(rustup_home),
            "CARGO_HOME": str(cargo_home),
            "RUSTUP_AUTO_INSTALL": "0",
            "PATH": _prepend_path(cargo_bin, result.get("PATH")),
        }
    )
    _validate_rustup_version(
        _probe((cargo_bin / "rustup", "--version"), result, resolved_workdir, "rustup --version")
    )
    _probe((cargo_bin / "rustup", "show", "active-toolchain"), result, resolved_workdir, "rustup show")
    for executable in ("cargo", "rustc", "rust-analyzer"):
        _probe((cargo_bin / executable, "--version"), result, resolved_workdir, f"{executable} version")
    components = _probe(
        (cargo_bin / "rustup", "component", "list", "--installed"),
        result,
        resolved_workdir,
        "rustup component",
    )
    installed = {
        component
        for component in _REQUIRED_COMPONENTS
        if any(line == component or line.startswith(component + "-") for line in components.stdout.splitlines())
    }
    missing_components = sorted(_REQUIRED_COMPONENTS - installed)
    if missing_components:
        raise ValidationError(
            "active Rust toolchain lacks required components: " + ", ".join(missing_components)
        )
    return result


def _is_executable(path: Path) -> bool:
    """Return whether ``path`` is an executable lexical Cargo-bin proxy.

    ``path`` may be a symlink and is deliberately not resolved, preserving its
    argv0 identity. The function performs metadata reads only and returns
    ``False`` for missing, non-file, or non-executable paths.
    """

    return path.is_file() and os.access(path, os.X_OK)


def _validate_rustup_version(result: subprocess.CompletedProcess[str]) -> None:
    """Validate a successful ``rustup --version`` result before resolving pins.

    ``result.stdout`` must contain semantic version ``1.28.1`` or newer, the
    first release that honors ``RUSTUP_AUTO_INSTALL=0``. Returns ``None`` on a
    supported version; malformed or older output raises ``ValidationError``.
    """

    matched = re.search(r"\brustup\s+(\d+)\.(\d+)\.(\d+)\b", result.stdout)
    if matched is None:
        raise ValidationError("Rust provisioning requires a recognizable rustup version >= 1.28.1")
    version = tuple(int(value) for value in matched.groups())
    if version < _MIN_RUSTUP_VERSION:
        rendered = ".".join(str(value) for value in version)
        raise ValidationError(f"Rust provisioning requires rustup >= 1.28.1, found {rendered}")


def _prepend_path(directory: Path, inherited: str | None) -> str:
    """Return PATH text with lexical ``directory`` before optional ``inherited``.

    ``directory`` is rendered without resolving symlinks. An absent or empty
    inherited PATH returns only that directory; otherwise the platform path
    separator joins both values. This helper has no filesystem side effects.
    """

    return str(directory) if not inherited else str(directory) + os.pathsep + inherited


def _probe(
    command: tuple[Path | str, ...],
    environment: Mapping[str, str],
    workdir: Path,
    diagnostic_label: str,
) -> subprocess.CompletedProcess[str]:
    """Run one bounded read-only Rust command in ``workdir``.

    ``command`` contains a lexical executable followed by fixed arguments and
    ``environment`` is copied for the child. ``diagnostic_label`` is a fixed
    tool and operation label supplied by the caller, never derived from child
    output or an exception. Returns its text-mode completed process only on
    exit status zero. It starts one subprocess with a five second timeout and
    captures output without writing configuration; spawn, timeout, or
    nonzero-exit failures raise ``ValidationError`` with at most 1000 final
    diagnostic characters.
    """

    try:
        completed = subprocess.run(
            [str(part) for part in command],
            cwd=workdir,
            env=dict(environment),
            capture_output=True,
            text=True,
            timeout=_PROBE_TIMEOUT_SECONDS,
            check=False,
        )
    except OSError as error:
        raise ValidationError(f"Rust provisioning cannot execute {diagnostic_label}: {error}") from error
    except subprocess.TimeoutExpired as error:
        raise ValidationError(
            f"Rust provisioning timed out running {diagnostic_label} after {_PROBE_TIMEOUT_SECONDS}s"
        ) from error
    if completed.returncode:
        detail = _bounded_output(completed.stderr or completed.stdout)
        raise ValidationError(f"Rust provisioning cannot {diagnostic_label}: {detail or 'rustup failed'}")
    return completed


def _bounded_output(value: str) -> str:
    """Return the final 1000 characters of ``value`` as normalized one-line text.

    The input is external command output. This pure helper returns an empty
    string for empty input and keeps error messages bounded without logging or
    otherwise exposing a full child environment.
    """

    return " ".join(value[-1000:].split())
