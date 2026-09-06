"""Pure assembly of owner-declared developer environments for adapters."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from dataclasses import replace
from pathlib import Path
from string import Formatter

from ..config import RuntimeConfig
from ..errors import ValidationError
from .rust import RUST_ENVIRONMENT_NAMES, rust_environment


_PROTECTED = frozenset({"HOME", "CODEX_HOME", "PATH", "ENV", "BASH_ENV", "ZDOTDIR", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME"})

#: Engine-owned Rust control variable. ``rust_environment`` deliberately never
#: sets or clears it so that a directory's own ``rust-toolchain`` pin is never
#: overridden; leaving it declarable here would let a preset silently plant
#: that same pin override and then leak back out through
#: ``configured_environment_keys``, which excludes it from the Rust key set
#: on the assumption that it is never one of this module's own values.
_RESERVED_CONTROL = frozenset({"RUSTUP_TOOLCHAIN"})


def developer_environment(environment: Mapping[str, str], config: RuntimeConfig, workdir: Path) -> dict[str, str]:
    """Return a child environment from an isolated baseline and one selected preset.

    The input mapping is copied and is the sole baseline; no ambient values are
    read. A selected preset prepends declared paths, expands only ``{workdir}``
    and ``{home}`` in declared variables, and checks required commands through
    the final PATH. Protected identity and shell-startup keys, the configured
    runtime's ``auth.names`` credentials, and the Rust toolchain-pin control
    variable ``RUSTUP_TOOLCHAIN`` are all rejected as preset variable names.
    Runtime Rust overrides preset Rust, and the shared Rust provisioner runs
    exactly once. Invalid declarations or missing required executables raise
    ``ValidationError`` without creating files or installing software.
    """

    result = dict(environment)
    selected = config.environment
    if selected is None:
        return rust_environment(result, config, workdir)
    auth_names = frozenset(config.auth.names) if config.auth is not None else frozenset()
    protected = sorted((_PROTECTED | _RESERVED_CONTROL | auth_names) & selected.variables.keys())
    if protected:
        raise ValidationError("environment variables may not override protected keys: " + ", ".join(protected))
    configured = [str(path) for path in selected.path]
    result["PATH"] = _join_path((*configured, *result.get("PATH", "").split(os.pathsep)))
    home = result.get("HOME", "")
    for name, value in selected.variables.items():
        result[name] = _expand(value, workdir, home)
    effective = config.rust if config.rust is not None else selected.rust
    result = rust_environment(result, replace(config, rust=effective), workdir)
    for command in selected.required_commands:
        if not _on_path(command, result.get("PATH", "")):
            raise ValidationError(f"environment required_commands declares missing executable: {command}")
    return result


def configured_environment_keys(config: RuntimeConfig) -> tuple[str, ...]:
    """Return deterministic explicit keys to forward to MCP subprocesses.

    A selected preset exposes PATH and its declared variables. Effective
    runtime-or-preset Rust adds its established keys even for legacy runtimes
    without a preset. The tuple contains no wildcard or ambient auth keys.
    """

    selected = config.environment
    has_rust = config.rust is not None or (selected is not None and selected.rust is not None)
    keys = ["PATH"] if selected is not None or has_rust else []
    if selected is not None:
        keys.extend(sorted(selected.variables))
    if has_rust:
        keys.extend(sorted(RUST_ENVIRONMENT_NAMES - {"PATH", "RUSTUP_TOOLCHAIN"}))
    return tuple(dict.fromkeys(keys))


def environment_digest(config: RuntimeConfig) -> str:
    """Return a stable digest of explicit environment and Rust declarations.

    Runtimes without a preset or legacy Rust return an empty string. Otherwise
    only declared paths, variables, command policies and effective Rust roots
    participate; ambient environment and authentication values are not read.
    """

    selected = config.environment
    if selected is None and config.rust is None:
        return ""
    rust = config.rust if config.rust is not None else selected.rust
    payload = {
        "path": [str(path) for path in selected.path] if selected is not None else [],
        "variables": dict(sorted(selected.variables.items())) if selected is not None else {},
        "required_commands": list(selected.required_commands) if selected is not None else [],
        "denied_commands": list(selected.denied_commands) if selected is not None else [],
        "rust": None if rust is None else {"rustup_home": str(rust.rustup_home), "cargo_bin": str(rust.cargo_bin)},
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _join_path(entries: tuple[str, ...]) -> str:
    """Join nonempty lexical PATH entries once, preserving first precedence."""

    return os.pathsep.join(dict.fromkeys(entry for entry in entries if entry))


def _expand(value: str, workdir: Path, home: str) -> str:
    """Expand only approved simple format fields in one declared value.

    Format conversions, format specifications, unknown names, and malformed
    format strings raise ``ValidationError`` rather than invoking arbitrary
    attribute or indexing syntax.
    """

    parts: list[str] = []
    try:
        parsed = Formatter().parse(value)
        for literal, field, specification, conversion in parsed:
            parts.append(literal)
            if field is None:
                continue
            if field not in {"workdir", "home"} or specification or conversion:
                raise ValidationError(f"environment variable template has unsupported field: {field}")
            parts.append(str(workdir) if field == "workdir" else home)
    except ValueError as error:
        raise ValidationError(f"environment variable template is invalid: {error}") from error
    return "".join(parts)


def _on_path(command: str, path: str) -> bool:
    """Return whether a bare command has an executable regular file on PATH."""

    return any((candidate := Path(directory) / command).is_file() and os.access(candidate, os.X_OK) for directory in path.split(os.pathsep) if directory)
