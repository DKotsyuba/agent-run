"""Bounded plans for an agent-run-owned, isolated OpenCode v2 service.

The adapter never attaches to the user's global service and never falls back to
the per-run CLI. A service is usable only when a descriptor proves the running
process honors the generated XDG homes and the private loopback endpoint.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Iterable, Mapping

from ...config import RuntimeConfig
from ...errors import PathEscapeError, ValidationError
from ..home import write_managed_file


SERVICE_HOST = "127.0.0.1"
MIN_PORT = 1024
MAX_PORT = 65535
STARTUP_TIMEOUT_SECONDS = 20.0
STARTUP_POLL_SECONDS = 0.2
DESCRIPTOR_NAME = "service.json"

_ALLOWED_INHERITED = ("PATH",)
_FALLBACK_PATH = "/usr/bin:/bin"


class ServiceIsolationError(ValidationError):
    """A candidate service cannot be proven to use the generated home."""


@dataclass(frozen=True)
class ServiceDescriptor:
    """Private record of a managed service proven to be isolated."""

    host: str
    port: int
    config_home: Path
    data_home: Path
    pid: int | None = None
    version: str | None = None
    config_hash: str | None = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    def as_dict(self) -> dict[str, object]:
        return {
            "host": self.host,
            "port": self.port,
            "config_home": str(self.config_home),
            "data_home": str(self.data_home),
            "pid": self.pid,
            "version": self.version,
            "config_hash": self.config_hash,
        }


@dataclass(frozen=True)
class ServicePlan:
    """Everything needed to start one private service; starts nothing itself."""

    argv: tuple[str, ...]
    cwd: Path
    environment: Mapping[str, str]
    host: str
    port: int
    config_home: Path
    data_home: Path
    descriptor_path: Path
    startup_timeout_seconds: float = STARTUP_TIMEOUT_SECONDS
    poll_interval_seconds: float = STARTUP_POLL_SECONDS

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


def service_home_paths(home: str | Path) -> tuple[Path, Path]:
    """Return the generated (XDG_CONFIG_HOME, XDG_DATA_HOME) below ``home``."""

    root = _service_root(home)
    return root / "xdg" / "config", root / "xdg" / "data"


def descriptor_path(home: str | Path) -> Path:
    return _service_root(home) / DESCRIPTOR_NAME


def _service_root(home: str | Path) -> Path:
    try:
        path = Path(home).expanduser()
    except (TypeError, RuntimeError) as error:
        raise ValidationError("opencode home must be an absolute path") from error
    if not path.is_absolute():
        raise ValidationError("opencode home must be an absolute path")
    return path


def _port(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"opencode service port must be an integer in {MIN_PORT}..{MAX_PORT}")
    if not MIN_PORT <= value <= MAX_PORT:
        raise ValidationError(f"opencode service port must be an integer in {MIN_PORT}..{MAX_PORT}")
    return value


def _binary(config: RuntimeConfig) -> Path:
    binary = config.binary
    if not isinstance(binary, Path) or not binary.is_absolute():
        raise ValidationError("opencode binary must be an absolute path")
    if not binary.is_file():
        raise ValidationError(f"opencode binary does not exist: {binary}")
    return binary


def _refuse_global_attach(inherited: Mapping[str, str]) -> None:
    for name in sorted(inherited):
        if name.startswith("OPENCODE_"):
            raise ServiceIsolationError(
                f"refusing to inherit {name}; the managed opencode service never "
                "attaches to a global service"
            )


def _environment(
    config: RuntimeConfig,
    root: Path,
    config_home: Path,
    data_home: Path,
    inherited: Mapping[str, str],
) -> Mapping[str, str]:
    _refuse_global_attach(inherited)
    environment: dict[str, str] = {
        "HOME": str(root),
        "XDG_CONFIG_HOME": str(config_home),
        "XDG_DATA_HOME": str(data_home),
        "XDG_STATE_HOME": str(root / "xdg" / "state"),
        "XDG_CACHE_HOME": str(root / "xdg" / "cache"),
        "OPENCODE_DISABLE_CLAUDE_CODE": "1",
    }
    for name in _ALLOWED_INHERITED:
        environment[name] = inherited.get(name) or _FALLBACK_PATH
    auth = config.auth
    if auth is not None:
        if auth.kind != "environment":
            raise ValidationError("opencode auth must use kind = 'environment'")
        for name in auth.names:
            value = inherited.get(name)
            if value is None:
                raise ValidationError(f"opencode auth environment variable is not set: {name}")
            environment[name] = value
    return MappingProxyType(environment)


def build_service_plan(
    config: RuntimeConfig,
    home: str | Path,
    *,
    port: int,
    inherited_environment: Mapping[str, str] | Iterable[tuple[str, str]] = (),
) -> ServicePlan:
    """Describe one private service; no process, socket, or file is touched."""

    if not isinstance(config, RuntimeConfig):
        raise ValidationError("opencode service plan requires a RuntimeConfig")
    if config.service_mode != "managed":
        raise ValidationError(
            "opencode requires service_mode = 'managed'; there is no CLI or global fallback"
        )
    binary = _binary(config)
    number = _port(port)
    root = _service_root(home)
    config_home, data_home = service_home_paths(root)
    inherited = dict(inherited_environment)
    environment = _environment(config, root, config_home, data_home, inherited)
    argv = (
        str(binary),
        "serve",
        "--hostname",
        SERVICE_HOST,
        "--port",
        str(number),
    )
    return ServicePlan(
        argv=argv,
        cwd=root,
        environment=environment,
        host=SERVICE_HOST,
        port=number,
        config_home=config_home,
        data_home=data_home,
        descriptor_path=descriptor_path(root),
    )


def _reported_path(reported: Mapping[str, object], key: str) -> Path:
    value = reported.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ServiceIsolationError(f"service did not report {key}")
    try:
        path = Path(value).expanduser()
    except RuntimeError as error:
        raise ServiceIsolationError(f"service reported an unusable {key}: {value}") from error
    if not path.is_absolute():
        raise ServiceIsolationError(f"service reported a relative {key}: {value}")
    try:
        return path.resolve()
    except (OSError, RuntimeError) as error:
        raise ServiceIsolationError(f"service reported an unusable {key}: {value}") from error


def _contained(candidate: Path, expected: Path, key: str) -> None:
    try:
        expected_root = expected.resolve()
    except (OSError, RuntimeError) as error:
        raise ServiceIsolationError(f"generated {key} is unusable: {expected}") from error
    if candidate != expected_root and not candidate.is_relative_to(expected_root):
        raise ServiceIsolationError(
            f"service {key} {candidate} is outside the generated home {expected_root}; "
            "the opencode runtime stays unavailable rather than attaching to a global service"
        )


def verify_isolation(plan: ServicePlan, reported: Mapping[str, object]) -> ServiceDescriptor:
    """Prove a candidate service uses the generated homes and private endpoint."""

    if not isinstance(plan, ServicePlan):
        raise ValidationError("isolation proof requires a ServicePlan")
    if not isinstance(reported, Mapping):
        raise ServiceIsolationError("service reported no isolation payload")
    _contained(_reported_path(reported, "config_home"), plan.config_home, "config_home")
    _contained(_reported_path(reported, "data_home"), plan.data_home, "data_home")
    host = reported.get("host", plan.host)
    port = reported.get("port")
    if host != plan.host:
        raise ServiceIsolationError(f"service answers on {host!r}, expected {plan.host!r}")
    if port != plan.port:
        raise ServiceIsolationError(f"service answers on port {port!r}, expected {plan.port}")
    pid = reported.get("pid")
    if pid is not None and (isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0):
        raise ServiceIsolationError(f"service reported an invalid pid: {pid!r}")
    version = reported.get("version")
    if version is not None and not isinstance(version, str):
        raise ServiceIsolationError("service reported an invalid version")
    return ServiceDescriptor(
        host=plan.host,
        port=plan.port,
        config_home=plan.config_home,
        data_home=plan.data_home,
        pid=pid,
        version=version,
    )


def write_service_descriptor(home: str | Path, descriptor: ServiceDescriptor) -> str:
    """Record a proven descriptor as one private file below the generated home."""

    if not isinstance(descriptor, ServiceDescriptor):
        raise ValidationError("service descriptor must be a ServiceDescriptor")
    content = json.dumps(descriptor.as_dict(), indent=2, sort_keys=True) + "\n"
    return write_managed_file(_service_root(home), DESCRIPTOR_NAME, content)


def read_service_descriptor(home: str | Path) -> ServiceDescriptor | None:
    """Return the proven descriptor, or None when isolation was never proven."""

    path = descriptor_path(home)
    if path.is_symlink():
        raise PathEscapeError(f"service descriptor must not be a symlink: {path}")
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ValidationError(f"cannot read service descriptor {path}: {error}") from error
    try:
        payload = json.loads(raw)
    except ValueError as error:
        raise ValidationError(f"service descriptor is not valid JSON: {path}") from error
    if not isinstance(payload, dict):
        raise ValidationError(f"service descriptor must be a JSON object: {path}")
    config_home, data_home = service_home_paths(home)
    descriptor = ServiceDescriptor(
        host=str(payload.get("host", "")),
        port=payload.get("port"),
        config_home=Path(str(payload.get("config_home", ""))),
        data_home=Path(str(payload.get("data_home", ""))),
        pid=payload.get("pid"),
        version=payload.get("version"),
        config_hash=payload.get("config_hash"),
    )
    if descriptor.host != SERVICE_HOST:
        raise ServiceIsolationError(
            f"recorded service host {descriptor.host!r} is not the private loopback endpoint"
        )
    _port(descriptor.port)
    _contained(Path(descriptor.config_home).resolve(), config_home, "config_home")
    _contained(Path(descriptor.data_home).resolve(), data_home, "data_home")
    return descriptor
