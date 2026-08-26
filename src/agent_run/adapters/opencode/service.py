"""Bounded plans for an agent-run-owned, isolated OpenCode v2 service.

The adapter never attaches to the user's global service and never falls back to
the per-run CLI. A service is usable only when a descriptor proves the running
process honors the generated XDG homes and the private loopback endpoint, and
only while that proof still holds: every attach re-proves the recorded pid, the
endpoint, the generated homes, and the hash of the generated config file.
"""

from __future__ import annotations

import json
import os
import re
import socket
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Iterable, Mapping

from ...config import RuntimeConfig
from ...errors import PathEscapeError, ValidationError
from ...lifecycle import (
    ProcessOps,
    SystemProcessOps,
    Termination,
    terminate_process_group,
    verify_process_group,
)
from ..home import content_hash, write_managed_file


SERVICE_HOST = "127.0.0.1"
PASSWORD_ENV = "OPENCODE_SERVER_PASSWORD"
MIN_PORT = 1024
MAX_PORT = 65535
STARTUP_TIMEOUT_SECONDS = 20.0
STARTUP_POLL_SECONDS = 0.2
DESCRIPTOR_NAME = "service.json"
SERVICE_LOG_NAME = "service.log"
CONFIG_RELATIVE_PATH = "xdg/config/opencode/opencode.json"
CONFIG_API_PATH = "/config"
GLOBAL_HEALTH_PATH = "/global/health"
#: Refuses an upgraded or misdirected candidate instead of a drifted contract.
PINNED_VERSION = "1.18.18"
PROBE_TIMEOUT_SECONDS = 0.5
TERMINATE_GRACE_SECONDS = 5.0
KILL_GRACE_SECONDS = 2.0

_AUTH_REFUSED = frozenset({401, 403})

#: The child never inherits PATH; a fixed one keeps argv resolution reproducible.
SERVICE_PATH = "/usr/local/bin:/usr/bin:/bin"
#: Any ambient variable under this prefix can point the child at a global service.
ATTACH_PREFIX = "OPENCODE_"

_ENV_NAME = re.compile(r"[A-Z_][A-Z0-9_]*\Z")
_HEX64 = re.compile(r"[0-9a-f]{64}\Z")


class ServiceIsolationError(ValidationError):
    """A candidate service cannot be proven to use the generated home."""


def require_server_password(value: str | None = None) -> str:
    """Return the managed-service password without persisting it."""

    candidate = os.environ.get(PASSWORD_ENV) if value is None else value
    if not isinstance(candidate, str) or not candidate.strip():
        raise ServiceIsolationError(f"{PASSWORD_ENV} must be set to a nonblank value")
    return candidate


@dataclass(frozen=True)
class ServiceDescriptor:
    """Private record of a managed service proven to be isolated."""

    host: str
    port: int
    config_home: Path
    data_home: Path
    pid: int
    config_hash: str
    version: str | None = None

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
            "config_hash": self.config_hash,
            "version": self.version,
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


def _pid(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ServiceIsolationError(f"service reported an invalid pid: {value!r}")
    return value


def _config_hash(value: object) -> str:
    if not isinstance(value, str) or not _HEX64.fullmatch(value):
        raise ServiceIsolationError(
            "service isolation proof requires the sha256 of the generated config"
        )
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
        if name != PASSWORD_ENV and name.startswith(ATTACH_PREFIX):
            raise ServiceIsolationError(
                f"refusing to inherit {name}; the managed opencode service never "
                "attaches to a global service"
            )


def resolve_environment_names(
    names: Iterable[str], inherited: Mapping[str, str], *, what: str
) -> dict[str, str]:
    """Read declared variable names out of an ambient environment, or refuse.

    Only names are ever configured: a literal secret in config, an unset name,
    or a name that could redirect the child at a global service is refused.
    """

    resolved: dict[str, str] = {}
    for name in names:
        if not isinstance(name, str) or not _ENV_NAME.fullmatch(name):
            raise ValidationError(
                f"opencode {what} must name an environment variable, not a secret value: {name!r}"
            )
        if name.startswith(ATTACH_PREFIX):
            raise ServiceIsolationError(
                f"refusing to forward {name} as {what}; it can redirect the child "
                "at a global opencode service"
            )
        value = inherited.get(name)
        if not isinstance(value, str) or not value.strip():
            raise ValidationError(f"opencode {what} environment variable is not set: {name}")
        resolved[name] = value
    return resolved


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
        "OPENCODE_DISABLE_AUTOUPDATE": "true",
        # Never inherited: an ambient PATH makes the launched argv nondeterministic.
        "PATH": SERVICE_PATH,
    }
    environment[PASSWORD_ENV] = require_server_password(inherited.get(PASSWORD_ENV))
    auth = config.auth
    if auth is not None:
        if auth.kind != "environment":
            raise ValidationError("opencode auth must use kind = 'environment'")
        environment.update(resolve_environment_names(auth.names, inherited, what="auth"))
    return MappingProxyType(environment)


def build_service_plan(
    config: RuntimeConfig,
    home: str | Path,
    *,
    port: int,
    inherited_environment: Mapping[str, str] | Iterable[tuple[str, str]] = (),
    argv: tuple[str, ...] | None = None,
) -> ServicePlan:
    """Describe one private service; no process, socket, or file is touched.

    ``argv`` is empty when a proven service already serves this endpoint: the
    adapter must never start a second ``serve`` against the same home.
    """

    if not isinstance(config, RuntimeConfig):
        raise ValidationError("opencode service plan requires a RuntimeConfig")
    if config.service_mode != "managed":
        raise ValidationError(
            "opencode requires service_mode = 'managed'; there is no CLI or global fallback"
        )
    number = _port(port)
    root = _service_root(home)
    config_home, data_home = service_home_paths(root)
    inherited = dict(inherited_environment)
    environment = _environment(config, root, config_home, data_home, inherited)
    if argv is None:
        argv = (str(_binary(config)), "serve", "--hostname", SERVICE_HOST, "--port", str(number))
    return ServicePlan(
        argv=tuple(argv),
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


def verify_isolation(
    plan: ServicePlan, health: Mapping[str, object], *, pid: int, config_hash: str
) -> ServiceDescriptor:
    """Prove the exact spawned service answered its authenticated health endpoint.

    v1's ``/api/health`` never reports "pid" (only ``{"healthy": true}``), so
    its absence proves nothing either way; the exclusive port, the private
    password, and the config-sentinel/version-pin checks carry isolation
    instead when it is missing.
    """

    if not isinstance(plan, ServicePlan):
        raise ValidationError("isolation proof requires a ServicePlan")
    if not isinstance(health, Mapping) or health.get("healthy") is not True:
        raise ServiceIsolationError("service reported unhealthy or invalid health payload")
    candidate_pid = _pid(pid)
    if "pid" in health:
        reported_pid = _pid(health.get("pid"))
        if reported_pid != candidate_pid:
            raise ServiceIsolationError(
                f"service health pid {reported_pid} does not match spawned pid {candidate_pid}"
            )
    version = health.get("version")
    if version is not None and not isinstance(version, str):
        raise ServiceIsolationError("service reported an invalid version")
    return ServiceDescriptor(
        host=plan.host,
        port=plan.port,
        config_home=plan.config_home,
        data_home=plan.data_home,
        pid=candidate_pid,
        config_hash=_config_hash(config_hash),
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
    version = payload.get("version")
    descriptor = ServiceDescriptor(
        host=str(payload.get("host", "")),
        port=_port(payload.get("port")),
        config_home=Path(str(payload.get("config_home", ""))),
        data_home=Path(str(payload.get("data_home", ""))),
        pid=_pid(payload.get("pid")),
        config_hash=_config_hash(payload.get("config_hash")),
        version=version if isinstance(version, str) else None,
    )
    if descriptor.host != SERVICE_HOST:
        raise ServiceIsolationError(
            f"recorded service host {descriptor.host!r} is not the private loopback endpoint"
        )
    _contained(Path(descriptor.config_home).resolve(), config_home, "config_home")
    _contained(Path(descriptor.data_home).resolve(), data_home, "data_home")
    return descriptor


def config_file_hash(config_path: str | Path) -> str:
    """Hash the generated config exactly as it sits on disk."""

    path = Path(config_path)
    if path.is_symlink():
        raise PathEscapeError(f"generated opencode config must not be a symlink: {path}")
    try:
        return content_hash(path.read_bytes())
    except OSError as error:
        raise ServiceIsolationError(f"generated opencode config is unreadable: {path}") from error


def process_alive(pid: int) -> bool:
    """True only when this user owns a live process with that pid."""

    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    except OSError as error:
        raise ServiceIsolationError(f"cannot check opencode service pid {pid}: {error}") from error
    return True


def attach_service(
    home: str | Path,
    config_path: str | Path,
    *,
    is_alive: Callable[[int], bool] = process_alive,
) -> ServiceDescriptor:
    """Re-prove the recorded service before every use, or refuse to attach.

    Reading the descriptor already re-proves the endpoint and the generated
    homes; this adds the two proofs that decay over time: the process is still
    running, and the config it was started with is still the config on disk.
    """

    descriptor = read_service_descriptor(home)
    if descriptor is None:
        raise ServiceIsolationError(
            "managed opencode service isolation is unproven; refusing to attach to a "
            "global service or fall back to the CLI"
        )
    if not is_alive(descriptor.pid):
        raise ServiceIsolationError(
            f"proven opencode service pid {descriptor.pid} is gone; the runtime stays "
            "unavailable rather than attaching to whatever now owns the endpoint"
        )
    observed = config_file_hash(config_path)
    if observed != descriptor.config_hash:
        raise ServiceIsolationError(
            "the generated opencode config changed after the service was proven; "
            f"expected {descriptor.config_hash}, found {observed}"
        )
    require_server_password()
    return descriptor


# --- starting the one managed service ------------------------------------


def generated_config_path(home: str | Path) -> Path:
    """The one generated config a managed service may be started with."""

    return _service_root(home) / CONFIG_RELATIVE_PATH


@dataclass(frozen=True)
class ServiceStart:
    """The proven service this call owns, and whether it was already live."""

    descriptor: ServiceDescriptor
    reused: bool


def start_service(
    config: RuntimeConfig,
    home: str | Path,
    *,
    port: int | None = None,
    inherited_environment: Mapping[str, str] | None = None,
    client_factory: Callable[[str], object] | None = None,
    ops: ProcessOps | None = None,
    sleep: Callable[[float], None] = time.sleep,
    monotonic: Callable[[], float] = time.monotonic,
) -> ServiceStart:
    """Start one private service, or reuse the live one already proven here.

    Nothing is ever adopted: a foreign listener on the candidate port is
    refused, and a candidate that cannot prove its pid, its credentials and its
    generated config is terminated by its own process group before returning.
    """

    root = _service_root(home)
    config_file = generated_config_path(root)
    if not config_file.is_file():
        raise ServiceIsolationError(
            f"generated opencode config is missing: {config_file}; materialize the "
            "opencode home before starting the managed service"
        )
    try:
        return ServiceStart(attach_service(root, config_file), True)
    except ServiceIsolationError:
        pass  # nothing proven is live here, so start exactly one candidate
    inherited = dict(os.environ if inherited_environment is None else inherited_environment)
    number = _free_port() if port is None else _port(port)
    _require_free_port(number)
    # No argv override: a managed service is always the default 'serve'.
    plan = build_service_plan(config, root, port=number, inherited_environment=inherited)
    config_hash = config_file_hash(config_file)
    process = _spawn(plan)
    try:
        from .readiness import await_health, await_model_roster

        client = _client(plan, root) if client_factory is None else client_factory(plan.base_url)
        health_started = monotonic()
        descriptor = verify_isolation(
            plan,
            await_health(plan, process, client, sleep=sleep, monotonic=monotonic),
            pid=process.pid,
            config_hash=config_hash,
        )
        verify_config_isolation(_fetch_json(client, CONFIG_API_PATH), config_file)
        verify_pinned_version(_fetch_json(client, GLOBAL_HEALTH_PATH))
        # Live-proven v1 race: providers register async right after health
        # goes healthy, so close it here, once, at readiness.
        await_model_roster(
            client, config.models, plan=plan, started_at=health_started,
            sleep=sleep, monotonic=monotonic,
        )
        # Recorded last, and inside the guard: a service nothing can attach to
        # is a leak, so a failed write takes the candidate down with it.
        write_service_descriptor(root, descriptor)
    except BaseException:
        # Cleanup is never allowed to mask the failure that caused it.
        try:
            _terminate_candidate(process, ops)
        except Exception:
            pass
        raise
    return ServiceStart(descriptor, False)


def _client(plan: ServicePlan, root: Path) -> object:
    from .http import OpenCodeHttpClient

    return OpenCodeHttpClient(plan.base_url, root)


def _free_port() -> int:
    """Ask the kernel for a free loopback port and release it immediately.

    A foreign process can still take the port before the child binds it; the
    child then exits at once and the readiness loop refuses that candidate.
    """

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind((SERVICE_HOST, 0))
        return _port(int(probe.getsockname()[1]))


def _require_free_port(port: int) -> None:
    """Refuse a candidate port unless nothing at all answers on it."""

    try:
        with socket.create_connection((SERVICE_HOST, port), PROBE_TIMEOUT_SECONDS):
            pass
    except ConnectionRefusedError:
        return
    except OSError as error:
        raise ServiceIsolationError(
            f"cannot prove {SERVICE_HOST}:{port} is free: {error}"
        ) from error
    raise ServiceIsolationError(
        f"something already listens on {SERVICE_HOST}:{port}; the managed opencode "
        "service never adopts a process it did not start"
    )


def _open_service_log(path: Path):
    """Append to one private log; a symlink there is refused, not followed."""

    descriptor = os.open(
        str(path), os.O_CREAT | os.O_APPEND | os.O_WRONLY | os.O_NOFOLLOW, 0o600
    )
    try:
        os.fchmod(descriptor, 0o600)
    except BaseException:
        os.close(descriptor)
        raise
    return os.fdopen(descriptor, "ab")


def _spawn(plan: ServicePlan) -> subprocess.Popen:
    """Start exactly one child, in its own session, with its own private log."""

    if not plan.argv:
        raise ValidationError("a service plan without argv starts nothing")
    with _open_service_log(plan.cwd / SERVICE_LOG_NAME) as log:
        return subprocess.Popen(
            list(plan.argv),
            cwd=str(plan.cwd),
            env=dict(plan.environment),
            stdin=subprocess.DEVNULL,
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )


def _fetch_json(client: object, path: str) -> object:
    capture = client.get(path)
    try:
        return capture.json()
    finally:
        capture.release()


#: default_agent + model: no stray global config coincidentally matches both.
_SENTINEL_KEYS: tuple[str, ...] = ("default_agent", "model")


def verify_config_isolation(payload: object, config_path: str | Path) -> None:
    """Prove /config came from the generated file via a sentinel round-trip;
    v1 has no per-source document list to walk instead."""

    if not isinstance(payload, Mapping):
        raise ServiceIsolationError(
            "opencode /config did not report a JSON object; refusing a service whose configuration cannot be proven"
        )
    path = Path(config_path)
    try:
        generated = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ServiceIsolationError(f"generated opencode config is unreadable: {path}") from error
    except ValueError as error:
        raise ServiceIsolationError(f"generated opencode config is not valid JSON: {path}") from error
    if not isinstance(generated, Mapping):
        raise ServiceIsolationError(f"generated opencode config must be a JSON object: {path}")
    for key in _SENTINEL_KEYS:
        expected = generated.get(key)
        if not isinstance(expected, str) or not expected:
            raise ServiceIsolationError(f"generated opencode config has no {key!r} sentinel")
        observed = payload.get(key)
        if observed != expected:
            raise ServiceIsolationError(
                f"opencode /config reports {key} {observed!r}, expected the generated "
                f"{expected!r}; refusing a service configured from somewhere else"
            )


def verify_pinned_version(payload: object, *, pinned: str = PINNED_VERSION) -> None:
    """Refuse a candidate whose reported version is not the pin: the real
    anti-drift guard against an unattended upgrade or a misdirected binary."""

    if not isinstance(payload, Mapping):
        raise ServiceIsolationError(
            "opencode /global/health did not report a JSON object; refusing a service whose version cannot be proven"
        )
    version = payload.get("version")
    if version != pinned:
        raise ServiceIsolationError(
            f"opencode service reports version {version!r}; refusing anything but the pinned {pinned!r}"
        )


def _terminate_candidate(
    process: subprocess.Popen, ops: ProcessOps | None = None
) -> Termination:
    """TERM, wait, then KILL exactly the group this call spawned, and nothing else."""

    operations = SystemProcessOps() if ops is None else ops
    if process.poll() is not None:
        return Termination((), True, 0.0)
    return terminate_process_group(
        operations,
        verify_process_group(operations, process.pid),
        owned_pid=process.pid,
        grace_seconds=TERMINATE_GRACE_SECONDS,
        kill_grace_seconds=KILL_GRACE_SECONDS,
    )
