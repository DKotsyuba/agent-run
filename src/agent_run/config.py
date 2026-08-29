"""Strict owner-authored configuration."""

from __future__ import annotations

import math
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Mapping

from .errors import ValidationError


_ENV_NAME = re.compile(r"[A-Z_][A-Z0-9_]*\Z")
_ASSET_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*\Z")
_IMPORT_REF = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*:[A-Za-z_][A-Za-z0-9_]*\Z"
)


@dataclass(frozen=True)
class CoreConfig:
    default_timeout_seconds: float = 480
    max_active_agents: int = 6
    warning_fraction: float = 0.90


@dataclass(frozen=True)
class CapacityConfig:
    collect_interval_seconds: int = 300
    sample_retention: int = 1000
    context_max_chars: int = 2500
    codexbar_binary: Path = Path("/opt/homebrew/bin/codexbar")


@dataclass(frozen=True)
class DeliveryConfig:
    retry_base_seconds: float = 2
    retry_cap_seconds: float = 60
    max_attempts: int = 0
    codex_queue_bin: Path | None = None


@dataclass(frozen=True)
class ProfilesConfig:
    directory: Path = Path("~/.agent-run/profiles")


@dataclass(frozen=True)
class McpConfig:
    transport: str
    command: Path
    args: tuple[str, ...] = ()
    env_from: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuntimeAuthConfig:
    kind: str
    source: Path | None = None
    target: str | None = None
    names: tuple[str, ...] = ()


@dataclass(frozen=True)
class RuntimeHookConfig:
    event: str
    command: tuple[str, ...]
    matcher: str | None = None


@dataclass(frozen=True)
class RuntimeConfig:
    enabled: bool
    adapter: str
    binary: Path
    home: Path
    models: tuple[str, ...]
    skills: tuple[str, ...] = ()
    mcp: tuple[str, ...] = ()
    max_active_agents: int | None = None
    auth: RuntimeAuthConfig | None = None
    hooks: tuple[RuntimeHookConfig, ...] = ()
    service_mode: str | None = None
    plugins: tuple[Path, ...] = ()
    limits_source: str | None = None


@dataclass(frozen=True)
class Config:
    schema_version: int
    core: CoreConfig = field(default_factory=CoreConfig)
    capacity: CapacityConfig = field(default_factory=CapacityConfig)
    delivery: DeliveryConfig = field(default_factory=DeliveryConfig)
    profiles: ProfilesConfig = field(default_factory=ProfilesConfig)
    mcp: Mapping[str, McpConfig] = field(
        default_factory=lambda: MappingProxyType({})
    )
    runtimes: Mapping[str, RuntimeConfig] = field(
        default_factory=lambda: MappingProxyType({})
    )


AgentRunConfig = Config


def _table(value: object, path: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValidationError(f"{path} must be a table")
    return value


def _reject_unknown(table: Mapping[str, object], allowed: set[str], path: str) -> None:
    for key in table:
        if key not in allowed:
            field = f"{path}.{key}" if path else key
            raise ValidationError(f"unknown config field: {field}")


def _string(value: object, path: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"{path} must be a nonblank string")
    return value


def _strings(value: object, path: str) -> tuple[str, ...]:
    if not isinstance(value, list):
        raise ValidationError(f"{path} must be an array of strings")
    return tuple(_string(item, f"{path}[{index}]") for index, item in enumerate(value))


def _names(value: object, path: str) -> tuple[str, ...]:
    names = _strings(value, path)
    for index, name in enumerate(names):
        if not _ASSET_NAME.fullmatch(name):
            raise ValidationError(f"{path}[{index}] must be a name, not a path")
    return names


def _bool(value: object, path: str) -> bool:
    if not isinstance(value, bool):
        raise ValidationError(f"{path} must be a boolean")
    return value


def _int(value: object, path: str, *, minimum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValidationError(f"{path} must be an integer >= {minimum}")
    return value


def _number(value: object, path: str, *, minimum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < minimum:
        raise ValidationError(f"{path} must be a number >= {minimum}")
    return float(value)


def _path(value: object, path: str) -> Path:
    text = _string(value, path)
    try:
        expanded = Path(text).expanduser()
    except RuntimeError as error:
        raise ValidationError(f"{path} must be an absolute path") from error
    if not expanded.is_absolute():
        raise ValidationError(f"{path} must be an absolute path")
    return expanded.resolve()


def _relative_target(value: object, path: str) -> str:
    text = _string(value, path)
    target = PurePosixPath(text)
    if target.is_absolute() or ".." in target.parts or text in {".", ".."}:
        raise ValidationError(f"{path} must be a relative path without '..'")
    return text


def _plugin_dirs(value: object, path: str) -> tuple[Path, ...]:
    """Absolute, existing plugin directories; anything else fails closed."""

    if not isinstance(value, list):
        raise ValidationError(f"{path} must be an array of absolute plugin directories")
    plugins = []
    for index, item in enumerate(value):
        entry = f"{path}[{index}]"
        directory = _path(item, entry)
        if not directory.is_dir():
            raise ValidationError(f"{entry} must be an existing directory: {directory}")
        if directory in plugins:
            raise ValidationError(f"{entry} is declared twice: {directory}")
        plugins.append(directory)
    return tuple(plugins)


def _env_names(value: object, path: str) -> tuple[str, ...]:
    names = _strings(value, path)
    for index, name in enumerate(names):
        if not _ENV_NAME.fullmatch(name):
            raise ValidationError(
                f"{path}[{index}] must name an environment variable, not a secret value"
            )
    return names


def _named_table(value: object, path: str) -> dict[str, dict[str, object]]:
    table = _table(value, path)
    result = {}
    for name, item in table.items():
        if not _ASSET_NAME.fullmatch(name):
            raise ValidationError(f"{path} name must be a name, not a path: {name!r}")
        result[name] = _table(item, f"{path}.{name}")
    return result


def _parse_core(value: object) -> CoreConfig:
    table = _table(value, "core")
    _reject_unknown(
        table,
        {"default_timeout_seconds", "max_active_agents", "warning_fraction"},
        "core",
    )
    warning = _number(table.get("warning_fraction", 0.90), "core.warning_fraction", minimum=0)
    if not 0 < warning < 1:
        raise ValidationError("core.warning_fraction must be strictly between 0 and 1")
    return CoreConfig(
        _number(
            table.get("default_timeout_seconds", 480),
            "core.default_timeout_seconds",
            minimum=0.000001,
        ),
        _int(table.get("max_active_agents", 6), "core.max_active_agents", minimum=1),
        warning,
    )


def _parse_capacity(value: object) -> CapacityConfig:
    table = _table(value, "capacity")
    _reject_unknown(
        table,
        {
            "collect_interval_seconds",
            "sample_retention",
            "context_max_chars",
            "codexbar_binary",
        },
        "capacity",
    )
    interval = _int(
        table.get("collect_interval_seconds", 300),
        "capacity.collect_interval_seconds",
        minimum=1,
    )
    context_max_chars = _int(
        table.get("context_max_chars", 2500),
        "capacity.context_max_chars",
        minimum=1,
    )
    if context_max_chars > 2500:
        raise ValidationError("capacity.context_max_chars must be <= 2500")
    codexbar_binary = table.get("codexbar_binary")
    return CapacityConfig(
        interval,
        _int(table.get("sample_retention", 1000), "capacity.sample_retention", minimum=1),
        context_max_chars,
        Path("/opt/homebrew/bin/codexbar")
        if codexbar_binary is None
        else _path(codexbar_binary, "capacity.codexbar_binary"),
    )


def _parse_delivery(value: object) -> DeliveryConfig:
    table = _table(value, "delivery")
    _reject_unknown(
        table,
        {"retry_base_seconds", "retry_cap_seconds", "max_attempts", "codex_queue_bin"},
        "delivery",
    )
    base = _number(
        table.get("retry_base_seconds", 2),
        "delivery.retry_base_seconds",
        minimum=0.000001,
    )
    cap = _number(
        table.get("retry_cap_seconds", 60),
        "delivery.retry_cap_seconds",
        minimum=base,
    )
    return DeliveryConfig(
        base,
        cap,
        _int(table.get("max_attempts", 0), "delivery.max_attempts", minimum=0),
        None
        if table.get("codex_queue_bin") is None
        else _path(table["codex_queue_bin"], "delivery.codex_queue_bin"),
    )


def _parse_profiles(value: object) -> ProfilesConfig:
    table = _table(value, "profiles")
    _reject_unknown(table, {"directory"}, "profiles")
    return ProfilesConfig(_path(table.get("directory", "~/.agent-run/profiles"), "profiles.directory"))


def _parse_mcp(value: object) -> Mapping[str, McpConfig]:
    result: dict[str, McpConfig] = {}
    for name, table in _named_table(value, "mcp").items():
        path = f"mcp.{name}"
        _reject_unknown(table, {"transport", "command", "args", "env_from"}, path)
        transport = _string(table.get("transport"), f"{path}.transport")
        if transport != "stdio":
            raise ValidationError(f"{path}.transport must be 'stdio'")
        result[name] = McpConfig(
            transport,
            _path(table.get("command"), f"{path}.command"),
            _strings(table.get("args", []), f"{path}.args"),
            _env_names(table.get("env_from", []), f"{path}.env_from"),
        )
    return MappingProxyType(result)


def _parse_auth(value: object, path: str) -> RuntimeAuthConfig:
    table = _table(value, path)
    _reject_unknown(table, {"kind", "source", "target", "names"}, path)
    kind = _string(table.get("kind"), f"{path}.kind")
    if kind == "file_link":
        if "names" in table:
            raise ValidationError(f"{path}.names is not valid for file_link auth")
        return RuntimeAuthConfig(
            kind,
            _path(table.get("source"), f"{path}.source"),
            _relative_target(table.get("target"), f"{path}.target"),
        )
    if kind == "environment":
        if "source" in table:
            raise ValidationError(f"{path}.source is not valid for environment auth")
        if "target" in table:
            raise ValidationError(f"{path}.target is not valid for environment auth")
        names = _env_names(table.get("names"), f"{path}.names")
        if not names:
            raise ValidationError(f"{path}.names must not be empty")
        return RuntimeAuthConfig(kind, names=names)
    raise ValidationError(f"{path}.kind must be 'file_link' or 'environment'")


def _parse_hooks(value: object, path: str) -> tuple[RuntimeHookConfig, ...]:
    if not isinstance(value, list):
        raise ValidationError(f"{path} must be an array of tables")
    hooks = []
    for index, item in enumerate(value):
        item_path = f"{path}[{index}]"
        table = _table(item, item_path)
        _reject_unknown(table, {"event", "command", "matcher"}, item_path)
        command = _strings(table.get("command"), f"{item_path}.command")
        if not command:
            raise ValidationError(f"{item_path}.command must not be empty")
        matcher = table.get("matcher")
        hooks.append(
            RuntimeHookConfig(
                _string(table.get("event"), f"{item_path}.event"),
                command,
                None if matcher is None else _string(matcher, f"{item_path}.matcher"),
            )
        )
    return tuple(hooks)


def _parse_runtimes(value: object) -> Mapping[str, RuntimeConfig]:
    result: dict[str, RuntimeConfig] = {}
    allowed = {
        "enabled",
        "adapter",
        "binary",
        "home",
        "models",
        "skills",
        "mcp",
        "max_active_agents",
        "auth",
        "hooks",
        "service_mode",
        "plugins",
        "limits_source",
    }
    for name, table in _named_table(value, "runtimes").items():
        path = f"runtimes.{name}"
        _reject_unknown(table, allowed, path)
        adapter = _string(table.get("adapter"), f"{path}.adapter")
        if not _IMPORT_REF.fullmatch(adapter):
            raise ValidationError(f"{path}.adapter must be 'module:attribute'")
        models = _strings(table.get("models"), f"{path}.models")
        if not models:
            raise ValidationError(f"{path}.models must not be empty")
        maximum = table.get("max_active_agents")
        auth = table.get("auth")
        service_mode = table.get("service_mode")
        limits_source = table.get("limits_source")
        result[name] = RuntimeConfig(
            _bool(table.get("enabled"), f"{path}.enabled"),
            adapter,
            _path(table.get("binary"), f"{path}.binary"),
            _path(table.get("home"), f"{path}.home"),
            models,
            _names(table.get("skills", []), f"{path}.skills"),
            _names(table.get("mcp", []), f"{path}.mcp"),
            None if maximum is None else _int(maximum, f"{path}.max_active_agents", minimum=1),
            None if auth is None else _parse_auth(auth, f"{path}.auth"),
            _parse_hooks(table.get("hooks", []), f"{path}.hooks"),
            None if service_mode is None else _string(service_mode, f"{path}.service_mode"),
            _plugin_dirs(table.get("plugins", []), f"{path}.plugins"),
            None if limits_source is None else _string(limits_source, f"{path}.limits_source"),
        )
        if result[name].service_mode not in {None, "managed"}:
            raise ValidationError(f"{path}.service_mode must be 'managed'")
        if result[name].limits_source not in {None, "native", "omniroute", "codexbar", "none"}:
            raise ValidationError(
                f"{path}.limits_source must be one of 'native', 'omniroute', 'codexbar', 'none'"
            )
    return MappingProxyType(result)


def load_config(path: str | Path) -> Config:
    """Load version 1 config without reading any referenced secret source."""

    try:
        with Path(path).open("rb") as stream:
            raw = tomllib.load(stream)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ValidationError(f"cannot load config {path}: {error}") from error
    _reject_unknown(
        raw, {"schema_version", "core", "capacity", "delivery", "profiles", "mcp", "runtimes"}, ""
    )
    version = raw.get("schema_version")
    if type(version) is not int or version != 1:
        raise ValidationError(f"unsupported schema_version: {version!r}")
    mcp = _parse_mcp(raw.get("mcp", {}))
    runtimes = _parse_runtimes(raw.get("runtimes", {}))
    for runtime_name, runtime in runtimes.items():
        for index, mcp_name in enumerate(runtime.mcp):
            if mcp_name not in mcp:
                raise ValidationError(
                    f"runtimes.{runtime_name}.mcp[{index}] references unknown MCP {mcp_name!r}"
                )
    return Config(
        schema_version=1,
        core=_parse_core(raw.get("core", {})),
        capacity=_parse_capacity(raw.get("capacity", {})),
        delivery=_parse_delivery(raw.get("delivery", {})),
        profiles=_parse_profiles(raw.get("profiles", {})),
        mcp=mcp,
        runtimes=runtimes,
    )
