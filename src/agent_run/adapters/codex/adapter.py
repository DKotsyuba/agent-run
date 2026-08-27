"""Codex runtime adapter: isolated home, app-server launch, models/limits.

No live ``codex`` calls happen anywhere in this module. Model rosters and
capacity limits are read from an isolated on-disk cache/evidence file below
the generated ``CODEX_HOME`` (populated by a live probe out of this task's
scope); this adapter only intersects that cache with the configured
allowlist and marks missing/stale evidence ``unknown``.
"""

from __future__ import annotations

import json
import math
import os
import shlex
import time
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from ...config import McpConfig, RuntimeConfig
from ...domain import StartRequest
from ...errors import PathEscapeError, ValidationError
from ...profiles import AgentProfile, normalize_read_roots
from ..base import (
    ADAPTER_API_VERSION,
    Capability,
    EventSink,
    LaunchPlan,
    LimitSample,
    ModelInfo,
    RuntimeAdapter,
    RuntimeHealth,
    RuntimeInfo,
    RuntimeSession,
)
from ..home import content_hash, create_symlink_bridge, write_managed_file
from ..plugin_skills import skill_dirs
from . import app_server, plugins as plugin_install


_CONFIG_REL = "config.toml"
_MODEL_CACHE_REL = "cache/models.json"
_ROLLOUT_EVIDENCE_REL = "cache/rollout_evidence.json"
_LIMITS_STALE_SECONDS = 900
_ROLLOUT_FILES = 24
_ROLLOUT_TAIL_BYTES = 262_144
_ROLLOUT_TAIL_LINES = 2_048
_APPROVAL_POLICY = "never"


def _toml_string(value: str) -> str:
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _toml_array(values) -> str:
    return "[" + ", ".join(_toml_string(value) for value in values) + "]"


def _read_json(path: Path) -> object | None:
    """Read one isolated cache file; unreadable, non-UTF-8 or invalid JSON is no evidence."""

    try:
        with path.open("r", encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, ValueError):
        # ValueError covers both json.JSONDecodeError and UnicodeDecodeError.
        return None


def _timestamp(value: object) -> datetime | None:
    """Convert a cached epoch second to UTC; anything unrepresentable is unknown."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value):
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _rollout_timestamp(value: object) -> datetime | None:
    """Normalize an epoch or offset-aware ISO timestamp through `_timestamp`."""

    if isinstance(value, str) and len(value) <= 64:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return None
            value = parsed.timestamp()
        except (OverflowError, OSError, ValueError):
            return None
    return _timestamp(value)


def _tail_lines(path: Path) -> tuple[str, ...]:
    """Read a bounded complete-line tail from one isolated rollout."""

    with path.open("rb") as stream:
        stream.seek(0, 2)
        end = stream.tell()
        stream.seek(max(0, end - _ROLLOUT_TAIL_BYTES))
        data = stream.read(_ROLLOUT_TAIL_BYTES)
    if end > _ROLLOUT_TAIL_BYTES:
        data = data.split(b"\n", 1)[-1]
    return tuple(data.decode("utf-8").splitlines()[-_ROLLOUT_TAIL_LINES:])


def _rollout_limits(
    home: Path, models: tuple[str, ...], now: float
) -> tuple[LimitSample, ...]:
    """Return the newest valid rate-limit event from this isolated Codex home."""

    sessions = Path(home) / "sessions"
    try:
        home_root = Path(home).resolve(strict=True)
        if sessions.is_symlink():
            return ()
        sessions_root = sessions.resolve(strict=True)
        sessions_root.relative_to(home_root)
        paths = sessions.glob("*/*/*/rollout-*.jsonl")
    except (OSError, RuntimeError, ValueError):
        return ()

    newest: list[tuple[int, str, Path]] = []
    try:
        for path in paths:
            try:
                if path.is_symlink():
                    continue
                resolved = path.resolve(strict=True)
                resolved.relative_to(sessions_root)
                if not resolved.is_file():
                    continue
                candidate = (resolved.stat().st_mtime_ns, str(resolved), resolved)
                newest = sorted((*newest, candidate), reverse=True)[:_ROLLOUT_FILES]
            except (OSError, RuntimeError, ValueError):
                continue
    except OSError:
        pass

    for _mtime, _name, path in newest:
        try:
            lines = _tail_lines(path)
        except (OSError, UnicodeError, ValueError):
            continue
        for line in reversed(lines):
            if '"rate_limits"' not in line or '"token_count"' not in line:
                continue
            try:
                event = json.loads(line)
            except (TypeError, ValueError):
                continue
            if not isinstance(event, dict) or event.get("type") != "event_msg":
                continue
            payload = event.get("payload")
            if not isinstance(payload, dict) or payload.get("type") != "token_count":
                continue
            limits = payload.get("rate_limits")
            observed_at = _rollout_timestamp(event.get("timestamp"))
            if not isinstance(limits, dict) or observed_at is None:
                continue

            stale = now - observed_at.timestamp() > _LIMITS_STALE_SECONDS
            samples = []
            for lane in ("primary", "secondary", "individual_limit"):
                window = limits.get(lane)
                if not isinstance(window, dict):
                    continue
                used = window.get("used_percent")
                if (
                    isinstance(used, bool)
                    or not isinstance(used, (int, float))
                    or not math.isfinite(used)
                ):
                    continue
                minutes = window.get("window_minutes")
                label = (
                    "model_weekly"
                    if lane == "individual_limit"
                    else "session_5h"
                    if minutes == 300
                    else "weekly"
                    if minutes == 10080
                    else lane
                )
                target = next(
                    (
                        value
                        for key in ("target", "model", "limit_name")
                        if isinstance((value := window.get(key)), str) and value in models
                    ),
                    None,
                )
                samples.append(
                    LimitSample(
                        lane=lane,
                        window=label,
                        remaining_percent=None
                        if stale
                        else max(0.0, min(100.0, 100.0 - float(used))),
                        reset_at=_rollout_timestamp(window.get("resets_at")),
                        observed_at=observed_at,
                        source="unknown" if stale else "isolated_rollout_evidence",
                        target=target,
                    )
                )
            if samples:
                return tuple(samples)
    return ()


def _resolved_directory(value: object, label: str) -> Path:
    try:
        resolved = Path(value).expanduser().resolve(strict=True)
    except (TypeError, OSError, RuntimeError) as error:
        raise ValidationError(f"{label} must be an existing directory: {value}") from error
    if not resolved.is_dir():
        raise ValidationError(f"{label} must be an existing directory: {value}")
    return resolved


def _prune_skills(home: Path, selected: frozenset[str]) -> None:
    """Drop adapter-owned skill directories that are no longer selected.

    Only direct children below ``skills/`` that carry a managed ``SKILL.md``
    are touched, so runtime-owned state below the generated home survives.
    """

    skills_root = home / "skills"
    if skills_root.is_symlink():
        raise PathEscapeError(f"codex skills root must not be a symlink: {skills_root}")
    if not skills_root.is_dir():
        return
    for child in sorted(skills_root.iterdir()):
        if child.name in selected or child.is_symlink() or not child.is_dir():
            continue
        managed = child / "SKILL.md"
        if managed.is_symlink() or not managed.is_file():
            continue
        managed.unlink()
        try:
            child.rmdir()
        except OSError:
            # The runtime kept unrelated files below this skill; leave them.
            pass


def _bridge_points_at_source(bridge: Path, source: Path | None) -> bool:
    """The bridge is authenticated only if it canonically resolves to the configured source."""

    if source is None or not bridge.is_symlink():
        return False
    try:
        return bridge.resolve(strict=True) == Path(source).expanduser().resolve(strict=True)
    except (OSError, RuntimeError):
        return False


def _require_resolved_mcp(
    config: RuntimeConfig, mcp_servers: Mapping[str, McpConfig], where: str
) -> None:
    """Every selected MCP must come from the caller-resolved mapping, not ambient config."""

    if not isinstance(mcp_servers, Mapping):
        raise ValidationError(f"codex {where} requires a resolved mcp_servers mapping")
    for name in config.mcp:
        try:
            definition = mcp_servers[name]
        except (KeyError, TypeError) as error:
            raise ValidationError(f"codex mcp reference is not configured: {name}") from error
        if not isinstance(definition, McpConfig):
            raise ValidationError(f"codex mcp reference is not resolved: {name}")


class CodexAdapter:
    def describe(self) -> RuntimeInfo:
        return RuntimeInfo(
            "codex",
            ADAPTER_API_VERSION,
            frozenset(
                {
                    Capability.STEER,
                    Capability.EFFORT,
                    Capability.READ_ROOTS,
                    Capability.WRITE,
                    Capability.TRANSCRIPT,
                    Capability.MODEL_ROSTER,
                    Capability.LIVE_LIMITS,
                    Capability.MCP,
                    Capability.SKILLS,
                    Capability.HOOKS,
                }
            ),
        )

    def validate(self, config: RuntimeConfig) -> None:
        if not isinstance(config, RuntimeConfig):
            raise ValidationError("codex adapter requires a RuntimeConfig")
        if config.service_mode is not None:
            raise ValidationError("codex runtime does not use service_mode")
        if config.auth is None or config.auth.kind != "file_link":
            raise ValidationError("codex runtime requires a file_link auth bridge")
        if not config.models:
            raise ValidationError("codex runtime requires at least one configured model")

    def materialize(
        self,
        config: RuntimeConfig,
        home: Path,
        *,
        mcp_servers: Mapping[str, McpConfig],
        skills_root: Path | None = None,
    ) -> str:
        self.validate(config)
        _require_resolved_mcp(config, mcp_servers, "materialize")
        if skills_root is None:
            skills_root = Path(home).parents[2] / "skills" / "codex"
        if not isinstance(skills_root, Path) or not skills_root.is_absolute():
            raise ValidationError("codex skills_root must be absolute")
        # A declared plugin that ships a selected skill owns that name, so the
        # child reads the plugin's current copy instead of the one below the
        # runtime skills root.
        sources = skill_dirs(config.plugins, skills_root, config.skills)
        skill_hashes: dict[str, str] = {}
        for name in config.skills:
            source = sources[name] / "SKILL.md"
            try:
                text = source.read_text(encoding="utf-8")
            except OSError as error:
                raise ValidationError(f"codex skill is not available: {name}: {error}") from error
            skill_hashes[name] = write_managed_file(home, f"skills/{name}/SKILL.md", text)
        _prune_skills(Path(home), frozenset(config.skills))

        mcp_lines: list[str] = []
        for name in sorted(config.mcp):
            mcp_def = mcp_servers[name]
            mcp_lines.append(f"[mcp_servers.{name}]")
            mcp_lines.append(f"command = {_toml_string(str(mcp_def.command))}")
            mcp_lines.append(f"args = {_toml_array(mcp_def.args)}")
            if mcp_def.env_from:
                mcp_lines.append(f"env_from = {_toml_array(mcp_def.env_from)}")
            mcp_lines.append("")

        plugin_lines, plugin_digest, plugin_roots = plugin_install.install(
            Path(home), config.plugins
        )

        # Codex reads config-level hooks as ``[[hooks.<Event>]]`` groups, each
        # holding its own ``[[hooks.<Event>.hooks]]`` command handlers, and it
        # runs none of them without a matching ``[hooks.state]`` trust digest.
        # ``timeout`` is written explicitly because that same value is hashed.
        hook_lines: list[str] = []
        trust_lines: list[str] = []
        hook_digests: list[str] = []
        groups: dict[str, int] = {}
        for hook in config.hooks:
            group = groups.get(hook.event, 0)
            groups[hook.event] = group + 1
            command = shlex.join(plugin_install.expand(hook.command, plugin_roots))
            key, trusted_hash = plugin_install.hook_trust(
                Path(home) / _CONFIG_REL, hook.event, group, hook.matcher, command
            )
            hook_lines.append(f"[[hooks.{hook.event}]]")
            if hook.matcher is not None:
                hook_lines.append(f"matcher = {_toml_string(hook.matcher)}")
            hook_lines.append("")
            hook_lines.append(f"[[hooks.{hook.event}.hooks]]")
            hook_lines.append('type = "command"')
            hook_lines.append(f"command = {_toml_string(command)}")
            hook_lines.append(f"timeout = {plugin_install.DEFAULT_TIMEOUT_SEC}")
            hook_lines.append("")
            trust_lines.append(f'[hooks.state."{key}"]')
            trust_lines.append(f'trusted_hash = "{trusted_hash}"')
            trust_lines.append("")
            hook_digests.append(f"{key}={trusted_hash}")

        body_lines = [
            "# generated by agent-run; do not edit by hand",
            "",
            *mcp_lines,
            *hook_lines,
            *trust_lines,
            *plugin_lines,
        ]
        generated_config = "\n".join(body_lines).rstrip() + "\n"
        write_managed_file(home, _CONFIG_REL, generated_config)

        auth_digest = ""
        if config.auth is not None and config.auth.kind == "file_link":
            bridge = create_symlink_bridge(home, config.auth.target, config.auth.source)
            auth_digest = str(bridge.resolve(strict=True))

        fingerprint = "\n".join(
            [
                generated_config,
                *(f"{name}:{digest}" for name, digest in sorted(skill_hashes.items())),
                *hook_digests,
                plugin_digest,
                auth_digest,
            ]
        )
        return content_hash(fingerprint)

    def probe(self, config: RuntimeConfig, home: Path) -> RuntimeHealth:
        try:
            self.validate(config)
        except ValidationError as error:
            return RuntimeHealth(False, None, None, str(error))
        home_path = Path(home)
        binary_ok = config.binary.exists() and os.access(config.binary, os.X_OK)
        home_ok = home_path.is_dir() and (home_path / _CONFIG_REL).is_file()
        auth_ok = None
        if config.auth is not None and config.auth.kind == "file_link":
            auth_ok = _bridge_points_at_source(home_path / config.auth.target, config.auth.source)
        cache = _read_json(home_path / _MODEL_CACHE_REL)
        version = cache.get("codex_version") if isinstance(cache, dict) else None
        available = bool(binary_ok and home_ok and (auth_ok is not False))
        reason = None if available else "codex binary, generated home, or auth bridge is missing"
        return RuntimeHealth(available, version if isinstance(version, str) else None, auth_ok, reason)

    def models(self, config: RuntimeConfig, home: Path) -> tuple[ModelInfo, ...]:
        cache = _read_json(Path(home) / _MODEL_CACHE_REL)
        entries = cache.get("models") if isinstance(cache, dict) else None
        if not isinstance(entries, list):
            return tuple(ModelInfo(model_id, "", ()) for model_id in config.models)
        by_id: dict[str, dict] = {}
        for item in entries:
            if not isinstance(item, dict):
                continue
            model_id = item.get("slug")
            if not isinstance(model_id, str):
                model_id = item.get("id")
            if isinstance(model_id, str):
                by_id[model_id] = item
        result = []
        for model_id in config.models:
            item = by_id.get(model_id)
            if item is None:
                continue
            description = item.get("description")
            efforts_raw = item.get("supported_reasoning_levels")
            if not isinstance(efforts_raw, list):
                efforts_raw = item.get("efforts")
            efforts: list[str] = []
            if isinstance(efforts_raw, list):
                for level in efforts_raw:
                    effort = level if isinstance(level, str) else (
                        level.get("effort") if isinstance(level, Mapping) else None
                    )
                    if isinstance(effort, str) and effort not in efforts:
                        efforts.append(effort)
            result.append(
                ModelInfo(
                    model_id,
                    description if isinstance(description, str) else "",
                    tuple(efforts),
                )
            )
        return tuple(result)

    def limits(self, config: RuntimeConfig, home: Path) -> tuple[LimitSample, ...]:
        payload = _read_json(Path(home) / _ROLLOUT_EVIDENCE_REL)
        samples_raw = payload.get("samples") if isinstance(payload, dict) else None
        now = time.time()
        result = []
        if isinstance(samples_raw, list):
            for item in samples_raw:
                if not isinstance(item, dict):
                    continue
                lane, window = item.get("lane"), item.get("window")
                if not isinstance(lane, str) or not isinstance(window, str):
                    continue
                observed_raw = item.get("observed_at")
                observed_at = _timestamp(observed_raw)
                stale = observed_at is None or (now - float(observed_raw)) > _LIMITS_STALE_SECONDS
                remaining = item.get("remaining_percent")
                if (
                    stale
                    or isinstance(remaining, bool)
                    or not isinstance(remaining, (int, float))
                    or not math.isfinite(remaining)
                ):
                    remaining = None
                reset_at = _timestamp(item.get("reset_at"))
                valid_for = item.get("valid_for_seconds")
                result.append(
                    LimitSample(
                        lane=lane,
                        window=window,
                        remaining_percent=remaining,
                        reset_at=reset_at,
                        observed_at=observed_at,
                        source="unknown" if stale else "rollout_evidence",
                        target=item.get("target")
                        if isinstance(item.get("target"), str)
                        else None,
                        valid_for_seconds=valid_for
                        if isinstance(valid_for, int) and not isinstance(valid_for, bool)
                        else None,
                    )
                )
        return tuple(result) or _rollout_limits(Path(home), config.models, now)

    def prepare(
        self,
        request: StartRequest,
        profile: AgentProfile,
        config: RuntimeConfig,
        home: Path,
        agent_dir: Path,
        *,
        mcp_servers: Mapping[str, McpConfig],
    ) -> LaunchPlan:
        if not isinstance(request, StartRequest):
            raise ValidationError("prepare requires a StartRequest")
        if not isinstance(profile, AgentProfile):
            raise ValidationError("prepare requires an AgentProfile")
        self.validate(config)
        _require_resolved_mcp(config, mcp_servers, "prepare")
        if request.runtime != "codex":
            raise ValidationError(f"codex adapter cannot prepare runtime {request.runtime!r}")
        if request.model not in config.models:
            raise ValidationError(f"model not allowed for codex: {request.model}")
        if request.output_schema is not None:
            raise ValidationError("codex runtime does not support output_schema")

        discovered = {info.id: info for info in self.models(config, home)}
        model = discovered.get(request.model)
        if model is None:
            raise ValidationError(
                f"model is not discovered in the codex roster cache: {request.model}"
            )
        if request.effort is not None and request.effort not in model.efforts:
            raise ValidationError(
                f"effort {request.effort!r} is not offered for model {request.model!r}"
            )
        if request.write and not profile.write:
            raise ValidationError("profile does not grant write access for this request")
        effective_write = bool(request.write and profile.write)
        if not profile.write and not profile.read_roots and not request.read_roots:
            raise ValidationError(
                "codex refuses a no-filesystem profile: grant write or at least one read root"
            )
        home_path = Path(home)
        if not (home_path / _CONFIG_REL).is_file():
            raise ValidationError(f"codex home is not materialized: {home_path}")

        workdir = _resolved_directory(request.workdir, "workdir")
        roots = tuple(
            str(root)
            for root in normalize_read_roots(
                (workdir, *profile.read_roots, *request.read_roots)
            )
        )
        # The writable grant never widens beyond the workdir, even when a read
        # root above it swallowed the workdir in the normalized antichain.
        writable_roots = (str(workdir),) if effective_write else ()
        sandbox_mode = "workspace-write" if effective_write else "read-only"

        # ``HOME`` is part of the isolation, not a convenience: the engine
        # resolves its personal skill/plugin roots (``~/.agents/skills``,
        # ``~/.agents/plugins``) from the home directory rather than from
        # ``CODEX_HOME``. This mapping fully replaces the parent environment,
        # so leaving ``HOME`` out does not unset it -- the engine falls back to
        # the passwd entry and reads the operator's own global skills straight
        # past this generated home (defect T20B).
        environment = {
            "CODEX_HOME": str(home_path),
            "HOME": str(home_path),
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        }
        if config.plugins and not effective_write:
            # A read-only sandbox cannot write the raw spool the plugin's
            # pre-execution wrapper needs, so that wrapper fails open to the
            # original command. This lets the PostToolUse hook -- which runs
            # outside the sandbox -- spool and replace instead, and only when
            # recovery succeeds. Write-capable agents spool natively and must
            # not get this fallback.
            environment["TOKENPIPE_POST_REPLACE"] = "1"
        adapter_state = {
            "model": request.model,
            "effort": request.effort,
            "sandbox_mode": sandbox_mode,
            "approval_policy": _APPROVAL_POLICY,
            "roots": roots,
            "writable_roots": writable_roots,
            "mcp": tuple(config.mcp),
            "skills": tuple(config.skills),
            "profile": profile.name,
            "request_timeout_seconds": request.timeout_seconds,
        }
        return LaunchPlan(
            argv=(str(config.binary), "app-server"),
            cwd=workdir,
            environment=MappingProxyType(environment),
            initial_input=request.task,
            runtime_stream_path=Path(agent_dir) / "runtime.jsonl",
            adapter_state=MappingProxyType(adapter_state),
            answer_path=Path(agent_dir) / "answer.md",
        )

    def launch(self, plan: LaunchPlan, sink: EventSink) -> RuntimeSession:
        transport = app_server.ProcessTransport(plan)
        try:
            return app_server.start_session(transport, plan, sink)
        except Exception:
            try:
                transport.terminate(1.0)
            except Exception:
                transport.close()
            raise


ADAPTER: RuntimeAdapter = CodexAdapter()
