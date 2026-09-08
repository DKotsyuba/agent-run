"""Codex runtime adapter: isolated home, app-server launch, models/limits.

Only the bounded ``--version`` health observation invokes ``codex`` here.
Model rosters and capacity limits are read from an isolated on-disk
cache/evidence file below the generated ``CODEX_HOME``; this adapter only
intersects that cache with the configured allowlist and marks missing/stale
evidence ``unknown``.
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
from ...errors import ValidationError
from ...profiles import normalize_read_roots
from ...role_plan import ResolvedRolePlan
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
from ..command_policy import materialize_refusal_commands, render_codex_denial_rules
from ..home import content_hash, create_symlink_bridge, write_managed_file
from ..snapshots import finalize_runtime_snapshots, snapshot_managed_tree
from ..plugin_skills import skill_dirs
from ..version import observe_binary_version
from . import app_server, model_cache, plugins as plugin_install
from .environment import (
    auth_bridge,
    bridge_points_at_source,
    build_environment,
    approval_fields,
    prepared_environment,
    require_resolved_mcp,
    resolved_directory,
)
from .skills import prune_skills
from .toml import toml_array as _toml_array, toml_string as _toml_string


_CONFIG_REL = "config.toml"
_MODEL_CACHE_REL = "cache/models.json"
_ROLLOUT_EVIDENCE_REL = "cache/rollout_evidence.json"
_LIMITS_STALE_SECONDS = 900
_ROLLOUT_FILES = 24
_ROLLOUT_TAIL_BYTES = 262_144
_ROLLOUT_TAIL_LINES = 2_048


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
                        valid_for_seconds=_LIMITS_STALE_SECONDS,
                    )
                )
            if samples:
                return tuple(samples)
    return ()


class CodexAdapter:
    def describe(self) -> RuntimeInfo:
        return RuntimeInfo(
            "codex",
            ADAPTER_API_VERSION,
            frozenset(
                {
                    Capability.STEER,
                    Capability.RESUME,
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
        """Validate Codex configuration accepted by the isolated adapter.

        ``config`` must be a ``RuntimeConfig`` with at least one model. Auth may
        be absent to use the host Codex account or an explicit file-link for a
        separate account.
        """
        if not isinstance(config, RuntimeConfig):
            raise ValidationError("codex adapter requires a RuntimeConfig")
        if config.auth is not None and config.auth.kind != "file_link":
            raise ValidationError("codex runtime auth must be a file_link bridge")
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
        """Regenerate managed assets with fixed extended-context settings.

        Args:
            config (RuntimeConfig): Runtime asset and auth declarations.
            home (Path): Destination for config, skills and the auth bridge.
            mcp_servers (Mapping[str, McpConfig]): Resolved declared MCP servers.
            skills_root (Path | None): Absolute skill source directory; None
                selects the runtime's default skills directory.

        Returns:
            str: Content fingerprint of generated configuration and assets.

        Replaces managed files, prunes unselected skills, installs plugins and
        links auth while preserving unmanaged runtime state. Every generation
        writes a 1,000,000-token window and 780,000-token total compaction limit.
        Invalid declarations raise ValidationError; I/O and plugin errors
        propagate.
        """
        self.validate(config)
        require_resolved_mcp(config, mcp_servers, "materialize")
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
            try:
                snapshot = snapshot_managed_tree(
                    Path(home), f"skills/{name}", sources[name]
                )
            except ValidationError as error:
                raise ValidationError(f"codex skill is not available: {name}: {error}") from error
            skill_hashes[name] = snapshot.sha256
        prune_skills(Path(home), frozenset(config.skills))

        mcp_lines: list[str] = []
        for name in sorted(config.mcp):
            mcp_def = mcp_servers[name]
            mcp_lines.append(f"[mcp_servers.{name}]")
            mcp_lines.append(f"command = {_toml_string(str(mcp_def.command))}")
            mcp_lines.append(f"args = {_toml_array(mcp_def.args)}")
            mcp_environment = mcp_def.env_from
            if mcp_environment:
                mcp_lines.append(f"env_vars = {_toml_array(mcp_environment)}")
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
            "model_context_window = 1000000",
            "model_auto_compact_token_limit = 780000",
            'model_auto_compact_token_limit_scope = "total"',
            "",
            *mcp_lines,
            *hook_lines,
            *trust_lines,
            *plugin_lines,
        ]
        generated_config = "\n".join(body_lines).rstrip() + "\n"
        write_managed_file(home, _CONFIG_REL, generated_config)
        denied_commands = config.environment.denied_commands if config.environment is not None else ()
        policy_environment = build_environment(config.binary, Path(home))
        if config.environment is not None and config.environment.path:
            policy_environment["PATH"] = os.pathsep.join(
                (
                    *(str(path) for path in config.environment.path),
                    policy_environment["PATH"],
                )
            )
        command_policy = materialize_refusal_commands(
            denied_commands,
            Path(home) / "command-refusals",
            environment=policy_environment,
        )
        policy_text = render_codex_denial_rules(
            denied_commands,
            command_paths=tuple(command_policy.resolved_commands.values()),
        )
        write_managed_file(
            home,
            "rules/agent-run-command-policy.rules",
            policy_text,
        )

        auth_digest = ""
        managed_links: tuple[tuple[str, str], ...] = ()
        bridge = auth_bridge(config)
        if bridge is not None:
            source, target = bridge
            auth_target = str(source.expanduser().resolve(strict=True))
            create_symlink_bridge(home, target, source)
            auth_digest = auth_target
            managed_links = ((target, auth_target),)

        fingerprint = "\n".join(
            [
                generated_config,
                *(f"{name}:{digest}" for name, digest in sorted(skill_hashes.items())),
                *hook_digests,
                plugin_digest,
                auth_digest,
                content_hash(policy_text),
            ]
        )
        revision = content_hash(fingerprint)
        finalize_runtime_snapshots(
            Path(home),
            revision,
            (
                "config.toml",
                "rules/agent-run-command-policy.rules",
                "command-refusals/.agent-run-command-policy.json",
                *(f"command-refusals/{command}" for command in sorted(denied_commands)),
            ),
            managed_links,
        )
        return revision

    def probe(self, config: RuntimeConfig, home: Path) -> RuntimeHealth:
        """Report health with a fresh bounded configured-binary version observation."""

        try:
            self.validate(config)
        except ValidationError as error:
            return RuntimeHealth(False, None, None, str(error))
        home_path = Path(home)
        binary_ok = config.binary.exists() and os.access(config.binary, os.X_OK)
        home_ok = home_path.is_dir() and (home_path / _CONFIG_REL).is_file()
        auth_ok = None
        bridge = auth_bridge(config)
        if bridge is not None:
            source, target = bridge
            auth_ok = bridge_points_at_source(home_path / target, source)
        version, version_reason = observe_binary_version(config.binary, home_path)
        available = bool(binary_ok and home_ok and (auth_ok is not False))
        reason = (
            version_reason
            if available
            else "codex binary, generated home, or auth bridge is missing"
        )
        return RuntimeHealth(available, version, auth_ok, reason)

    def models(self, config: RuntimeConfig, home: Path) -> tuple[ModelInfo, ...]:
        cache_path = Path(home) / _MODEL_CACHE_REL
        cache = _read_json(cache_path)
        if not model_cache.is_fresh(cache_path, time.time()):
            model_cache.refresh_models(config, Path(home))
            cache = _read_json(cache_path)
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
            if not isinstance(model_id, str):
                model_id = item.get("model")
            if isinstance(model_id, str):
                by_id[model_id] = item
        result = []
        for model_id in config.models:
            item = by_id.get(model_id)
            if item is None:
                continue
            description = item.get("description")
            efforts_raw = item.get("supportedReasoningEfforts")
            if not isinstance(efforts_raw, list):
                efforts_raw = item.get("supported_reasoning_levels")
            if not isinstance(efforts_raw, list):
                efforts_raw = item.get("efforts")
            efforts: list[str] = []
            if isinstance(efforts_raw, list):
                for level in efforts_raw:
                    if isinstance(level, str):
                        effort = level
                    elif isinstance(level, Mapping):
                        effort = level.get("reasoningEffort")
                        if not isinstance(effort, str):
                            effort = level.get("effort")
                    else:
                        effort = None
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
        role: ResolvedRolePlan,
        config: RuntimeConfig,
        home: Path,
        agent_dir: Path,
        *,
        resume_session_id: str | None = None,
    ) -> LaunchPlan:
        """Build an isolated Codex launch plan for an authorized request.

        Validates request grants and runtime assets against ``role``. Network
        roles receive app-server's tagged sandbox request form. Workspace-write
        threads cannot grant external read roots in the pinned app-server
        contract. ``gpt-6-astra`` is limited to read-only architecture and
        review roles. Raises ``ValidationError`` when an authorization or
        runtime constraint fails.
        """
        if not isinstance(request, StartRequest):
            raise ValidationError("prepare requires a StartRequest")
        if not isinstance(role, ResolvedRolePlan):
            raise ValidationError("prepare requires a ResolvedRolePlan")
        self.validate(config)
        if config.skills != tuple(skill.id for skill in role.skills) or config.mcp != tuple(
            server.id for server in role.mcp
        ):
            raise ValidationError("codex runtime assets do not match the resolved role")
        if request.write != role.write or request.read_roots != role.read_roots:
            raise ValidationError("codex request grants do not match the resolved role")
        if request.runtime != "codex":
            raise ValidationError(f"codex adapter cannot prepare runtime {request.runtime!r}")
        if request.model not in config.models:
            raise ValidationError(f"model not allowed for codex: {request.model}")
        if request.output_schema is not None:
            raise ValidationError("codex runtime does not support output_schema")
        if request.model == "gpt-6-astra":
            if role.role_name not in ("architect", "review"):
                raise ValidationError("gpt-6-astra is limited to architect and review roles")
            if role.write:
                raise ValidationError("gpt-6-astra does not permit write-capable launches")

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
        effective_write = role.write
        home_path = Path(home)
        if not (home_path / _CONFIG_REL).is_file():
            raise ValidationError(f"codex home is not materialized: {home_path}")

        workdir = resolved_directory(request.workdir, "workdir")
        roots = tuple(
            str(root)
            for root in normalize_read_roots(
                (workdir, *role.read_roots)
            )
        )
        # The writable grant never widens beyond the workdir, even when a read
        # root above it swallowed the workdir in the normalized antichain.
        writable_roots = (str(workdir),) if effective_write else ()
        if effective_write and roots != writable_roots:
            raise ValidationError(
                "codex workspace-write threads cannot grant external read roots; "
                "copy the material into the workdir and omit --read-root"
            )
        sandbox_mode = "workspace-write" if effective_write else "read-only"

        # ``HOME`` is part of the isolation, not a convenience: the engine
        # resolves its personal skill/plugin roots (``~/.agents/skills``,
        # ``~/.agents/plugins``) from the home directory rather than from
        # ``CODEX_HOME``. This mapping fully replaces the parent environment,
        # so leaving ``HOME`` out does not unset it -- the engine falls back to
        # the passwd entry and reads the operator's own global skills straight
        # past this generated home (defect T20B).
        denied_commands = (
            config.environment.denied_commands if config.environment is not None else ()
        )
        environment = prepared_environment(
            config.binary,
            home_path,
            mcp_environment_names=tuple(
                dict.fromkeys(
                    env_name for server in role.mcp for env_name in server.env_from
                )
            ),
            denied_commands=denied_commands,
            refresh=resume_session_id is None,
        )
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
            **approval_fields(effective_write),
            "roots": roots,
            "writable_roots": writable_roots,
            "mcp": tuple(server.id for server in role.mcp),
            "skills": tuple(skill.id for skill in role.skills),
            "profile": role.role_name,
            "request_timeout_seconds": request.timeout_seconds,
        }
        if role.network:
            # The app-server's read-only sandbox is a unit variant: it takes
            # no parameters, so network access cannot be granted without also
            # granting workspace writes. Refuse rather than widen the sandbox
            # behind a read-only profile's back; research runs on claude.
            if not effective_write:
                raise ValidationError(
                    "codex read-only sandbox cannot grant network access; "
                    "run network profiles on claude or grant write"
                )
            adapter_state["network_access"] = True
        argv = [str(config.binary)]
        if request.fast:
            argv.extend(("-c", "service_tier=fast", "-c", "features.fast_mode=true"))
        argv.append("app-server")
        return LaunchPlan(
            argv=tuple(argv),
            cwd=workdir,
            environment=MappingProxyType(environment),
            # app-server has no system-prompt argument, so prepend the role.
            initial_input=f"{role.prompt}\n\n{request.task}",
            runtime_stream_path=Path(agent_dir) / "runtime.jsonl",
            adapter_state=MappingProxyType(adapter_state),
            answer_path=Path(agent_dir) / "answer.md",
            resume_session_id=resume_session_id,
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
