"""Claude Code runtime adapter: strict isolation, no live auth/quota calls.

Generated assets use only declared configuration, profiles, and skills. An
existing uv managed-Python root is the sole ambient exception for offline hooks.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from ...config import McpConfig, RuntimeConfig
from ...domain import StartRequest
from ...errors import ValidationError
from ..home import content_hash, managed_uv_python_environment
from ...profiles import AgentProfile, normalize_read_roots
from ..base import (
    ADAPTER_API_VERSION,
    Capability,
    EventSink,
    LaunchPlan,
    LimitSample,
    ModelInfo,
    RuntimeHealth,
    RuntimeInfo,
)
from ..command_policy import materialize_refusal_commands, render_claude_denials
from ..continuation import cli_resume_plan
from ..developer_environment import (
    configured_environment_keys,
    developer_environment,
    environment_digest,
)
from ..version import observe_binary_version
from ..plugin_skills import local_skill_names, unlisted_plugin_skills
from ..rust import RUST_ENVIRONMENT_NAMES
from ..snapshots import finalize_runtime_snapshots
from .auth import TOKEN_ENV_NAME, auth_environment, keychain_token
from .constants import (
    ALWAYS_DISALLOWED as _ALWAYS_DISALLOWED, AUTH_NAMES as _AUTH_NAMES,
    CAPABILITIES as _CAPABILITIES, KNOWN_HOOK_EVENTS as _KNOWN_HOOK_EVENTS,
    MODEL_ALIASES as _MODEL_ALIASES, MODEL_DESCRIPTIONS as _MODEL_DESCRIPTIONS,
    NETWORK_TOOLS as _NETWORK_TOOLS,
    READ_TOOLS as _READ_TOOLS, SHELL_TOOLS as _SHELL_TOOLS,
    SKILL_TOOLS as _SKILL_TOOLS, SUPPORTED_EFFORTS as _SUPPORTED_EFFORTS,
    WRITE_TOOLS as _WRITE_TOOLS,
)
from .launch_io import abort_launch
from .limits import agent_rate_limit_samples
from .materialize import (
    render_declared_plugin_snapshots,
    render_mcp_config,
    render_plugin_dirs,
    render_settings,
)
from .session import ClaudeSession
from .stream import is_secret_env_name

__all__ = ["ADAPTER_API_VERSION", "ADAPTER", "ClaudeAdapter"]

class ClaudeAdapter:
    """Runtime adapter for the ``claude`` engine."""

    def describe(self) -> RuntimeInfo:
        return RuntimeInfo("claude", ADAPTER_API_VERSION, _CAPABILITIES)

    def validate(self, config: RuntimeConfig) -> None:
        """Validate Claude-specific runtime configuration before materialization.

        ``config`` must provide environment auth, supported hooks, and complete
        plugin skill declarations. Rust provisioning is accepted here and
        applied only by ``prepare``; invalid auth, plugins, or hooks raise
        ``ValidationError`` without touching the filesystem.
        """
        if config.auth is None:
            raise ValidationError("claude runtime requires an auth bridge")
        if config.auth.kind != "environment":
            raise ValidationError("claude runtime auth.kind must be 'environment'")
        unknown = sorted(set(config.auth.names) - _AUTH_NAMES)
        if unknown:
            raise ValidationError(
                f"claude runtime auth.names has unsupported entries: {', '.join(unknown)}"
            )
        unlisted = unlisted_plugin_skills(config.plugins, config.skills)
        if unlisted:
            raise ValidationError(
                "claude loads each declared plugin whole, so runtimes.claude.skills "
                "must list every skill they ship; unlisted: " + ", ".join(unlisted)
            )
        allowed_events = _KNOWN_HOOK_EVENTS
        for index, hook in enumerate(config.hooks):
            if hook.event not in allowed_events:
                raise ValidationError(
                    f"runtimes.claude.hooks[{index}].event is not a known Claude hook event: {hook.event!r}"
                )

    def materialize(
        self,
        config: RuntimeConfig,
        home: Path,
        *,
        mcp_servers: Mapping[str, McpConfig],
        skills_root: Path | None = None,
    ) -> str:
        """Render settings, strict MCP config, and plugin dirs into ``home``.

        ``mcp_servers`` is the caller's resolution of the selected MCP names
        to their full definitions, required per the frozen adapter contract.
        A configured but unresolved MCP name fails closed rather than
        emitting a non-functional entry.

        The returned newline-delimited digest includes declared Rust roots when
        provisioning is enabled and a selected developer-environment preset's
        declared paths, variables, and command policy, so materialization
        revision evidence changes with the launch environment without storing
        credentials.
        """

        settings_digest = render_settings(home, config.hooks)
        mcp_digest = render_mcp_config(home, config.mcp, mcp_servers)
        if skills_root is None and not config.skills:
            skills_root = Path(home)
        if not isinstance(skills_root, Path) or not skills_root.is_absolute():
            raise ValidationError("claude skills_root must be absolute")
        plugin_digest = render_plugin_dirs(home, skills_root, local_skill_names(config.plugins, config.skills))
        snapshot_assets = getattr(config, "plugin_snapshot_assets", {})
        declared_digest = render_declared_plugin_snapshots(
            home, config.plugins, snapshot_assets
        )
        digests = [settings_digest, mcp_digest, plugin_digest, declared_digest]
        if config.rust is not None:
            digests.append(content_hash(f"{config.rust.rustup_home}\0{config.rust.cargo_bin}"))
        digests.append(environment_digest(config))
        revision = "\n".join(digests)
        managed_files = (
            "settings.json",
            *(("mcp/mcp-config.json",) if config.mcp else ()),
        )
        finalize_runtime_snapshots(home, revision, managed_files)
        return revision

    def probe(self, config: RuntimeConfig, home: Path) -> RuntimeHealth:
        """Report local health with a fresh bounded configured-binary version."""

        available = config.binary.exists() and os.access(config.binary, os.X_OK)
        authenticated: bool | None = None
        if config.auth is not None and config.auth.kind == "environment":
            authenticated = any(name in os.environ for name in config.auth.names)
            if not authenticated and TOKEN_ENV_NAME in config.auth.names:
                # ``prepare`` can source this launch from the Keychain, so
                # reporting "unauthenticated" on a bare environment would be
                # a false alarm. Read only -- probe never refreshes.
                authenticated = keychain_token(time.time()) is not None
        version, version_reason = observe_binary_version(config.binary, Path(home))
        reason = version_reason if available else f"claude binary not executable: {config.binary}"
        return RuntimeHealth(available, version, authenticated, reason)

    def models(self, config: RuntimeConfig, home: Path) -> tuple[ModelInfo, ...]:
        """Report the configured roster without any live call.

        Ids are always the configured public ids; ``fable`` is additionally
        described as Claude Fable 5.1 with its API id
        (``claude-fable-5-1``), while every other configured id keeps the
        generic ``configured claude model`` description.
        """

        return tuple(
            ModelInfo(model, _MODEL_DESCRIPTIONS.get(model, f"configured claude model: {model}"))
            for model in config.models
        )

    def limits(self, config: RuntimeConfig, home: Path) -> tuple[LimitSample, ...]:
        return agent_rate_limit_samples(Path(home), time.time())

    def _auth_environment(self, binary: Path, names: tuple[str, ...]) -> Mapping[str, str]:
        """Resolve the auth env for a child; subclasses may supply their own."""
        return auth_environment(binary, names)

    def prepare(
        self,
        request: StartRequest,
        profile: AgentProfile,
        config: RuntimeConfig,
        home: Path,
        agent_dir: Path,
        *,
        mcp_servers: Mapping[str, McpConfig],
        resume_session_id: str | None = None,
    ) -> LaunchPlan:
        """Build the isolated launch plan for one start request.

        Public model ids remain on the plan boundary; only child argv aliases
        ``fable``. Presets add declared paths, variables, Rust, and command
        denials to the isolated environment; invalid inputs raise before launch.
        """

        if request.fast:
            raise ValidationError(f"{request.runtime} runtime does not support fast mode")
        if request.model not in config.models:
            raise ValidationError(f"model is not in the configured roster: {request.model}")
        if request.effort is not None and request.effort not in _SUPPORTED_EFFORTS:
            raise ValidationError(
                f"claude runtime effort must be one of {sorted(_SUPPORTED_EFFORTS)}: {request.effort!r}"
            )

        if request.write and not profile.write:
            raise ValidationError("claude profile does not allow requested write access")
        allow_write = request.write and profile.write
        declared_roots = tuple(
            normalize_read_roots((root,))[0]
            for root in (*profile.read_roots, *request.read_roots)
        )
        roots = normalize_read_roots((request.workdir, *declared_roots))
        if allow_write:
            for root in declared_roots:
                if root != request.workdir and root.is_relative_to(request.workdir):
                    raise ValidationError(
                        "claude runtime cannot keep a read root read-only while it is nested "
                        f"inside a writable workdir: {root}"
                    )

        skill_tools = _SKILL_TOOLS if config.skills else ()
        shell_tools = _SHELL_TOOLS if allow_write else ()
        network_tools = _NETWORK_TOOLS if profile.network else ()
        base_tools = (
            _READ_TOOLS + skill_tools + (_WRITE_TOOLS if allow_write else ()) + shell_tools + network_tools
        )
        write_scope = tuple(f"{tool}({request.workdir}/**)" for tool in _WRITE_TOOLS) if allow_write else ()
        allowed_tools = (
            _READ_TOOLS + skill_tools + write_scope + shell_tools + network_tools
            + tuple(f"mcp__{name}" for name in config.mcp)
        )
        disallowed_tools = () if profile.network else _ALWAYS_DISALLOWED
        permission_mode = "acceptEdits" if allow_write else "default"

        system_prompt_parts = [profile.body]
        if request.output_schema is not None:
            schema_text = json.dumps(request.output_schema, sort_keys=True)
            system_prompt_parts.append(
                "Respond with a final message containing only valid JSON matching this "
                f"schema, with no surrounding prose: {schema_text}"
            )

        session_id = str(uuid.uuid4())

        argv: list[str] = [
            str(config.binary),
            "--print",
            "--output-format",
            "stream-json",
            "--input-format",
            "stream-json",
            "--verbose",
            "--model",
            _MODEL_ALIASES.get(request.model, request.model),
            "--permission-mode",
            permission_mode,
            "--setting-sources",
            "",
            "--strict-mcp-config",
            "--settings",
            str(home / "settings.json"),
        ]
        if config.mcp:
            argv += ["--mcp-config", str(home / "mcp" / "mcp-config.json")]
        for name in local_skill_names(config.plugins, config.skills):
            argv += ["--plugin-dir", str(home / "plugins" / name)]
        for plugin in config.plugins:
            selected = getattr(config, "plugin_snapshot_assets", {}).get(plugin.name)
            argv += [
                "--plugin-dir",
                str(home / "declared-plugins" / plugin.name) if selected else str(plugin),
            ]
        for root in roots:
            argv += ["--add-dir", str(root)]
        argv += ["--tools", ",".join(base_tools)]
        argv += ["--allowedTools", ",".join(allowed_tools)]
        argv += ["--disallowedTools", ",".join(disallowed_tools)]
        if request.effort is not None:
            argv += ["--effort", request.effort]
        argv += ["--append-system-prompt", "\n\n".join(system_prompt_parts)]
        argv += ["--session-id", session_id]

        environment: dict[str, str] = {"HOME": str(home), **managed_uv_python_environment()}
        path_value = os.environ.get("PATH")
        if path_value:
            environment["PATH"] = path_value
        environment = developer_environment(environment, config, request.workdir)

        selected_environment = config.environment
        if selected_environment is not None and selected_environment.denied_commands:
            policy = materialize_refusal_commands(
                selected_environment.denied_commands,
                agent_dir / "command-policy",
                search_paths=tuple(
                    part for part in environment.get("PATH", "").split(os.pathsep) if part
                ),
                environment=environment,
            )
            environment["PATH"] = os.pathsep.join(
                (str(policy.directory), *environment.get("PATH", "").split(os.pathsep))
            )
            denial_patterns = render_claude_denials(
                selected_environment.denied_commands,
                command_paths=tuple(policy.resolved_commands.values()),
            )
            argv[argv.index("--disallowedTools") + 1] = ",".join(
                dict.fromkeys((*disallowed_tools, *denial_patterns))
            )

        auth_names: tuple[str, ...] = ()
        injected_secret_names: tuple[str, ...] = ()
        if config.auth is not None:
            auth_names = config.auth.names
            injected = self._auth_environment(config.binary, auth_names)
            environment.update(injected)
            injected_secret_names = tuple(
                name for name in injected if is_secret_env_name(name)
            )

        configured_keys = configured_environment_keys(config)
        mcp_env_names: list[str] = []
        for name in config.mcp:
            server = mcp_servers.get(name)
            if server is None:
                raise ValidationError(f"no resolved MCP definition for runtimes.claude.mcp entry: {name}")
            for env_name in server.env_from:
                if env_name in configured_keys or (
                    config.rust is not None and env_name in RUST_ENVIRONMENT_NAMES
                ):
                    value = environment.get(env_name)
                else:
                    value = os.environ.get(env_name)
                if not value:
                    detail = (
                        "Rust provisioning does not permit RUSTUP_TOOLCHAIN; use rust-toolchain files"
                        if config.rust is not None and env_name == "RUSTUP_TOOLCHAIN"
                        else f"claude mcp {name!r} requires environment variable {env_name}, which is not set"
                    )
                    raise ValidationError(
                        detail
                    )
                environment[env_name] = value
                mcp_env_names.append(env_name)

        mcp_environment = {
            name: environment[name] for name in configured_keys if name in environment
        }
        if config.mcp and mcp_environment:
            render_mcp_config(agent_dir, config.mcp, mcp_servers, environment=mcp_environment)
            argv[argv.index("--mcp-config") + 1] = str(agent_dir / "mcp" / "mcp-config.json")

        initial_input = (
            json.dumps(
                {
                    "type": "user",
                    "message": {"role": "user", "content": [{"type": "text", "text": request.task}]},
                },
                sort_keys=True,
            )
            + "\n"
        )

        return LaunchPlan(
            argv=tuple(argv),
            cwd=request.workdir,
            environment=MappingProxyType(environment),
            initial_input=initial_input,
            runtime_stream_path=agent_dir / "runtime.jsonl",
            adapter_state=MappingProxyType(
                {
                    "session_id": session_id,
                    "permission_mode": permission_mode,
                    "allowed_tools": allowed_tools,
                    "model": request.model,
                    "secret_env_names": tuple(
                        dict.fromkeys((*auth_names, *injected_secret_names, *mcp_env_names))
                    ),
                }
            ),
            answer_path=agent_dir / "answer.md",
            resume_session_id=resume_session_id,
        )

    def launch(self, plan: LaunchPlan, sink: EventSink) -> "ClaudeSession":
        """Launch a fresh or explicitly resumed Claude session in its own process group."""
        plan = cli_resume_plan(plan, session_option="--session-id")
        process = subprocess.Popen(
            list(plan.argv),
            cwd=str(plan.cwd),
            env=dict(plan.environment),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        try:
            return ClaudeSession(process, plan, sink)
        except BaseException:
            abort_launch(process)
            raise


ADAPTER = ClaudeAdapter()
