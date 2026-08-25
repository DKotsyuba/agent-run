"""Claude Code runtime adapter: strict isolation, no live auth/quota calls.

Materialization and launch preparation never inherit the caller's global
Claude settings, plugins, or MCP configuration. Every generated asset is
built only from ``RuntimeConfig``, the selected ``AgentProfile``, and the
owner-authored skill directories below ``~/.agent-run/skills/claude``.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import threading
import time
import uuid
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from ...config import McpConfig, RuntimeConfig, RuntimeHookConfig
from ...domain import AgentStatus, Outcome, StartRequest
from ...errors import ValidationError
from ...paths import agent_run_home
from ...profiles import AgentProfile
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
from ..home import content_hash, create_symlink_bridge, write_managed_file
from .stream import StreamDecoder

__all__ = ["ADAPTER_API_VERSION", "ADAPTER", "ClaudeAdapter"]

_CAPABILITIES = frozenset(
    {
        Capability.STEER,
        Capability.EFFORT,
        Capability.OUTPUT_SCHEMA,
        Capability.READ_ROOTS,
        Capability.WRITE,
        Capability.TRANSCRIPT,
        Capability.MODEL_ROSTER,
        Capability.MCP,
        Capability.SKILLS,
        Capability.HOOKS,
    }
)

_READ_TOOLS = ("Read", "Grep", "Glob")
_WRITE_TOOLS = ("Edit", "Write", "NotebookEdit")
_BASH_TOOL = ("Bash",)
_ALWAYS_DISALLOWED = ("WebFetch", "WebSearch")
_AUTH_NAMES = frozenset({"CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"})
_KNOWN_HOOK_EVENTS = frozenset(
    {
        "PreToolUse",
        "PostToolUse",
        "UserPromptSubmit",
        "Stop",
        "SubagentStop",
        "Notification",
        "PreCompact",
        "SessionStart",
        "SessionEnd",
    }
)


def _skills_root() -> Path:
    return agent_run_home() / "skills" / "claude"


def _render_settings(home: Path, hooks: tuple[RuntimeHookConfig, ...]) -> str:
    """Write the generated settings.json holding only declared hooks."""

    grouped: dict[str, list[dict[str, object]]] = {}
    for hook in hooks:
        entry: dict[str, object] = {"hooks": [{"type": "command", "command": " ".join(hook.command)}]}
        if hook.matcher is not None:
            entry["matcher"] = hook.matcher
        grouped.setdefault(hook.event, []).append(entry)
    settings = {"hooks": grouped} if grouped else {}
    return write_managed_file(home, "settings.json", json.dumps(settings, sort_keys=True))


def _render_mcp_config(
    home: Path, names: tuple[str, ...], mcp_servers: Mapping[str, McpConfig]
) -> str:
    """Write the strict MCP config for the selected names only.

    Fails closed when a configured name has no resolved definition rather
    than emitting a non-functional entry.
    """

    if not names:
        return content_hash("no_mcp")
    servers: dict[str, object] = {}
    for name in names:
        server = mcp_servers.get(name)
        if server is None:
            raise ValidationError(f"no resolved MCP definition for runtimes.claude.mcp entry: {name}")
        servers[name] = {
            "type": server.transport,
            "command": str(server.command),
            "args": list(server.args),
        }
    return write_managed_file(home, "mcp/mcp-config.json", json.dumps({"mcpServers": servers}, sort_keys=True))


def _render_plugin_dirs(home: Path, skills_root: Path, names: tuple[str, ...]) -> str:
    """Generate one plugin directory per selected skill, symlinking its owner-authored SKILL.md."""

    digests = []
    for name in sorted(names):
        source = skills_root / name / "SKILL.md"
        if not source.exists():
            raise ValidationError(f"claude skill not found: {name}")
        manifest_digest = write_managed_file(
            home, f"plugins/{name}/.claude-plugin/plugin.json", json.dumps({"name": name}, sort_keys=True)
        )
        create_symlink_bridge(home, f"plugins/{name}/skills/{name}/SKILL.md", source)
        digests.append(f"{name}:{manifest_digest}")
    return content_hash(",".join(digests)) if digests else content_hash("no_skills")


class ClaudeAdapter:
    """Runtime adapter for the ``claude`` engine."""

    def describe(self) -> RuntimeInfo:
        return RuntimeInfo("claude", ADAPTER_API_VERSION, _CAPABILITIES)

    def validate(self, config: RuntimeConfig) -> None:
        if config.service_mode is not None:
            raise ValidationError("claude runtime does not support service_mode")
        if config.auth is None:
            raise ValidationError("claude runtime requires an auth bridge")
        if config.auth.kind != "environment":
            raise ValidationError("claude runtime auth.kind must be 'environment'")
        unknown = sorted(set(config.auth.names) - _AUTH_NAMES)
        if unknown:
            raise ValidationError(
                f"claude runtime auth.names has unsupported entries: {', '.join(unknown)}"
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
        mcp_servers: Mapping[str, McpConfig] = MappingProxyType({}),
    ) -> str:
        """Render settings, strict MCP config, and plugin dirs into ``home``.

        ``mcp_servers`` is an optional resolution of the selected MCP names to
        their full definitions; it is not part of the frozen adapter
        interface, so callers that only pass ``(config, home)`` still work as
        long as ``config.mcp`` is empty. A configured but unresolved MCP name
        fails closed rather than emitting a non-functional entry.
        """

        settings_digest = _render_settings(home, config.hooks)
        mcp_digest = _render_mcp_config(home, config.mcp, mcp_servers)
        plugin_digest = _render_plugin_dirs(home, _skills_root(), config.skills)
        return "\n".join((settings_digest, mcp_digest, plugin_digest))

    def probe(self, config: RuntimeConfig, home: Path) -> RuntimeHealth:
        available = config.binary.exists() and os.access(config.binary, os.X_OK)
        authenticated: bool | None = None
        if config.auth is not None and config.auth.kind == "environment":
            authenticated = any(name in os.environ for name in config.auth.names)
        reason = None if available else f"claude binary not executable: {config.binary}"
        return RuntimeHealth(available, None, authenticated, reason)

    def models(self, config: RuntimeConfig, home: Path) -> tuple[ModelInfo, ...]:
        return tuple(ModelInfo(model, f"configured claude model: {model}") for model in config.models)

    def limits(self, config: RuntimeConfig, home: Path) -> tuple[LimitSample, ...]:
        return ()

    def prepare(
        self,
        request: StartRequest,
        profile: AgentProfile,
        config: RuntimeConfig,
        home: Path,
        agent_dir: Path,
        *,
        mcp_servers: Mapping[str, McpConfig] = MappingProxyType({}),
    ) -> LaunchPlan:
        if request.model not in config.models:
            raise ValidationError(f"model is not in the configured roster: {request.model}")

        allow_write = profile.write
        allowed_tools = _READ_TOOLS + (_WRITE_TOOLS + _BASH_TOOL if allow_write else ())
        allowed_tools += tuple(f"mcp__{name}" for name in config.mcp)
        permission_mode = "acceptEdits" if allow_write else "default"

        system_prompt_parts = [profile.body]
        if request.effort is not None:
            system_prompt_parts.append(f"Apply reasoning effort: {request.effort}.")
        if request.output_schema is not None:
            schema_text = json.dumps(request.output_schema, sort_keys=True)
            system_prompt_parts.append(
                "Respond with a final message containing only valid JSON matching this "
                f"schema, with no surrounding prose: {schema_text}"
            )

        session_id = str(uuid.uuid4())
        roots = tuple(dict.fromkeys((request.workdir, *profile.read_roots)))

        argv: list[str] = [
            str(config.binary),
            "--print",
            "--output-format",
            "stream-json",
            "--input-format",
            "stream-json",
            "--verbose",
            "--model",
            request.model,
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
        for name in config.skills:
            argv += ["--plugin-dir", str(home / "plugins" / name)]
        for root in roots:
            argv += ["--add-dir", str(root)]
        argv += ["--allowedTools", ",".join(allowed_tools)]
        argv += ["--disallowedTools", ",".join(_ALWAYS_DISALLOWED)]
        argv += ["--append-system-prompt", "\n\n".join(system_prompt_parts)]
        argv += ["--session-id", session_id]

        environment: dict[str, str] = {}
        path_value = os.environ.get("PATH")
        if path_value:
            environment["PATH"] = path_value
        if config.auth is not None:
            for name in config.auth.names:
                value = os.environ.get(name)
                if value:
                    environment[name] = value
        for name in config.mcp:
            for env_name in _mcp_env_names(mcp_servers, name):
                value = os.environ.get(env_name)
                if value:
                    environment[env_name] = value

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
                }
            ),
        )

    def launch(self, plan: LaunchPlan, sink: EventSink) -> "ClaudeSession":
        process = subprocess.Popen(
            list(plan.argv),
            cwd=str(plan.cwd),
            env=dict(plan.environment),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
            start_new_session=True,
        )
        return ClaudeSession(process, plan, sink)


def _mcp_env_names(mcp_servers: Mapping[str, McpConfig], name: str) -> tuple[str, ...]:
    server = mcp_servers.get(name)
    return server.env_from if server is not None else ()


class ClaudeSession:
    """A launched Claude Code child process and its stream reader thread."""

    def __init__(self, process: subprocess.Popen, plan: LaunchPlan, sink: EventSink) -> None:
        self._process = process
        self._plan = plan
        self._sink = sink
        self._decoder = StreamDecoder()
        self._lock = threading.Lock()
        self._raw_stream = plan.runtime_stream_path.open("a", encoding="utf-8")
        self._cancelled = False
        self._reader = threading.Thread(target=self._read_stdout, daemon=True)
        if plan.initial_input and process.stdin is not None:
            try:
                process.stdin.write(plan.initial_input)
                process.stdin.flush()
            except (BrokenPipeError, OSError):
                pass
        self._reader.start()

    @property
    def pid(self) -> int | None:
        return self._process.pid

    def _read_stdout(self) -> None:
        stdout = self._process.stdout
        if stdout is None:
            return
        for raw_line in stdout:
            with self._lock:
                self._raw_stream.write(raw_line if raw_line.endswith("\n") else raw_line + "\n")
                self._raw_stream.flush()
                result = self._decoder.feed(raw_line, at=time.time())
            if result.session_id:
                self._sink.session(result.session_id)
            for message in result.messages:
                self._sink.message(message)
            if result.event:
                self._sink.event(*result.event)
            if result.warning:
                self._sink.event("stream_diagnostic", {"reason": result.warning})

    def steer(self, text: str) -> None:
        stdin = self._process.stdin
        if stdin is None or self._process.poll() is not None:
            return
        line = (
            json.dumps(
                {"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": text}]}},
                sort_keys=True,
            )
            + "\n"
        )
        try:
            stdin.write(line)
            stdin.flush()
        except (BrokenPipeError, OSError):
            pass

    def cancel(self, grace_seconds: float) -> None:
        self._cancelled = True
        if self._process.poll() is not None:
            return
        try:
            self._process.send_signal(signal.SIGINT)
        except OSError:
            return
        deadline = time.time() + max(grace_seconds, 0.0)
        while time.time() < deadline and self._process.poll() is None:
            time.sleep(0.05)

    def wait(self, timeout_seconds: float | None) -> Outcome | None:
        try:
            exit_code = self._process.wait(timeout=timeout_seconds)
        except subprocess.TimeoutExpired:
            return None
        self._reader.join(timeout=5)
        with self._lock:
            self._raw_stream.close()
        metadata = self._decoder.finalize()
        if self._cancelled:
            status = AgentStatus.CANCELLED
        elif exit_code == 0 and not metadata.is_error and metadata.subtype != "no_answer":
            status = AgentStatus.SUCCEEDED
        else:
            status = AgentStatus.FAILED
        failure_kind = None if status == AgentStatus.SUCCEEDED else (metadata.subtype or "error")
        failure_text = None if status == AgentStatus.SUCCEEDED else metadata.result_text
        return Outcome(
            status=status,
            exit_code=exit_code,
            failure_kind=failure_kind,
            failure_text=failure_text,
            runtime_session_id=metadata.runtime_session_id,
        )


ADAPTER = ClaudeAdapter()
