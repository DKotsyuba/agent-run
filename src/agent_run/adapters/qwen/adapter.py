"""One-shot adapter for Qwen Code's headless stream-JSON interface."""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    EventSink,
    LaunchPlan,
    LimitSample,
    ModelInfo,
    RuntimeHealth,
    RuntimeInfo,
)
from agent_run.adapters.claude.adapter import ClaudeSession
from agent_run.adapters.home import content_hash, write_managed_file
from agent_run.config import McpConfig, RuntimeConfig
from agent_run.domain import StartRequest
from agent_run.errors import ValidationError
from agent_run.profiles import AgentProfile

__all__ = ["ADAPTER", "QwenAdapter"]

_AUTH_NAMES = frozenset({"OPENAI_API_KEY", "OPENAI_BASE_URL"})
_CAPABILITIES = frozenset(
    {Capability.READ_ROOTS, Capability.WRITE, Capability.TRANSCRIPT, Capability.MODEL_ROSTER, Capability.MCP}
)


def _mcp_document(names: tuple[str, ...], servers: Mapping[str, McpConfig]) -> dict[str, object]:
    """Return strict Qwen stdio MCP settings for resolved configured names.

    Raises ``ValidationError`` when a selected server is absent or is not a
    stdio command, because silently dropping it would weaken the child role.
    """

    rendered: dict[str, object] = {}
    for name in names:
        server = servers.get(name)
        if server is None:
            raise ValidationError(f"no resolved MCP definition for runtimes.qwen.mcp entry: {name}")
        if server.transport != "stdio" or server.command is None:
            raise ValidationError(f"qwen MCP {name!r} must use stdio with a command")
        rendered[name] = {"command": str(server.command), "args": list(server.args)}
    return rendered


class QwenAdapter:
    """Adapt Qwen Code 0.22.2 as an isolated one-shot child runtime."""

    def describe(self) -> RuntimeInfo:
        """Describe the stable runtime name, API version, and capabilities."""
        return RuntimeInfo("qwen", ADAPTER_API_VERSION, _CAPABILITIES)

    def validate(self, config: RuntimeConfig) -> None:
        """Validate Qwen's environment-auth and one-shot-only configuration."""
        if config.service_mode is not None:
            raise ValidationError("qwen runtime does not support service_mode")
        if config.auth is None or config.auth.kind != "environment":
            raise ValidationError("qwen runtime auth.kind must be 'environment'")
        unknown = sorted(set(config.auth.names) - _AUTH_NAMES)
        if unknown:
            raise ValidationError(f"qwen runtime auth.names has unsupported entries: {', '.join(unknown)}")

    def materialize(
        self,
        config: RuntimeConfig,
        home: Path,
        *,
        mcp_servers: Mapping[str, McpConfig],
        skills_root: Path | None = None,
    ) -> str:
        """Create an isolated Qwen home with strict MCP and context settings."""
        del skills_root
        context_path = Path(home) / "agent-run-context.md"
        document = {
            "context": {"fileName": str(context_path)},
            "mcpServers": _mcp_document(config.mcp, mcp_servers),
            "tools": {"sandbox": True},
        }
        text = json.dumps(document, indent=2, sort_keys=True) + "\n"
        write_managed_file(Path(home), ".qwen/settings.json", text)
        return content_hash(text)

    def probe(self, config: RuntimeConfig, home: Path) -> RuntimeHealth:
        """Report local binary and declared authentication availability only."""
        del home
        available = config.binary.exists() and os.access(config.binary, os.X_OK)
        authenticated = bool(config.auth and all(os.environ.get(name) for name in config.auth.names))
        reason = None if available else f"qwen binary not executable: {config.binary}"
        return RuntimeHealth(available, "0.22.2" if available else None, authenticated, reason)

    def models(self, config: RuntimeConfig, home: Path) -> tuple[ModelInfo, ...]:
        """Return the configured model roster without a live provider call."""
        del home
        return tuple(ModelInfo(model, f"configured qwen model: {model}") for model in config.models)

    def limits(self, config: RuntimeConfig, home: Path) -> tuple[LimitSample, ...]:
        """Return no live quota samples because Qwen exposes no local quota API."""
        del config, home
        return ()

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
        """Build a sandboxed one-shot invocation and isolated environment."""
        if profile.network:
            raise ValidationError("qwen runtime does not support network profiles")
        if request.model not in config.models:
            raise ValidationError(f"model is not in the configured roster: {request.model}")
        if request.write and not profile.write:
            raise ValidationError("qwen profile does not allow requested write access")
        allow_write = request.write and profile.write
        approval_mode = "auto-edit" if allow_write else "plan"
        role_text = profile.body
        if request.output_schema is not None:
            role_text += "\n\nRespond only with JSON matching: " + json.dumps(request.output_schema, sort_keys=True)
        write_managed_file(Path(home), "agent-run-context.md", role_text + "\n")
        self.materialize(config, Path(home), mcp_servers=mcp_servers)

        environment = {"HOME": str(home), "OPENAI_MODEL": request.model}
        if os.environ.get("PATH"):
            environment["PATH"] = os.environ["PATH"]
        secret_names: list[str] = []
        assert config.auth is not None
        for name in config.auth.names:
            value = os.environ.get(name)
            if not value:
                raise ValidationError(f"qwen requires environment variable {name}, which is not set")
            environment[name] = value
            secret_names.append(name)
        for name in config.mcp:
            server = mcp_servers.get(name)
            if server is None:
                raise ValidationError(f"no resolved MCP definition for runtimes.qwen.mcp entry: {name}")
            for env_name in server.env_from:
                value = os.environ.get(env_name)
                if not value:
                    raise ValidationError(f"qwen MCP {name!r} requires environment variable {env_name}")
                environment[env_name] = value
                secret_names.append(env_name)

        argv = (
            str(config.binary), "-p", request.task, "--output-format", "stream-json",
            "--approval-mode", approval_mode, "--sandbox", "--model", request.model,
        )
        state = MappingProxyType({
            "approval_mode": approval_mode,
            "model": request.model,
            "sandbox": True,
            "write": allow_write,
            "workdir": str(request.workdir),
            "secret_env_names": tuple(dict.fromkeys(secret_names)),
        })
        return LaunchPlan(
            argv=argv,
            cwd=request.workdir,
            environment=MappingProxyType(environment),
            initial_input="",
            runtime_stream_path=agent_dir / "runtime.jsonl",
            adapter_state=state,
            answer_path=agent_dir / "answer.md",
        )

    def launch(self, plan: LaunchPlan, sink: EventSink) -> ClaudeSession:
        """Launch Qwen in its own process group and decode its JSONL stream."""
        process = subprocess.Popen(
            list(plan.argv), cwd=str(plan.cwd), env=dict(plan.environment), stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, bufsize=1,
            start_new_session=True,
        )
        return ClaudeSession(process, plan, sink)


ADAPTER = QwenAdapter()
