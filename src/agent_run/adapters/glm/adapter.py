"""GLM Coding Plan runtime: the claude engine pointed at Z.ai's endpoint.

Z.ai's GLM Coding Plan speaks the Anthropic Messages protocol, which the
claude CLI already implements, so this adapter is a thin subclass of
:class:`agent_run.adapters.claude.adapter.ClaudeAdapter`. The only
differences are the runtime name, the auth contract
(``ANTHROPIC_AUTH_TOKEN`` + ``ANTHROPIC_BASE_URL`` with a Keychain fallback
for the key and the plan's endpoint as the default base URL), and an
``ANTHROPIC_MODEL`` export matching the model the coding-helper aliases.
Everything else -- argv shape, stream decoding, session lifecycle, hook and
plugin semantics -- is the claude adapter's, unmodified. Auth resolution
plugs into the claude adapter's ``_auth_environment`` hook, so no process
global state is touched and concurrent claude launches cannot observe GLM
credentials.
"""

from __future__ import annotations

import os
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from ...config import McpConfig, RuntimeConfig
from ...domain import StartRequest
from ...errors import ValidationError
from ...profiles import AgentProfile
from ..base import ADAPTER_API_VERSION, LaunchPlan, RuntimeHealth, RuntimeInfo
from ..claude.adapter import _KNOWN_HOOK_EVENTS, ClaudeAdapter
from ..plugin_skills import unlisted_plugin_skills
from .auth import DEFAULT_BASE_URL, keychain_glm_key

__all__ = ["ADAPTER", "ADAPTER_API_VERSION", "GlmAdapter"]

_AUTH_NAMES = frozenset({"ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_BASE_URL"})
_TOKEN_ENV_NAME = "ANTHROPIC_AUTH_TOKEN"
_BASE_URL_ENV_NAME = "ANTHROPIC_BASE_URL"
_MODEL_ENV_NAME = "ANTHROPIC_MODEL"
#: Models whose real window is 1M (1_048_576, per Z.ai's registry). Claude
#: Code clamps unknown models to 200k unless the id carries the ``[1m]``
#: suffix, so the adapter appends it for these — verified live 2026-08-30:
#: without the suffix the CLI announces a 200k auto-compact ceiling, with it
#: the warning disappears and z.ai still answers.
_MILLION_CONTEXT_MODELS = frozenset({"glm-5.3", "glm-5.3-flash", "glm-5.2"})
_MILLION_SUFFIX = "[1m]"


def _glm_environment() -> dict[str, str]:
    """Resolve the GLM auth pair: keychain first, the plan endpoint always.

    The orchestrator's own process environment is NOT a source here: Claude
    Code exports ANTHROPIC_BASE_URL (api.anthropic.com) into every shell it
    runs, and an inherited ANTHROPIC_AUTH_TOKEN would be the orchestrator's
    Anthropic credential — either inherited value silently points the GLM
    child at the wrong provider (proven live 2026-08-30: the plan key was
    sent to api.anthropic.com → 401). The keychain entry is the identity of
    this runtime; env is only a last-resort fallback when the keychain has
    nothing. The base URL is the plan endpoint, full stop.
    """

    token = keychain_glm_key() or os.environ.get(_TOKEN_ENV_NAME)
    if not token:
        raise ValidationError(
            "glm requires the macOS Keychain entry 'com.pluto.agent-run.glm' "
            "(account GLM_CODING_KEY) or ANTHROPIC_AUTH_TOKEN in the process "
            "environment"
        )
    return {
        _TOKEN_ENV_NAME: token,
        _BASE_URL_ENV_NAME: DEFAULT_BASE_URL,
    }


class GlmAdapter(ClaudeAdapter):
    """Runtime adapter for the ``glm`` engine (claude CLI at api.z.ai)."""

    def describe(self) -> RuntimeInfo:
        base = super().describe()
        return RuntimeInfo("glm", base.adapter_api_version, base.capabilities)

    def validate(self, config: RuntimeConfig) -> None:
        # Auth contract differs from claude's (a keychain-backed token plus a
        # defaulted base URL), so the auth checks are glm's own; the
        # remaining semantics -- service mode, whole-plugin skill listing,
        # known hook events -- mirror the claude adapter verbatim.
        if config.service_mode is not None:
            raise ValidationError("glm runtime does not support service_mode")
        if config.auth is None:
            raise ValidationError("glm runtime requires an auth bridge")
        if config.auth.kind != "environment":
            raise ValidationError("glm runtime auth.kind must be 'environment'")
        unknown = sorted(set(config.auth.names) - _AUTH_NAMES)
        if unknown:
            raise ValidationError(
                f"glm runtime auth.names has unsupported entries: {', '.join(unknown)}"
            )
        unlisted = unlisted_plugin_skills(config.plugins, config.skills)
        if unlisted:
            raise ValidationError(
                "glm loads each declared plugin whole, so runtimes.glm.skills "
                "must list every skill they ship; unlisted: " + ", ".join(unlisted)
            )
        for index, hook in enumerate(config.hooks):
            if hook.event not in _KNOWN_HOOK_EVENTS:
                raise ValidationError(
                    f"runtimes.glm.hooks[{index}].event is not a known Claude hook event: {hook.event!r}"
                )

    def probe(self, config: RuntimeConfig, home: Path) -> RuntimeHealth:
        """Report local binary and credential availability; never calls the network."""

        del home
        available = config.binary.exists() and os.access(config.binary, os.X_OK)
        authenticated = bool(os.environ.get(_TOKEN_ENV_NAME)) or keychain_glm_key() is not None
        reason = None if available else f"glm binary not executable: {config.binary}"
        return RuntimeHealth(available, None, authenticated, reason)

    def _auth_environment(self, binary: Path, names: tuple[str, ...]) -> Mapping[str, str]:
        """Supply the resolved GLM pair instead of claude's own auth bridge."""

        del binary, names
        return _glm_environment()

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
        plan = super().prepare(
            request, profile, config, home, agent_dir, mcp_servers=mcp_servers
        )
        cli_model = request.model
        argv = plan.argv
        if request.model in _MILLION_CONTEXT_MODELS:
            cli_model = request.model + _MILLION_SUFFIX
            rebuilt = list(argv)
            for index, value in enumerate(rebuilt[:-1]):
                if value == "--model":
                    rebuilt[index + 1] = cli_model
            argv = tuple(rebuilt)
        environment = dict(plan.environment)
        # The coding-helper convention: aliases like glm-5.3 resolve through
        # ANTHROPIC_MODEL, belt and suspenders alongside the --model flag.
        environment[_MODEL_ENV_NAME] = cli_model
        return replace(plan, argv=argv, environment=MappingProxyType(environment))


ADAPTER = GlmAdapter()
