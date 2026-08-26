"""Claude Code runtime adapter: strict isolation, no live auth/quota calls.

Materialization and launch preparation never inherit the caller's global
Claude settings, plugins, or MCP configuration. Every generated asset is
built only from ``RuntimeConfig``, the selected ``AgentProfile``, and the
owner-authored skill directories below ``~/.agent-run/skills/claude``.
"""

from __future__ import annotations

import contextlib
import json
import math
import os
import shlex
import signal
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

from ...config import McpConfig, RuntimeConfig, RuntimeHookConfig
from ...domain import AgentStatus, Outcome, StartRequest
from ...errors import ValidationError
from ..home import seal_answer
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
from ..home import content_hash, create_symlink_bridge, write_managed_file
from .stream import StreamDecoder, sanitize_line, terminal_event_data

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
        Capability.LIVE_LIMITS,
        Capability.MCP,
        Capability.SKILLS,
        Capability.HOOKS,
    }
)

_READ_TOOLS = ("Read", "Grep", "Glob")
_WRITE_TOOLS = ("Edit", "Write", "NotebookEdit")
_ALWAYS_DISALLOWED = ("WebFetch", "WebSearch")
_AUTH_NAMES = frozenset({"CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN"})
_SUPPORTED_EFFORTS = frozenset({"low", "medium", "high", "xhigh", "max"})
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

_LIMITS_STALE_SECONDS = 900
_AGENT_RUNTIME_FILES = 24
_RUNTIME_TAIL_BYTES = 262_144
_RUNTIME_TAIL_LINES = 2_048


def _timestamp(value: object) -> datetime | None:
    """Convert an epoch second to UTC; anything unrepresentable is unknown."""

    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if not math.isfinite(value):
        return None
    try:
        return datetime.fromtimestamp(value, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _tail_lines(path: Path) -> tuple[str, ...]:
    """Read a bounded complete-line tail from one agent's runtime stream."""

    with path.open("rb") as stream:
        stream.seek(0, 2)
        end = stream.tell()
        stream.seek(max(0, end - _RUNTIME_TAIL_BYTES))
        data = stream.read(_RUNTIME_TAIL_BYTES)
    if end > _RUNTIME_TAIL_BYTES:
        data = data.split(b"\n", 1)[-1]
    return tuple(data.decode("utf-8").splitlines()[-_RUNTIME_TAIL_LINES:])


def _agent_rate_limit_samples(home: Path, now: float) -> tuple[LimitSample, ...]:
    """Return the newest ``rate_limit_event`` samples from sibling agent dirs.

    In stream-json mode the claude CLI emits a ``rate_limit_event`` line on
    stdout; ``ClaudeSession._read_stdout`` already persists every sanitized
    stdout line verbatim to each agent's ``runtime.jsonl`` below the shared
    agent-run home (``<agent_run_home>/agents/<agent_id>/runtime.jsonl``),
    three levels above this adapter's own generated
    ``<agent_run_home>/runtimes/claude/home``. This scans the newest such
    files for the latest event; anything unreadable, malformed, or older
    than ``_LIMITS_STALE_SECONDS`` yields ``unknown`` per the M009 contract.
    """

    try:
        agents_root = Path(home).resolve(strict=True).parents[2] / "agents"
        if agents_root.is_symlink():
            return ()
        agents_root = agents_root.resolve(strict=True)
        paths = agents_root.glob("*/runtime.jsonl")
    except (IndexError, OSError, RuntimeError, ValueError):
        return ()

    newest: list[tuple[float, str, Path]] = []
    try:
        for path in paths:
            try:
                if path.is_symlink():
                    continue
                resolved = path.resolve(strict=True)
                resolved.relative_to(agents_root)
                if not resolved.is_file():
                    continue
                candidate = (resolved.stat().st_mtime, str(resolved), resolved)
                newest = sorted((*newest, candidate), reverse=True)[:_AGENT_RUNTIME_FILES]
            except (OSError, RuntimeError, ValueError):
                continue
    except OSError:
        pass

    for mtime, _name, path in newest:
        try:
            lines = _tail_lines(path)
        except (OSError, UnicodeError, ValueError):
            continue
        stale = now - mtime > _LIMITS_STALE_SECONDS
        observed_at = _timestamp(mtime)
        for line in reversed(lines):
            if '"rate_limit_event"' not in line:
                continue
            try:
                event = json.loads(line)
            except (TypeError, ValueError):
                continue
            if not isinstance(event, dict) or event.get("type") != "rate_limit_event":
                continue
            info = event.get("rate_limit_info")
            windows = info.get("unifiedWindows") if isinstance(info, dict) else None
            if not isinstance(windows, dict):
                continue
            samples = []
            for window_name in sorted(windows):
                window = windows[window_name]
                if not isinstance(window, dict):
                    continue
                utilization = window.get("utilization")
                if (
                    isinstance(utilization, bool)
                    or not isinstance(utilization, (int, float))
                    or not math.isfinite(utilization)
                ):
                    continue
                samples.append(
                    LimitSample(
                        lane="usage",
                        window=window_name,
                        remaining_percent=None
                        if stale
                        else max(0.0, min(100.0, (1.0 - float(utilization)) * 100.0)),
                        reset_at=_timestamp(window.get("resetsAt")),
                        observed_at=observed_at,
                        source="unknown" if stale else "runtime_stream_evidence",
                    )
                )
            if samples:
                return tuple(samples)
    return ()


def _render_settings(home: Path, hooks: tuple[RuntimeHookConfig, ...]) -> str:
    """Write the generated settings.json holding only declared hooks."""

    grouped: dict[str, list[dict[str, object]]] = {}
    for hook in hooks:
        entry: dict[str, object] = {"hooks": [{"type": "command", "command": shlex.join(hook.command)}]}
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
    """Generate one plugin directory per selected skill.

    Every top-level child of the owner-authored skill directory (SKILL.md,
    plus any scripts/, references/, assets/, or other sibling) is bridged
    into the generated plugin's skill directory through the same validated
    symlink bridge, not just the manifest file.
    """

    digests = []
    for name in sorted(names):
        skill_dir = skills_root / name
        manifest_source = skill_dir / "SKILL.md"
        if not manifest_source.exists():
            raise ValidationError(f"claude skill not found: {name}")
        manifest_digest = write_managed_file(
            home, f"plugins/{name}/.claude-plugin/plugin.json", json.dumps({"name": name}, sort_keys=True)
        )
        children = []
        for child in sorted(skill_dir.iterdir()):
            create_symlink_bridge(home, f"plugins/{name}/skills/{name}/{child.name}", child)
            children.append(child.name)
        digests.append(f"{name}:{manifest_digest}:{','.join(children)}")
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
        mcp_servers: Mapping[str, McpConfig],
        skills_root: Path | None = None,
    ) -> str:
        """Render settings, strict MCP config, and plugin dirs into ``home``.

        ``mcp_servers`` is the caller's resolution of the selected MCP names
        to their full definitions, required per the frozen adapter contract.
        A configured but unresolved MCP name fails closed rather than
        emitting a non-functional entry.
        """

        settings_digest = _render_settings(home, config.hooks)
        mcp_digest = _render_mcp_config(home, config.mcp, mcp_servers)
        if skills_root is None and not config.skills:
            skills_root = Path(home)
        if not isinstance(skills_root, Path) or not skills_root.is_absolute():
            raise ValidationError("claude skills_root must be absolute")
        plugin_digest = _render_plugin_dirs(home, skills_root, config.skills)
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
        return _agent_rate_limit_samples(Path(home), time.time())

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

        base_tools = _READ_TOOLS + (_WRITE_TOOLS if allow_write else ())
        write_scope = tuple(f"{tool}({request.workdir}/**)" for tool in _WRITE_TOOLS) if allow_write else ()
        allowed_tools = _READ_TOOLS + write_scope + tuple(f"mcp__{name}" for name in config.mcp)
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
            "--no-session-persistence",
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
        argv += ["--tools", ",".join(base_tools)]
        argv += ["--allowedTools", ",".join(allowed_tools)]
        argv += ["--disallowedTools", ",".join(_ALWAYS_DISALLOWED)]
        if request.effort is not None:
            argv += ["--effort", request.effort]
        argv += ["--append-system-prompt", "\n\n".join(system_prompt_parts)]
        argv += ["--session-id", session_id]

        environment: dict[str, str] = {"HOME": str(home)}
        path_value = os.environ.get("PATH")
        if path_value:
            environment["PATH"] = path_value

        auth_names: tuple[str, ...] = ()
        if config.auth is not None:
            auth_names = config.auth.names
            if not any(os.environ.get(name) for name in auth_names):
                raise ValidationError(
                    "claude runtime auth requires one of the declared environment "
                    f"variables to be set: {', '.join(auth_names)}"
                )
            for name in auth_names:
                value = os.environ.get(name)
                if value:
                    environment[name] = value

        mcp_env_names: list[str] = []
        for name in config.mcp:
            server = mcp_servers.get(name)
            if server is None:
                raise ValidationError(f"no resolved MCP definition for runtimes.claude.mcp entry: {name}")
            for env_name in server.env_from:
                value = os.environ.get(env_name)
                if not value:
                    raise ValidationError(
                        f"claude mcp {name!r} requires environment variable {env_name}, which is not set"
                    )
                environment[env_name] = value
                mcp_env_names.append(env_name)

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
                    "secret_env_names": tuple(dict.fromkeys((*auth_names, *mcp_env_names))),
                }
            ),
            answer_path=agent_dir / "answer.md",
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
        try:
            return ClaudeSession(process, plan, sink)
        except BaseException:
            _abort_launch(process)
            raise


def _abort_launch(process: subprocess.Popen) -> None:
    """Native-cancel, reap, and close pipes for a child that never got a session.

    Runs when opening the runtime log, wiring the reader thread, or writing
    the initial prompt fails during ``ClaudeSession`` construction, so the
    child never outlives a failed ``launch``.
    """

    if process.poll() is None:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except OSError:
            try:
                process.kill()
            except OSError:
                pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    for pipe in (process.stdin, process.stdout):
        if pipe is not None:
            try:
                pipe.close()
            except OSError:
                pass


def _open_runtime_log(path: Path):
    """Open the runtime stream log privately: O_CREAT|O_APPEND|O_WRONLY, mode 0600."""

    descriptor = os.open(str(path), os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    os.fchmod(descriptor, 0o600)
    return os.fdopen(descriptor, "a", encoding="utf-8")


def _known_secrets(plan: LaunchPlan) -> frozenset[str]:
    """Literal secret values that must never reach the decoder or the disk log."""

    names = plan.adapter_state.get("secret_env_names", ())
    return frozenset(value for name in names if (value := plan.environment.get(name)))


class ClaudeSession:
    """A launched Claude Code child process and its stream reader thread.

    Owns the child from construction until ``wait`` returns; any failure
    while wiring up the log, the reader thread, or the initial prompt
    propagates out of ``__init__`` so ``launch`` can native-cancel the
    process group instead of leaking a running child.
    """

    def __init__(self, process: subprocess.Popen, plan: LaunchPlan, sink: EventSink) -> None:
        self._process = process
        self._plan = plan
        self._sink = sink
        self._decoder = StreamDecoder()
        self._lock = threading.Lock()
        self._cancelled = False
        self._reader_error: BaseException | None = None
        # Set the instant a terminal ``result`` line is decoded, or when the
        # reader loop ends for any other reason (crash, EOF). ``wait`` blocks
        # on this instead of on OS process exit: the real engine holds stdin
        # open for another turn after answering, which in agent-run's
        # one-shot task model never comes, so process exit is not a signal
        # we can wait on.
        self._settled = threading.Event()
        # Set when ``wait`` had to end a still-alive child itself (rather
        # than the child exiting on its own): the resulting exit code (a
        # signal-terminated process rarely reports 0) must not then flip an
        # otherwise-successful engine result to failed.
        self._force_stopped = False
        self._secrets = _known_secrets(plan)
        self._raw_stream = _open_runtime_log(plan.runtime_stream_path)
        try:
            if plan.initial_input and process.stdin is not None:
                try:
                    process.stdin.write(plan.initial_input)
                    process.stdin.flush()
                except (BrokenPipeError, OSError):
                    pass
            self._reader = threading.Thread(target=self._read_stdout, daemon=True)
            self._reader.start()
        except BaseException:
            self._raw_stream.close()
            raise

    @property
    def pid(self) -> int | None:
        return self._process.pid

    @property
    def owns_process_group(self) -> bool:
        return True

    def _read_stdout(self) -> None:
        try:
            stdout = self._process.stdout
            if stdout is None:
                return
            for raw_line in stdout:
                try:
                    sanitized = sanitize_line(raw_line, self._secrets)
                    with self._lock:
                        self._raw_stream.write(sanitized if sanitized.endswith("\n") else sanitized + "\n")
                        self._raw_stream.flush()
                    result = self._decoder.feed(sanitized, at=time.time())
                    if result.session_id:
                        self._sink.session(result.session_id)
                    for message in result.messages:
                        self._sink.message(message)
                    if result.event:
                        self._sink.event(*result.event)
                    if result.warning:
                        # Best-effort: a bookkeeping write here must never mask
                        # the real outcome already captured in ``result``/self._decoder.
                        with contextlib.suppress(Exception):
                            self._sink.event("stream_diagnostic", {"reason": result.warning})
                    if result.terminal:
                        self._sink.event("runtime_result", terminal_event_data(result.terminal))
                        # First result/success (or error) wins: settle now
                        # instead of waiting for the child to exit on its own.
                        self._settled.set()
                except BaseException as error:  # persisted for wait(); keep draining the pipe
                    if self._reader_error is None:
                        self._reader_error = error
        finally:
            # Covers the crash/EOF case too: the child exited (or the pipe
            # closed) without ever producing a terminal line, so ``wait``
            # must still unblock and fall back to ``finalize()``.
            self._settled.set()

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

    def _stop_process(self) -> None:
        """End a child that is still alive after its answer already arrived.

        The real engine holds stdin open for another turn in this
        stream-json session; agent-run's one-shot task model has nothing
        more to send, so leaving it running only strands the process group
        and leaves room for a spurious duplicate cycle (observed live: a
        second system/init-to-result cycle on the same session, long after
        the first result/success). Idempotent settling in the decoder means
        such a duplicate is discarded even if this loses the race, but
        closing stdin and signaling promptly avoids relying on that.
        """

        self._force_stopped = True
        stdin = self._process.stdin
        if stdin is not None:
            with contextlib.suppress(OSError, ValueError):
                stdin.close()
        with contextlib.suppress(OSError):
            self._process.send_signal(signal.SIGINT)
        try:
            self._process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(OSError):
                os.killpg(self._process.pid, signal.SIGKILL)
            with contextlib.suppress(subprocess.TimeoutExpired):
                self._process.wait(timeout=5)

    def wait(self, timeout_seconds: float | None) -> Outcome | None:
        # Settle on stream content, not process exit: the child may keep
        # running (or spontaneously start a second turn) after its first
        # result/success line arrives, per the live evidence above.
        if not self._settled.wait(timeout=timeout_seconds):
            return None
        try:
            # Brief natural-exit grace: a process that answered and is
            # already finishing on its own should not be signalled.
            exit_code = self._process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            self._stop_process()
            try:
                exit_code = self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                return None
        self._reader.join(timeout=5 if timeout_seconds is None else timeout_seconds)
        if self._reader.is_alive():
            return None
        with self._lock:
            self._raw_stream.close()
        for pipe in (self._process.stdin, self._process.stdout):
            if pipe is not None:
                try:
                    pipe.close()
                except OSError:
                    pass
        if self._reader_error is not None:
            raise self._reader_error
        metadata = self._decoder.finalize()
        # A signal-terminated exit code from ``_stop_process`` reflects how
        # agent-run ended an already-answered child, not whether the engine
        # itself succeeded; only a naturally-exited child must report 0.
        exit_ok = exit_code == 0 or self._force_stopped
        succeeded = (
            exit_ok
            and not metadata.is_error
            and metadata.subtype != "no_answer"
            and bool(metadata.result_text)
        )
        if self._cancelled:
            status = AgentStatus.CANCELLED
        elif succeeded:
            status = AgentStatus.SUCCEEDED
        else:
            status = AgentStatus.FAILED
        if status == AgentStatus.SUCCEEDED:
            failure_kind = None
            failure_text = None
        else:
            empty_result = (
                exit_ok
                and not metadata.is_error
                and metadata.subtype != "no_answer"
                and not metadata.result_text
            )
            failure_kind = "empty_result" if empty_result else (metadata.subtype or "error")
            failure_text = metadata.result_text
        answer_path = None
        answer_bytes = None
        answer_sha256 = None
        if status is AgentStatus.SUCCEEDED:
            answer_path = self._plan.answer_path or self._plan.runtime_stream_path.with_name(
                "answer.md"
            )
            answer_bytes, answer_sha256 = seal_answer(
                answer_path, metadata.result_text or ""
            )
        return Outcome(
            status=status,
            exit_code=exit_code,
            failure_kind=failure_kind,
            failure_text=failure_text,
            runtime_session_id=metadata.runtime_session_id,
            answer_path=answer_path,
            answer_bytes=answer_bytes,
            answer_sha256=answer_sha256,
        )


ADAPTER = ClaudeAdapter()
