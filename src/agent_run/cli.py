"""Thin, JSON-only command line interface for :mod:`agent_run`."""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import sys
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import TextIO

from .capacity.collect import collect_once
from .capacity.launchd import argv as launchd_argv
from .capacity.launchd import build_configured_job, render_plist
from .config import load_config
from .delivery.codex_queue import TRANSPORT_NAME, CodexQueueSender, CodexQueueTransport
from .delivery.dispatch import DeliveryDispatcher
from .doctor import run_doctor
from .domain import AgentId, OrchestratorRef, StartRequest
from .errors import AgentRunError, ValidationError
from .hooks.bind import ref_from_payload, run_hook
from .hooks.context import build_context
from .launch import launch_detached
from .paths import agent_run_home, config_path, state_db_path
from .service import AgentQuery, AgentService
from .state import StateStore, reconcile_active_agents, reconcile_reaped_agent

_MAX_STDIN_CHARS = 1_048_576
_EXPECTED_ERROR_EXIT = 2
_QUEUE_TIMEOUT_SECONDS = 30.0
_POST_TERMINAL_TIMEOUT_SECONDS = 31.0
_CAPACITY_LAUNCHD_LABEL = "com.pluto.agent-run.capacity"
#: The only runtime that owns a managed service; there is no generic daemon.
_SERVICE_RUNTIME = "opencode"


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValidationError(message)


def _session(parser: argparse.ArgumentParser, *, required: bool = False) -> None:
    parser.add_argument("--session-transport", required=required)
    parser.add_argument("--session-id", required=required)
    parser.add_argument("--session-turn-id")


def _parser() -> argparse.ArgumentParser:
    parser = _Parser(prog="agent-run")
    parser.add_argument("--home", default=None)
    commands = parser.add_subparsers(dest="command", required=True)

    start = commands.add_parser("start")
    start.add_argument("--runtime", required=True)
    start.add_argument("--model", required=True)
    start.add_argument("--profile", required=True)
    start.add_argument("--task", required=True)
    start.add_argument("--workdir", default=os.getcwd())
    start.add_argument("--write", action="store_true")
    start.add_argument("--effort")
    start.add_argument("--timeout", type=float)
    start.add_argument("--read-root", action="append", default=[])
    start.add_argument("--output-schema")
    start.add_argument("--request-id")
    _session(start)

    bind = commands.add_parser("bind")
    bind.add_argument("agent_id")
    _session(bind, required=True)

    for name in ("cancel", "status", "answer"):
        command = commands.add_parser(name)
        command.add_argument("agent_id")

    steer = commands.add_parser("steer")
    steer.add_argument("agent_id")
    steer.add_argument("--text", required=True)

    agents = commands.add_parser("agents")
    agents.add_argument("--active", action="store_true")
    agents.add_argument("--offset", type=int, default=0)
    agents.add_argument("--limit", type=int, default=100)
    _session(agents)

    summary = commands.add_parser("summary")
    summary.add_argument("--agent-id")
    _session(summary)

    transcript = commands.add_parser("transcript")
    transcript.add_argument("agent_id")
    transcript.add_argument("--cursor", type=int, default=0)
    transcript.add_argument("--limit", type=int, default=200)
    transcript_mode = transcript.add_mutually_exclusive_group()
    transcript_mode.add_argument("--follow", action="store_true")
    transcript_mode.add_argument("--full", action="store_true")

    commands.add_parser("models")
    commands.add_parser("limits")

    context = commands.add_parser("context")
    _session(context, required=True)

    capacity = commands.add_parser("capacity").add_subparsers(
        dest="capacity_command", required=True
    )
    collect = capacity.add_parser("collect")
    collect.add_argument("--once", action="store_true", required=True)
    launchd = capacity.add_parser("launchd")
    launchd.add_argument("--binary", required=True)
    launchd.add_argument("--label", default=_CAPACITY_LAUNCHD_LABEL)
    launchd.add_argument("--stdout-log", default="/dev/null")
    launchd.add_argument("--stderr-log")

    delivery = commands.add_parser("delivery").add_subparsers(
        dest="delivery_command", required=True
    )
    delivery_status = delivery.add_parser("status")
    delivery_status.add_argument("agent_id")
    delivery_cancel = delivery.add_parser("cancel")
    delivery_cancel.add_argument("delivery_id")
    delivery.add_parser("dispatch")
    delivery_launchd = delivery.add_parser("launchd")
    delivery_launchd.add_argument("--binary", required=True)
    delivery_launchd.add_argument(
        "--label", default="com.pluto.agent-run.delivery"
    )
    delivery_launchd.add_argument("--stdout-log", default="/dev/null")
    delivery_launchd.add_argument("--stderr-log")

    runtime_service = commands.add_parser("service").add_subparsers(
        dest="service_command", required=True
    )
    service_start = runtime_service.add_parser("start")
    service_start.add_argument("--runtime", required=True)
    service_start.add_argument("--port", type=int)

    hook = commands.add_parser("hook").add_subparsers(
        dest="hook_command", required=True
    )
    hook.add_parser("context")
    hook.add_parser("bind")

    commands.add_parser("init")
    commands.add_parser("doctor")
    commands.add_parser("mcp")
    return parser


def _read(stream: TextIO) -> str:
    value = stream.read(_MAX_STDIN_CHARS + 1)
    if len(value) > _MAX_STDIN_CHARS:
        raise ValidationError("stdin exceeds the 1048576-character limit")
    return value


def _text(value: str, stream: TextIO, what: str) -> str:
    result = _read(stream) if value == "-" else value
    if not result.strip():
        raise ValidationError(f"{what} must be nonblank")
    return result


def _object(value: str, what: str) -> dict:
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as error:
        raise ValidationError(f"{what} must be valid JSON") from error
    if not isinstance(decoded, dict):
        raise ValidationError(f"{what} must be a JSON object")
    return decoded


def _payload(stream: TextIO) -> dict:
    return _object(_read(stream), "hook payload")


def _hook_payload(payload: dict, *, bind: bool) -> dict:
    if "session_id" not in payload:
        return payload
    expected_event = "PostToolUse" if bind else "UserPromptSubmit"
    event = payload.get("hook_event_name")
    if event is not None and event != expected_event:
        raise ValidationError(f"raw hook_event_name must be {expected_event}")
    session_id = payload.get("session_id")
    if not isinstance(session_id, str) or not session_id.strip():
        raise ValidationError("raw hook session_id must be nonblank")
    normalized = {
        "transport": TRANSPORT_NAME,
        "external_session_id": session_id,
    }
    if "turn_id" in payload:
        normalized["external_turn_id"] = payload["turn_id"]
    if not bind:
        return normalized

    agent_ids: set[str] = set()
    pending = [payload.get("tool_response")]
    while pending:
        value = pending.pop()
        if isinstance(value, Mapping):
            for key, item in value.items():
                if key == "agent_id":
                    if not isinstance(item, str):
                        raise ValidationError("raw PostToolUse agent_id must be a string")
                    agent_ids.add(item)
                else:
                    pending.append(item)
        elif isinstance(value, list):
            pending.extend(value)
    if not agent_ids:
        raise ValidationError("raw PostToolUse payload has no agent_id")
    if len(agent_ids) != 1:
        raise ValidationError("raw PostToolUse payload has conflicting agent_id values")
    normalized["agent_id"] = agent_ids.pop()
    return normalized


def _ref(args: argparse.Namespace, *, required: bool = False) -> OrchestratorRef | None:
    transport = getattr(args, "session_transport", None)
    session_id = getattr(args, "session_id", None)
    turn_id = getattr(args, "session_turn_id", None)
    if transport is None and session_id is None and turn_id is None:
        if required:
            raise ValidationError("session transport and id are required")
        return None
    if not transport or not session_id:
        raise ValidationError("session transport and id must be supplied together")
    return OrchestratorRef(transport, session_id, turn_id)


def _request(args: argparse.Namespace, stream: TextIO) -> StartRequest:
    schema = None if args.output_schema is None else _object(args.output_schema, "output schema")
    timeout = {} if args.timeout is None else {"timeout_seconds": args.timeout}
    return StartRequest(
        args.runtime,
        args.model,
        args.profile,
        _text(args.task, stream, "task"),
        Path(args.workdir),
        write=args.write,
        effort=args.effort,
        **timeout,
        read_roots=tuple(Path(root) for root in args.read_root),
        output_schema=schema,
        orchestrator=_ref(args),
        request_id=args.request_id,
    )


def _full_transcript(service, args: argparse.Namespace):
    cursor = args.cursor
    messages = []
    pages = 0
    while True:
        page = service.transcript(args.agent_id, cursor=cursor, limit=args.limit)
        pages += 1
        messages.extend(page.messages)
        if page.complete:
            return {
                "agent_id": page.agent_id,
                "messages": messages,
                "cursor": args.cursor,
                "next_cursor": None,
                "complete": True,
                "pages": pages,
            }
        if page.next_cursor is None or page.next_cursor <= cursor:
            raise AgentRunError("transcript pagination did not advance")
        cursor = page.next_cursor


def _follow_transcript(service, args: argparse.Namespace):
    import time

    from .domain import TERMINAL

    cursor = args.cursor
    messages = []
    pages = 0
    while True:
        page = service.transcript(args.agent_id, cursor=cursor, limit=args.limit)
        pages += 1
        if page.messages:
            next_cursor = page.messages[-1].seq
            if next_cursor <= cursor:
                raise AgentRunError("transcript pagination did not advance")
            cursor = next_cursor
            messages.extend(page.messages)
        elif not page.complete:
            raise AgentRunError("transcript pagination did not advance")
        if not page.complete:
            continue
        if service.get(args.agent_id).status in TERMINAL:
            return {
                "agent_id": page.agent_id,
                "messages": messages,
                "cursor": args.cursor,
                "next_cursor": None,
                "complete": True,
                "pages": pages,
            }
        time.sleep(0.25)


def _execute(args: argparse.Namespace, service, stream: TextIO):
    command = args.command
    if command == "start":
        result = service.start(_request(args, stream))
        return {"agent_id": result.agent_id, "created": result.created}
    if command == "bind":
        return service.bind(args.agent_id, _ref(args, required=True))
    if command == "cancel":
        return service.cancel(args.agent_id)
    if command == "steer":
        return service.steer(args.agent_id, _text(args.text, stream, "steer text"))
    if command == "status":
        return service.get(args.agent_id)
    if command == "agents":
        return service.list(
            AgentQuery(args.active, _ref(args), args.offset, args.limit)
        )
    if command == "summary":
        return service.summary(agent_id=args.agent_id, orchestrator=_ref(args))
    if command == "transcript":
        return (
            _follow_transcript(service, args)
            if args.follow
            else _full_transcript(service, args)
            if args.full
            else service.transcript(args.agent_id, args.cursor, args.limit)
        )
    if command == "answer":
        return service.answer(args.agent_id)
    if command == "models":
        return service.models()
    if command == "limits":
        return service.limits()
    if command == "context":
        return service.context(_ref(args, required=True))
    if command == "capacity":
        return service.capacity_collect()
    if command == "delivery":
        if args.delivery_command == "status":
            return service.delivery_status(args.agent_id)
        if args.delivery_command == "cancel":
            return service.delivery_cancel(args.delivery_id)
        return service.delivery_dispatch()
    if command == "hook":
        payload = _payload(stream)
        return (
            service.hook_context(payload)
            if args.hook_command == "context"
            else service.hook_bind(payload)
        )
    if command == "init":
        return service.init()
    if command == "doctor":
        return service.doctor()
    raise AgentRunError(f"unsupported command: {command}")


def _jsonable(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Enum):
        return _jsonable(value.value)
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: _jsonable(getattr(value, field.name)) for field in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list, set, frozenset)):
        return [_jsonable(item) for item in value]
    raise TypeError(f"cannot serialize {type(value).__name__}")


def _emit(value, stream: TextIO) -> None:
    json.dump(
        _jsonable(value), stream, allow_nan=False, ensure_ascii=False, sort_keys=True
    )
    stream.write("\n")


def _capacity_launchd(home: Path, args: argparse.Namespace) -> dict[str, object]:
    config = load_config(config_path(home))
    job = build_configured_job(
        config.capacity,
        args.label,
        Path(args.binary),
        stdout_log=Path(args.stdout_log),
        stderr_log=(
            home / "capacity-worker.err.log"
            if args.stderr_log is None
            else Path(args.stderr_log)
        ),
    )
    return {
        "label": job.label,
        "interval_seconds": job.interval_seconds,
        "argv": launchd_argv(job),
        "plist": render_plist(job),
    }


def _delivery_launchd(home: Path, args: argparse.Namespace) -> dict[str, object]:
    from .delivery.launchd import argv, build_configured_job, render_plist

    config = load_config(config_path(home))
    job = build_configured_job(
        config.delivery,
        args.label,
        Path(args.binary),
        home,
        stdout_log=Path(args.stdout_log),
        stderr_log=(
            home / "delivery-worker.err.log"
            if args.stderr_log is None
            else Path(args.stderr_log)
        ),
    )
    return {
        "label": job.label,
        "interval_seconds": job.interval_seconds,
        "argv": argv(job),
        "plist": render_plist(job),
    }


def _service_start(home: Path, args: argparse.Namespace) -> dict[str, object]:
    """Start (or reuse) the one managed service of a runtime that owns one."""

    from .adapters.opencode.service import start_service

    if args.runtime != _SERVICE_RUNTIME:
        raise ValidationError(
            f"service start supports --runtime {_SERVICE_RUNTIME} only, not {args.runtime!r}"
        )
    runtime = load_config(config_path(home)).runtimes.get(_SERVICE_RUNTIME)
    if runtime is None or not runtime.enabled:
        raise ValidationError(f"runtime is not configured or not enabled: {_SERVICE_RUNTIME}")
    started = start_service(runtime, runtime.home, port=args.port)
    return {
        "runtime": _SERVICE_RUNTIME,
        "reused": started.reused,
        "service": started.descriptor.as_dict(),
    }


def _dispatch_once(home: Path):
    config = load_config(config_path(home))
    executable = (
        os.environ["CODEX_QUEUE_BIN"]
        if "CODEX_QUEUE_BIN" in os.environ
        else config.delivery.codex_queue_bin
    )
    if executable is None or not str(executable) or not Path(executable).is_absolute():
        raise ValidationError(
            "delivery.codex_queue_bin or CODEX_QUEUE_BIN must name an absolute executable"
        )
    store = StateStore.open(state_db_path(home))
    try:
        if isinstance(store, StateStore):
            reconcile_active_agents(store)
        sender = CodexQueueSender(str(executable), timeout_seconds=_QUEUE_TIMEOUT_SECONDS)
        dispatcher = DeliveryDispatcher(
            store,
            {TRANSPORT_NAME: CodexQueueTransport(sender)},
            config.delivery,
        )
        return dispatcher.run(home=home)
    finally:
        store.close()


def _launch_callback(home: Path):
    def launch(
        agent_id: AgentId,
        request: StartRequest,
        _adapter,
        plan,
        candidate_dir: Path,
    ) -> None:
        def post_reap(pid: int, _wait_status: int) -> None:
            store = StateStore.open(state_db_path(home))
            try:
                reconcile_reaped_agent(store, agent_id, pid)
            finally:
                store.close()

        # The exec'd supervisor reloads config and adapter itself; only the plan
        # travels, because its environment carries live secrets.
        launch_detached(
            {
                "agent_id": str(agent_id),
                "home": str(home),
                "runtime": request.runtime,
                "timeout_seconds": request.timeout_seconds,
                "answer_path": str(candidate_dir / "answer.md"),
                "agent_dir": str(candidate_dir),
                "warning_fraction": load_config(config_path(home)).core.warning_fraction,
                "plan": plan.to_payload(),
            },
            executable=sys.executable,
            post_terminal_timeout_seconds=_POST_TERMINAL_TIMEOUT_SECONDS,
            post_reap=post_reap,
        )

    return launch


class _Runtime:
    def __init__(self, home: Path) -> None:
        self.home = home
        self.core = AgentService.from_home(home, launch=_launch_callback(home))

    def __getattr__(self, name: str):
        return getattr(self.core, name)

    def close(self) -> None:
        self.core.close()

    def _inputs(self):
        return load_config(config_path(self.home)), StateStore.open(
            state_db_path(self.home)
        )

    def context(self, ref: OrchestratorRef):
        config, store = self._inputs()
        try:
            return build_context(store, ref, config=config)
        finally:
            store.close()

    def capacity_collect(self):
        config, store = self._inputs()
        try:
            return collect_once(store, config)
        finally:
            store.close()

    def delivery_status(self, agent_id: str):
        return self.core.get(agent_id).delivery

    def delivery_cancel(self, delivery_id: str):
        _config, store = self._inputs()
        try:
            return {"delivery_id": delivery_id, "cancelled": store.cancel_delivery(delivery_id)}
        finally:
            store.close()

    def delivery_dispatch(self):
        return _dispatch_once(self.home)

    def hook_context(self, payload: dict):
        config, store = self._inputs()
        try:
            result = build_context(
                store, ref_from_payload(_hook_payload(payload, bind=False)), config=config
            )
            if result.injected and result.text.strip():
                return {
                    "hookSpecificOutput": {
                        "hookEventName": "UserPromptSubmit",
                        "additionalContext": result.text,
                    }
                }
            return {}
        finally:
            store.close()

    def hook_bind(self, payload: dict):
        _config, store = self._inputs()
        try:
            result = run_hook(store, _hook_payload(payload, bind=True))
            return {
                "hookSpecificOutput": {
                    "hookEventName": "PostToolUse",
                    "additionalContext": result.message(),
                }
            }
        finally:
            store.close()


def _initialize(home: Path):
    created = not home.exists()
    if not created and not home.is_dir():
        raise ValidationError("agent-run home must be a directory")
    home.mkdir(mode=0o700, parents=True, exist_ok=True)
    if created:
        home.chmod(0o700)
    path = config_path(home)
    if path.is_symlink():
        raise ValidationError("config.toml must not be a symlink")
    if not path.exists():
        try:
            descriptor = os.open(
                path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
            )
        except FileExistsError:
            pass
        else:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                stream.write("schema_version = 1\n")
                stream.flush()
                os.fsync(stream.fileno())
    load_config(path)
    store = StateStore.initialize(state_db_path(home))
    store.close()
    return {"home": home, "config": path, "state": state_db_path(home)}


def _doctor(home: Path):
    return run_doctor(home)


def main(
    argv: list[str] | None = None,
    *,
    service=None,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    stdin = sys.stdin if stdin is None else stdin
    stdout = sys.stdout if stdout is None else stdout
    stderr = sys.stderr if stderr is None else stderr
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            args = _parser().parse_args(argv)
    except SystemExit as error:
        return int(error.code)
    except AgentRunError as error:
        _emit({"error": {"type": type(error).__name__, "message": str(error)}}, stderr)
        return _EXPECTED_ERROR_EXIT
    owned: _Runtime | None = None
    try:
        home = agent_run_home(args.home)
        if service is None and args.command == "init":
            result = _initialize(home)
        elif service is None and args.command == "doctor":
            result = _doctor(home)
        elif args.command == "capacity" and args.capacity_command == "launchd":
            result = _capacity_launchd(home, args)
        elif args.command == "delivery" and args.delivery_command == "launchd":
            result = _delivery_launchd(home, args)
        elif args.command == "service":
            result = _service_start(home, args)
        else:
            if service is None:
                owned = _Runtime(home)
                target = owned
            else:
                target = service
            if args.command == "mcp":
                from .mcp import serve

                returned = serve(
                    owned.core if owned is not None else target,
                    stdin=stdin,
                    stdout=stdout,
                )
                return returned if isinstance(returned, int) else 0
            result = _execute(args, target, stdin)
        _emit(result, stdout)
        return 0
    except AgentRunError as error:
        _emit({"error": {"type": type(error).__name__, "message": str(error)}}, stderr)
        return _EXPECTED_ERROR_EXIT
    finally:
        if owned is not None:
            owned.close()


if __name__ == "__main__":
    raise SystemExit(main())
