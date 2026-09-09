"""Thin, JSON-only command line interface for :mod:`agent_run`."""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import subprocess
import sys
import time
from collections.abc import Mapping
from dataclasses import fields, is_dataclass, replace
from enum import Enum
from pathlib import Path
from typing import TextIO

from .accounts import account_runtime_home, account_store_dir
from .adapters.claude.auth import claude_login_environment
from .api_launchd import argv as api_launchd_argv
from .api_launchd import build_job as build_api_launchd_job
from .api_launchd import render_plist as render_api_launchd_plist
from .broker_client import BrokerClient
from .capacity.collect import collect_once
from .capacity.launchd import argv as launchd_argv
from .capacity.launchd import build_configured_job, render_plist
from .config import RuntimeConfig, load_config
from .doctor import run_doctor
from .domain import AgentId, OrchestratorRef, StartRequest
from .errors import AgentRunError, ValidationError
from .launch import ChildReaper, launch_detached
from .launch_evidence import bootstrap_error_fields
from .logging_setup import configure_logging
from .paths import agent_run_home, config_path, state_db_path
from .preparation import request_payload
from .role_plan import ResolvedRolePlan
from .service import AgentQuery, AgentService
from .state import StateStore, reconcile_reaped_agent
from .state.run_stats import backfill_run_stats
from .wait import DEFAULT_POLL_SECONDS, wait_for_agent

_logger = logging.getLogger("agent_run.cli")

_MAX_STDIN_CHARS = 1_048_576
_EXPECTED_ERROR_EXIT = 2
_POST_TERMINAL_TIMEOUT_SECONDS = 31.0
_API_LAUNCHD_LABEL = "com.agent-run.api"
_CAPACITY_LAUNCHD_LABEL = "com.pluto.agent-run.capacity"


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ValidationError(message)


def _session(parser: argparse.ArgumentParser, *, required: bool = False) -> None:
    parser.add_argument("--session-transport", required=required)
    parser.add_argument("--session-id", required=required)
    parser.add_argument("--session-turn-id")


def _wait_options(parser: argparse.ArgumentParser) -> None:
    """Add the shared ``wait`` polling options to a subcommand parser.

    ``--timeout`` is the watcher budget in seconds, where ``0`` (the default)
    waits forever because the run's own ``timeout_seconds`` bounds it;
    ``--poll`` is the seconds between polls, defaulting to the wait module's
    ``DEFAULT_POLL_SECONDS``.
    """

    parser.add_argument("--timeout", type=float, default=0.0)
    parser.add_argument("--poll", type=float, default=DEFAULT_POLL_SECONDS)


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
    start.add_argument("--fast", action="store_true")
    start.add_argument("--effort")
    start.add_argument("--timeout", type=float)
    start.add_argument("--read-root", action="append", default=[])
    start.add_argument("--output-schema")
    start.add_argument("--request-id")
    start.add_argument("--account")
    _session(start)

    resume = commands.add_parser("resume")
    resume.add_argument("agent_id")
    task = resume.add_mutually_exclusive_group(required=True)
    task.add_argument("--task")
    task.add_argument("--task-file")
    resume.add_argument("--timeout", type=float)
    resume.add_argument("--request-id")
    _session(resume)

    auth = commands.add_parser("auth")
    auth.add_argument("label")
    auth.add_argument("runtime")

    login = commands.add_parser("login")
    login.add_argument("runtime")
    login.add_argument("--account")

    for name in ("cancel", "answer"):
        command = commands.add_parser(name)
        command.add_argument("agent_id")

    agent_wait = commands.add_parser("wait")
    agent_wait.add_argument("agent_id")
    _wait_options(agent_wait)

    steer = commands.add_parser("steer")
    steer.add_argument("agent_id")
    steer.add_argument("--text", required=True)

    agents = commands.add_parser("agents")
    agents.add_argument("--active", action="store_true")
    agents.add_argument("--offset", type=int, default=0)
    agents.add_argument("--limit", type=int, default=100)
    _session(agents)

    transcript = commands.add_parser("transcript")
    transcript.add_argument("agent_id")
    transcript.add_argument("--cursor", type=int, default=0)
    transcript.add_argument("--limit", type=int, default=200)
    transcript_mode = transcript.add_mutually_exclusive_group()
    transcript_mode.add_argument("--follow", action="store_true")
    transcript_mode.add_argument("--full", action="store_true")

    stats = commands.add_parser("stats").add_subparsers(
        dest="stats_command", required=True
    )
    stats.add_parser("backfill")

    capacity = commands.add_parser("capacity").add_subparsers(
        dest="capacity_command", required=True
    )
    collect = capacity.add_parser("collect")
    collect.add_argument("--once", action="store_true", required=True)
    capacity.add_parser(
        "order",
        help=(
            "List capacity priority (first route is highest); the orchestrator "
            "still chooses a compatible role/model alias and does not launch work."
        ),
    )
    launchd = capacity.add_parser("launchd")
    launchd.add_argument("--binary", required=True)
    launchd.add_argument("--label", default=_CAPACITY_LAUNCHD_LABEL)
    launchd.add_argument("--stdout-log", default="/dev/null")
    launchd.add_argument("--stderr-log")

    commands.add_parser("init")
    commands.add_parser("doctor")
    commands.add_parser("mcp")
    api = commands.add_parser("api").add_subparsers(
        dest="api_command", required=True
    )
    api_serve = api.add_parser("serve")
    api_serve.add_argument("--socket")
    api_launchd = api.add_parser("launchd")
    api_launchd.add_argument("--binary", required=True)
    api_launchd.add_argument("--label", default=_API_LAUNCHD_LABEL)
    api_launchd.add_argument("--stdout-log", default=None)
    api_launchd.add_argument("--stderr-log", default=None)
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
        fast=args.fast,
        account=args.account,
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
    """Dispatch one parsed CLI command to its service operation."""

    command = args.command
    if command == "resume":
        if args.task_file is None:
            task = _text(args.task, stream, "resume task")
        else:
            try:
                task = _read(stream) if args.task_file == "-" else Path(args.task_file).read_bytes().decode("utf-8")
            except (OSError, UnicodeError) as error:
                raise ValidationError(f"cannot read resume task file: {error}") from error
        result = service.resume(
            args.agent_id, task, timeout_seconds=args.timeout,
            request_id=args.request_id, orchestrator=_ref(args),
        )
        return {"agent_id": result.agent_id, "created": result.created}
    if command == "start":
        result = service.start(_request(args, stream))
        return {"agent_id": result.agent_id, "created": result.created}
    if command == "cancel":
        return service.cancel(args.agent_id)
    if command == "steer":
        return service.steer(args.agent_id, _text(args.text, stream, "steer text"))
    if command == "agents":
        return service.list(
            AgentQuery(args.active, _ref(args), args.offset, args.limit)
        )
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
    if command == "capacity":
        return (
            service.capacity_collect()
            if args.capacity_command == "collect"
            else service.capacity_order()
        )
    if command == "init":
        return service.init()
    if command == "doctor":
        return service.doctor()
    raise AgentRunError(f"unsupported command: {command}")


def _wait_command(
    args: argparse.Namespace, service, stdout: TextIO, stderr: TextIO
) -> int:
    """Run a blocking ``wait`` verb and return its status-coded exit.

    The polling loop lives in :mod:`agent_run.wait`; this only hands it the
    parsed arguments and owns the process-visible side effects: the terminal
    payload goes to stdout, and when the watcher gives up the current status
    payload is joined by a one-line note on stderr.
    """

    outcome = wait_for_agent(
        service, args.agent_id, timeout=args.timeout, poll=args.poll
    )
    _emit(outcome.payload, stdout)
    if outcome.note is not None:
        stderr.write(f"{outcome.note}\n")
    return outcome.exit_code


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


def _error_payload(error: AgentRunError) -> dict:
    """The standard error envelope, extended with a bootstrap failure's evidence."""

    return {
        "error": {
            "type": type(error).__name__,
            "message": str(error),
            **bootstrap_error_fields(error),
        }
    }


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


def _api_launchd(home: Path, args: argparse.Namespace) -> dict[str, object]:
    job = build_api_launchd_job(
        args.label,
        Path(args.binary),
        home,
        stdout_log=(
            home / "logs" / "api.log"
            if args.stdout_log is None
            else Path(args.stdout_log)
        ),
        stderr_log=(
            home / "logs" / "api.err.log"
            if args.stderr_log is None
            else Path(args.stderr_log)
        ),
    )
    return {
        "label": job.label,
        "argv": api_launchd_argv(job),
        "plist": render_api_launchd_plist(job),
    }


def _launch_callback(home: Path, *, child_reaper: ChildReaper | None = None):
    """Build the service launch callback for one home and reaping policy.

    CLI callers omit ``child_reaper`` and retain the per-child waiter. The
    resident socket daemon passes its shared reaper so completed supervisors do
    not become zombies while preserving exact post-reap reconciliation.
    """

    def launch(
        agent_id: AgentId,
        request: StartRequest,
        role: ResolvedRolePlan,
    ) -> None:
        def post_reap(pid: int, _wait_status: int) -> None:
            store = StateStore.open(state_db_path(home))
            try:
                reconcile_reaped_agent(store, agent_id, pid)
            finally:
                store.close()

        payload = {
            "agent_id": str(agent_id),
            "home": str(home),
            "request": request_payload(request),
            "role": role.to_payload(),
        }
        if child_reaper is None:
            launch_detached(
                payload,
                executable=sys.executable,
                post_terminal_timeout_seconds=_POST_TERMINAL_TIMEOUT_SECONDS,
                post_reap=post_reap,
            )
        else:
            launch_detached(
                payload,
                executable=sys.executable,
                post_terminal_timeout_seconds=_POST_TERMINAL_TIMEOUT_SECONDS,
                post_reap=post_reap,
                child_reaper=child_reaper,
            )

    return launch


class _Runtime:
    """Own one transport-facing service and its optional daemon child reaper."""

    def __init__(self, home: Path, *, child_reaper: ChildReaper | None = None) -> None:
        """Compose the service for ``home`` using the selected reaping policy."""

        self.home = home
        self.child_reaper = child_reaper
        self.core = AgentService.from_home(
            home, launch=_launch_callback(home, child_reaper=child_reaper)
        )

    def __getattr__(self, name: str):
        return getattr(self.core, name)

    def close(self) -> None:
        self.core.close()

    def _inputs(self):
        return load_config(config_path(self.home)), StateStore.open(
            state_db_path(self.home)
        )

    def capacity_collect(self):
        config, store = self._inputs()
        try:
            return collect_once(store, config, agent_run_home=self.home)
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


def _auth(home: Path, args: argparse.Namespace, stderr: TextIO) -> dict[str, object] | int:
    """Run the legacy labelled-login command for Codex or Claude.

    ``home`` supplies the active configuration, ``args.label`` must be a
    configured account of ``args.runtime``, and ``stderr`` receives only fixed
    failure diagnostics. Claude delegates to its scoped CLI login helper;
    Codex retains its existing account-store flow. Unknown/disabled runtimes,
    undeclared labels, and unsupported adapters raise ``ValidationError``.
    Provider login or status failures return their nonzero exit code without
    rendering provider output or credential values.

    :param home: Agent-run home containing ``config.toml``.
    :param args: Parsed ``auth <label> <runtime>`` command arguments.
    :param stderr: User-facing diagnostic stream.
    :returns: Secret-free success data or the provider CLI's nonzero exit code.
    :raises ValidationError: If the declared runtime/account cannot be used.
    """

    config = load_config(config_path(home))
    runtime = config.runtimes.get(args.runtime)
    if runtime is None or not runtime.enabled:
        raise ValidationError(f"runtime is not configured or not enabled: {args.runtime}")
    if args.label not in runtime.accounts:
        raise ValidationError(
            f"account {args.label!r} is not declared for runtime {args.runtime}"
        )
    if runtime.adapter == "agent_run.adapters.claude.adapter:ADAPTER":
        return _claude_login(runtime, args.label, stderr)
    if "adapters.codex" not in runtime.adapter:
        raise ValidationError(f"auth login not supported for runtime {args.runtime} yet")
    store = account_store_dir(home, args.runtime, args.label)
    store.mkdir(mode=0o700, parents=True, exist_ok=True)
    store.chmod(0o700)
    environment = {key: os.environ[key] for key in ("PATH", "HOME") if key in os.environ}
    environment["CODEX_HOME"] = str(store)
    login = subprocess.run([str(runtime.binary), "login"], env=environment)
    if login.returncode:
        stderr.write(f"auth login failed for {args.label} {args.runtime} (exit {login.returncode})\n")
        return login.returncode
    status = subprocess.run(
        [str(runtime.binary), "login", "status"],
        env=environment,
        capture_output=True,
        text=True,
    )
    if status.returncode:
        stderr.write(f"auth login status failed for {args.label} {args.runtime} (exit {status.returncode})\n")
        return status.returncode
    return {"account": args.label, "runtime": args.runtime, "status": "ok"}


def _claude_login(
    runtime: RuntimeConfig, label: str | None, stderr: TextIO
) -> dict[str, object] | int:
    """Run interactive Claude auth in the selected private account directory.

    ``runtime`` is an enabled Claude configuration and ``label`` is either a
    declared account name or ``None`` for its base scoped state. The real CLI
    receives only :func:`claude_login_environment` and is first allowed to run
    its interactive ``auth login`` flow; a successful exit is verified by its
    JSON status exit code without rendering status output. Invalid labels and
    nonzero provider exits return explicit safe errors without copying or
    inspecting credentials.

    :param RuntimeConfig runtime: Configured Claude runtime selected by the
        outer command.
    :param str | None label: Optional selected account label.
    :param TextIO stderr: User-facing diagnostic stream for safe fixed failure
        text.
    :returns dict[str, object] | int: Success data or the Claude CLI's nonzero
        exit code.
    """

    state_home = None if label is None else account_runtime_home(runtime.home, label)
    scoped_runtime = replace(runtime, credential_state_home=state_home)
    environment = claude_login_environment(scoped_runtime)
    login = subprocess.run([str(runtime.binary), "auth", "login"], env=environment)
    account = "default" if label is None else label
    if login.returncode:
        stderr.write(f"auth login failed for {account} claude (exit {login.returncode})\n")
        return login.returncode
    status = subprocess.run(
        [str(runtime.binary), "auth", "status", "--json"],
        env=environment,
        capture_output=True,
        text=True,
    )
    if status.returncode:
        stderr.write(f"auth login status failed for {account} claude (exit {status.returncode})\n")
        return status.returncode
    return {"account": label, "runtime": "claude", "status": "ok"}


def _login(home: Path, args: argparse.Namespace, stderr: TextIO) -> dict[str, object] | int:
    """Dispatch the convenience login syntax while preserving account isolation.

    ``args.runtime`` must name enabled Claude. An omitted account selects the
    native global CLI state; an explicit declared label selects isolated state.
    Other engines retain ``agent-run auth <label> <runtime>``.

    :param Path home: Agent-run home containing the active configuration.
    :param argparse.Namespace args: Parsed ``login`` command arguments.
    :param TextIO stderr: User-facing diagnostic stream for safe CLI failures.
    :returns dict[str, object] | int: Login success data or a nonzero Claude
        CLI exit code.
    :raises ValidationError: For unavailable runtime, unsupported syntax, or an
        undeclared/missing Claude account selection.
    """

    config = load_config(config_path(home))
    runtime = config.runtimes.get(args.runtime)
    if runtime is None or not runtime.enabled:
        raise ValidationError(f"runtime is not configured or not enabled: {args.runtime}")
    if runtime.adapter != "agent_run.adapters.claude.adapter:ADAPTER":
        raise ValidationError(
            f"login supports Claude only; use agent-run auth <label> {args.runtime}"
        )
    label = args.account
    if args.account is not None and args.account not in runtime.accounts:
        raise ValidationError(f"account {args.account!r} is not declared for runtime claude")
    return _claude_login(runtime, label, stderr)


def _doctor(home: Path):
    return run_doctor(home)


def _stats(home: Path, args: argparse.Namespace) -> dict[str, object]:
    if args.stats_command == "backfill":
        store = StateStore.open(state_db_path(home))
        try:
            return backfill_run_stats(store)
        finally:
            store.close()
    raise AgentRunError(f"unsupported stats command: {args.stats_command}")


def main(
    argv: list[str] | None = None,
    *,
    service=None,
    stdin: TextIO | None = None,
    stdout: TextIO | None = None,
    stderr: TextIO | None = None,
) -> int:
    """Parse and execute one CLI, MCP-proxy, or resident API invocation.

    A resident ``api serve`` invocation owns one shared ``ChildReaper`` for its
    full lifetime. A one-shot ``start`` without an injected service submits to
    that resident daemon so accepted asynchronous work outlives this process;
    other commands retain their local service behavior. Returns the process
    exit code and closes every client, service, or reaper it created.
    """

    stdin = sys.stdin if stdin is None else stdin
    stdout = sys.stdout if stdout is None else stdout
    stderr = sys.stderr if stderr is None else stderr
    try:
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            args = _parser().parse_args(argv)
    except SystemExit as error:
        return int(error.code)
    except AgentRunError as error:
        _emit(_error_payload(error), stderr)
        return _EXPECTED_ERROR_EXIT
    owned: _Runtime | None = None
    start_broker: BrokerClient | None = None
    child_reaper: ChildReaper | None = None
    started = time.monotonic()
    try:
        home = agent_run_home(args.home)
        configure_logging(home, "mcp" if args.command == "mcp" else "cli")
        _logger.info("cli command=%s", args.command)
        if service is None and args.command == "init":
            result = _initialize(home)
        elif service is None and args.command == "auth":
            result = _auth(home, args, stderr)
            if isinstance(result, int):
                return result
        elif service is None and args.command == "login":
            result = _login(home, args, stderr)
            if isinstance(result, int):
                return result
        elif service is None and args.command == "doctor":
            result = _doctor(home)
        elif service is None and args.command == "stats":
            result = _stats(home, args)
        elif args.command == "capacity" and args.capacity_command == "launchd":
            result = _capacity_launchd(home, args)
        elif args.command == "api" and args.api_command == "launchd":
            result = _api_launchd(home, args)
        else:
            if args.command == "mcp":
                from .mcp import serve

                mcp_broker = service if service is not None else BrokerClient(home / "api.sock")
                returned = serve(mcp_broker, stdin=stdin, stdout=stdout)
                close = getattr(mcp_broker, "close", None)
                if callable(close):
                    close()
                _logger.info(
                    "cli command=mcp outcome=ok duration_ms=%.1f",
                    (time.monotonic() - started) * 1000,
                )
                return returned if isinstance(returned, int) else 0
            if service is None:
                if args.command == "api":
                    child_reaper = ChildReaper()
                if args.command in {"start", "resume"}:
                    start_broker = BrokerClient(home / "api.sock")
                    target = start_broker
                else:
                    owned = _Runtime(home, child_reaper=child_reaper)
                    target = owned
            else:
                target = service
            if args.command == "api":
                from .api_socket import serve

                def _api_service() -> object:
                    # Runs on the dispatcher thread: the store's SQLite
                    # connection must be created where it will be used.
                    if owned is None:
                        return target
                    fresh = AgentService.from_home(
                        home, launch=_launch_callback(home, child_reaper=child_reaper)
                    )
                    fresh._registry.preload_enabled()
                    return fresh

                returned = serve(
                    _api_service,
                    socket_path=args.socket if args.socket else home / "api.sock",
                )
                _logger.info(
                    "cli command=api outcome=ok duration_ms=%.1f",
                    (time.monotonic() - started) * 1000,
                )
                return returned if isinstance(returned, int) else 0
            if args.command == "wait":
                # A wait verb exits with the run's own terminal code, so it
                # returns here instead of through the always-successful emit.
                code = _wait_command(args, target, stdout, stderr)
                _logger.info(
                    "cli command=%s outcome=ok duration_ms=%.1f",
                    args.command, (time.monotonic() - started) * 1000,
                )
                return code
            result = _execute(args, target, stdin)
        _emit(result, stdout)
        _logger.info(
            "cli command=%s outcome=%s duration_ms=%.1f",
            args.command,
            "degraded" if getattr(result, "ok", True) is False else "ok",
            (time.monotonic() - started) * 1000,
        )
        # A doctor report with any error-severity finding is a failed check,
        # not a successful command -- surface that as a nonzero exit.
        if args.command == "doctor" and getattr(result, "ok", True) is False:
            return _EXPECTED_ERROR_EXIT
        # A collection report with any failed, partial, or data-less runtime
        # is a degraded round: the capacity view is incomplete or stale, so
        # the one-shot command (and the launchd loop driving it) must not
        # report success while the JSON payload stays intact on stdout.
        if (
            args.command == "capacity"
            and args.capacity_command == "collect"
            and getattr(result, "ok", True) is False
        ):
            return _EXPECTED_ERROR_EXIT
        return 0
    except AgentRunError as error:
        _emit(_error_payload(error), stderr)
        _logger.warning(
            "cli command=%s outcome=%s duration_ms=%.1f",
            args.command, type(error).__name__, (time.monotonic() - started) * 1000,
        )
        return _EXPECTED_ERROR_EXIT
    finally:
        if owned is not None:
            owned.close()
        if start_broker is not None:
            start_broker.close()
        if child_reaper is not None:
            child_reaper.close()


if __name__ == "__main__":
    raise SystemExit(main())
