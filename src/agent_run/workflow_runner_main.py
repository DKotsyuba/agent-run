"""Fresh-interpreter entrypoint for one detached workflow runner.

Hosted exactly like :mod:`agent_run.supervisor_main`: the parent forks and
immediately execs this module, and everything the runner needs arrives as one
JSON payload on an inherited pipe.  The payload, identity and readiness
plumbing is reused from there rather than restated, so the two detached
processes cannot drift apart.

What the runner executes is still a placeholder plan of no-op sleep steps.
Journalling each of them through the workflow store proves the whole run
lifecycle -- ownership, readiness, cancellation, reconciliation -- end to end
before the script engine exists.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time
from collections.abc import Mapping, Sequence
from contextlib import suppress
from pathlib import Path

from .errors import StateTransitionError, ValidationError
from .lifecycle import ReadyChannel, install_signal_handlers, restore_signal_handlers
from .paths import state_db_path
from .state import step_key, workflow_owner_identity
from .state.db import nonblank
from .state.store import StateStore
from .supervisor import supervisor_identity
from .supervisor_main import (
    _failure_reason,
    _read_payload,
    _redirect_standard_streams,
    _report_identity,
    _text,
)
from .workflow_run import validate_plan


CANCELLED_FAILURE_KIND = "runner_cancelled"
POLL_SECONDS = 0.05
_RESULT_JSON_LIMIT = 1024 * 1024


def _run_directory(home: Path, run_id: str) -> Path:
    """Create and return the private artifact directory for one workflow run."""

    directory = home / "workflows" / run_id
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(directory, 0o700)
    return directory


def _append_runner_log(path: Path, kind: str, message: object) -> None:
    """Append one timestamped durable runner line with private permissions."""

    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        os.chmod(path, 0o600)
        with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
            descriptor = -1
            stream.write(f"{time.time():.6f} {kind} {message}\n")
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _stored_result(home: Path, run_id: str, result: object) -> object:
    """Return a bounded JSON-safe result, spooling oversized JSON privately."""

    try:
        encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    except (TypeError, ValueError) as error:
        raise ValidationError("workflow result must be JSON-safe") from error
    payload = encoded.encode("utf-8")
    if len(payload) <= _RESULT_JSON_LIMIT:
        return result
    path = _run_directory(home, run_id) / "result.json"
    descriptor = os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600)
    try:
        os.chmod(path, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(payload)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    return {"spool_path": str(path.relative_to(home)), "bytes": len(payload)}


class _StopRequest:
    """The first stop signal received, remembered for the step loop to act on."""

    def __init__(self) -> None:
        self.signal_name: str | None = None

    def request(self, number: int) -> None:
        if self.signal_name is None:
            self.signal_name = signal.Signals(number).name

    @property
    def requested(self) -> bool:
        return self.signal_name is not None


def runner_identity() -> str:
    """The durable owner string this process claims its run with.

    The pid travels with the ps command because ``workflow_runs`` has a single
    ownership column; :func:`agent_run.state.reconcile_workflow_runs` needs
    both to decide whether the owner is still alive.
    """

    return workflow_owner_identity(os.getpid(), supervisor_identity())


def execute_plan(
    store: StateStore,
    run_id: str,
    steps: Sequence[Mapping[str, object]],
    *,
    identity: str,
    ready: ReadyChannel | None = None,
    poll_seconds: float = POLL_SECONDS,
    resume: bool = False,
) -> str:
    """Claim the run, journal every placeholder step, and finish the run.

    READY is reported only once the ownership claim has committed, so a parent
    that has seen READY knows reconciliation can find -- and lose -- this run.
    A stop signal fails the in-flight step as ``runner_cancelled`` and finishes
    the run ``cancelled``: the journal never shows a step that merely stopped.

    ``resume=True`` re-claims a finished-but-resumable run instead of claiming
    a freshly created one (see :func:`agent_run.state.workflow.resume_workflow_run`).
    Either way, a step whose ``step_key`` already has a ``succeeded`` journal
    row is replayed by skipping it rather than re-executed -- this is a no-op
    check on a fresh run, since its journal starts empty.
    """

    nonblank("run_id", run_id)
    nonblank("identity", identity)
    if resume:
        store.resume_workflow_run(run_id, identity)
    else:
        store.claim_workflow_run(run_id, identity)
    if ready is not None:
        ready.ready()
    stop = _StopRequest()
    previous = install_signal_handlers(stop.request)
    try:
        for position, step in enumerate(steps):
            if stop.requested:
                break
            key = step_key(step, position)
            if store.cached_step_result(run_id, key) is not None:
                continue  # already succeeded in an earlier pass -- replay, don't re-run
            seconds = float(step["seconds"])
            store.record_step_start(run_id, key, step)
            if _sleep(seconds, stop, poll_seconds):
                store.finish_step(
                    run_id,
                    key,
                    "failed",
                    failure_kind=CANCELLED_FAILURE_KIND,
                    failure_params={"signal": stop.signal_name},
                )
                break
            store.finish_step(
                run_id, key, "succeeded", result={"slept_seconds": seconds}
            )
        status = "cancelled" if stop.requested else "succeeded"
        store.finish_workflow_run(run_id, status)
        return status
    except BaseException:
        with suppress(ValidationError, StateTransitionError):
            store.finish_workflow_run(run_id, "failed")
        raise
    finally:
        restore_signal_handlers(previous)


def _sleep(seconds: float, stop: _StopRequest, poll_seconds: float) -> bool:
    """Sleep in slices so a stop signal is noticed promptly; True when stopped."""

    deadline = time.monotonic() + seconds
    while not stop.requested:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        time.sleep(min(poll_seconds, remaining))
    return True


def _arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="agent-run-workflow-runner")
    parser.add_argument("--payload-fd", type=int, required=True)
    parser.add_argument("--ready-fd", type=int, required=True)
    parser.add_argument("--identity-fd", type=int, required=True)
    return parser.parse_args(sys.argv[1:] if argv is None else argv)


def _run(payload: Mapping[str, object], home: Path, ready: ReadyChannel) -> None:
    run_id = _text(payload, "run_id")
    resume = bool(payload.get("resume", False))
    store = StateStore.open(state_db_path(home))
    try:
        plan = payload.get("plan")
        if isinstance(plan, Mapping) and set(plan) == {"script"} and isinstance(plan["script"], str):
            from agent_run.cli import _launch_callback
            from agent_run.service import AgentService
            from agent_run.workflow_executor import make_step_executor
            from agent_run.workflow_script import run_script

            if resume:
                store.resume_workflow_run(run_id, runner_identity())
            else:
                store.claim_workflow_run(run_id, runner_identity())
            ready.ready()
            stop = _StopRequest()
            previous = install_signal_handlers(stop.request)
            service = AgentService.from_home(home, launch=_launch_callback(home))
            try:
                log_path = _run_directory(home, run_id) / "runner.log"
                executor = make_step_executor(home, store, run_id, service=service, stop=stop)
                outcome = run_script(
                    plan["script"],
                    executor,
                    lambda name: _append_runner_log(log_path, "phase", name),
                    lambda text: _append_runner_log(log_path, "log", text),
                    4,
                )
                store.finish_workflow_run(
                    run_id,
                    "cancelled"
                    if stop.requested
                    else "failed"
                    if executor.failed
                    or (isinstance(outcome, Mapping) and "failure_kind" in outcome)
                    else "succeeded",
                    result=_stored_result(home, run_id, outcome),
                )
            except BaseException as error:
                _append_runner_log(log_path, "runner_error", repr(error))
                raise
            finally:
                service.close()
                restore_signal_handlers(previous)
            return
        steps = validate_plan(plan)
        execute_plan(
            store, run_id, steps, identity=runner_identity(), ready=ready, resume=resume
        )
    finally:
        store.close()


def main(argv: list[str] | None = None) -> int:
    """Run one detached workflow runner over the plan carried in its payload."""

    args = _arguments(argv)
    ready = ReadyChannel.from_write_fd(args.ready_fd)
    try:
        _report_identity(args.identity_fd)
        payload = _read_payload(args.payload_fd)
        home = Path(_text(payload, "home"))
    except BaseException as error:  # fail closed: the parent surfaces the reason
        ready.failed(_failure_reason(error))
        ready.close_write()
        return 1

    exit_code = 0
    try:
        _redirect_standard_streams()
        _run(payload, home, ready)
    except BaseException as error:
        with suppress(Exception):
            run_id = _text(payload, "run_id")
            _append_runner_log(
                _run_directory(home, run_id) / "runner.log", "runner_error", repr(error)
            )
        ready.failed(_failure_reason(error))
        exit_code = 1
    finally:
        ready.close_write()
    return exit_code


if __name__ == "__main__":  # pragma: no cover - exercised through the exec path
    os._exit(main())
