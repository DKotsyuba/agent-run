"""Bootstrap failure evidence shared by the parent launcher and its child.

Before this module existed, a detached supervisor that died before it could
prove its identity (a missing venv python, a broken package install after a
release directory was deleted, ...) left the parent with nothing but an empty
read on the identity pipe: no stage, no exit status, no traceback, and no
durable agent_id to report back to the caller. The helpers here let both the
pre-exec fork window in ``launch.py`` and the early bootstrap of
``supervisor_main.py`` hand back a small bounded record of what went wrong,
and let the parent turn that (or its absence) into one named, diagnosable
error.
"""

from __future__ import annotations

import json
import logging
import os
import select
import time
import traceback as _traceback

from .errors import ValidationError

_logger = logging.getLogger("agent_run.launch")

#: The child's executable no longer exists or is not runnable (e.g. its
#: release directory was deleted while the process was still alive).
FAILURE_KIND_EXECUTABLE_MISSING = "supervisor_executable_missing"

#: A forked child died before it could prove its identity to the parent, with
#: or without an error record explaining why.
FAILURE_KIND_BOOTSTRAP = "supervisor_start_failed"

_MESSAGE_CHARS = 500
_TRACEBACK_CHARS = 2000
_RECORD_BYTES = 4096
_DEFAULT_ERROR_READ_SECONDS = 0.5


class SupervisorBootstrapError(ValidationError):
    """A detached supervisor never proved it was supervising.

    Raised by the preflight check (the executable itself is unusable) or by
    the parent when the identity pipe closes empty. Carries whatever
    diagnosable evidence exists -- the failing stage, the child's exception
    type/message/traceback when it managed to write one, the provisional
    (unproven) child pid, and its reaped exit/signal status -- so a caller
    keeps the agent_id and a real reason instead of a bare "exited before
    session proof".
    """

    def __init__(
        self,
        message: str,
        *,
        failure_kind: str,
        failure_stage: str | None = None,
        bootstrap_error_type: str | None = None,
        bootstrap_traceback: str | None = None,
        provisional_pid: int | None = None,
        proven: bool = False,
    ) -> None:
        super().__init__(message)
        self.failure_kind = failure_kind
        self.failure_stage = failure_stage
        self.bootstrap_error_type = bootstrap_error_type
        self.bootstrap_traceback = bootstrap_traceback
        self.provisional_pid = provisional_pid
        self.proven = proven


def preflight_executable(executable: str) -> None:
    """Refuse to fork when the interpreter a session started with is gone.

    Turns the whole "release directory deleted out from under a live MCP
    session" incident class into one instant, self-explaining refusal instead
    of a fork that is doomed to die inside ``execv``.
    """

    if os.path.exists(executable) and os.access(executable, os.X_OK):
        _logger.debug("preflight ok executable=%s", executable)
        return
    _logger.warning("preflight failed executable=%s stage=preflight", executable)
    raise SupervisorBootstrapError(
        f"supervisor executable is gone (release deleted?): {executable}; "
        "reconnect/restart this MCP session",
        failure_kind=FAILURE_KIND_EXECUTABLE_MISSING,
        failure_stage="preflight",
    )


def write_exec_failure(fd: int, error: BaseException) -> None:
    """Record why ``execv`` failed, in the narrow fork-to-exec window.

    Nothing but ``setsid`` and ``execv`` may run between fork and exec; this
    is the one permitted addition on failure, so it stays a single bounded
    ``os.write`` with no traceback formatting.
    """

    errno = getattr(error, "errno", None)
    message = getattr(error, "strerror", None) or str(error)
    record: dict[str, object] = {
        "stage": "exec",
        "type": type(error).__name__,
        "message": str(message)[:_MESSAGE_CHARS],
    }
    if errno is not None:
        record["errno"] = errno
    _write_record(fd, record)


def write_bootstrap_record(fd: int, stage: str, error: BaseException) -> None:
    """Best-effort, bounded write of one failure record; never raises."""

    tail = "".join(
        _traceback.format_exception(type(error), error, error.__traceback__)
    )
    record = {
        "stage": stage,
        "type": type(error).__name__,
        "message": str(error).strip()[:_MESSAGE_CHARS],
        "traceback": tail[-_TRACEBACK_CHARS:],
    }
    _write_record(fd, record)


def _write_record(fd: int, record: dict[str, object]) -> None:
    try:
        blob = (json.dumps(record) + "\n").encode("utf-8", "replace")
        os.write(fd, blob[:_RECORD_BYTES])
    except OSError:
        pass


def read_bootstrap_record(
    fd: int, timeout_seconds: float = _DEFAULT_ERROR_READ_SECONDS
) -> dict[str, object] | None:
    """Nonblocking, bounded read of whatever the child wrote before dying."""

    try:
        readable, _, _ = select.select([fd], [], [], max(0.0, timeout_seconds))
    except OSError:
        return None
    if not readable:
        return None
    try:
        chunk = os.read(fd, _RECORD_BYTES)
    except OSError:
        return None
    if not chunk:
        return None
    try:
        return json.loads(chunk.splitlines()[0].decode("utf-8", "replace"))
    except (ValueError, IndexError):
        return None


def diagnose_bootstrap_failure(
    pid: int,
    error_fd: int,
    *,
    read_timeout_seconds: float = _DEFAULT_ERROR_READ_SECONDS,
) -> SupervisorBootstrapError:
    """Build the one error a caller sees when identity was never proven.

    Reads the error pipe (bounded, nonblocking) for whatever the child
    recorded, and reaps the child (bounded) for its exit/signal status. The
    child is expected to already be gone -- an empty identity read only
    happens once every writer of the identity pipe has closed it -- so the
    reap here is just draining the zombie, not waiting on a live process.
    """

    record = read_bootstrap_record(error_fd, read_timeout_seconds)
    status_text = _describe_exit_status(_wait_for_exit_status(pid, read_timeout_seconds))
    if record is not None:
        stage = record.get("stage")
        error_type = record.get("type")
        message = record.get("message")
        traceback_tail = record.get("traceback")
        summary = (
            f"detached supervisor died before session proof at stage "
            f"{stage!r}: {error_type}: {message} ({status_text})"
        )
        _logger.warning(
            "bootstrap_failure pid=%d stage=%s error_type=%s (%s)",
            pid, stage, error_type, status_text,
        )
        return SupervisorBootstrapError(
            summary,
            failure_kind=FAILURE_KIND_BOOTSTRAP,
            failure_stage=str(stage) if stage is not None else None,
            bootstrap_error_type=str(error_type) if error_type is not None else None,
            bootstrap_traceback=str(traceback_tail) if traceback_tail is not None else None,
            provisional_pid=pid,
            proven=False,
        )
    summary = (
        "detached supervisor exited before session proof with no bootstrap "
        f"evidence ({status_text}); provisional pid {pid} was never proven"
    )
    _logger.warning("bootstrap_failure pid=%d stage=unknown no_evidence (%s)", pid, status_text)
    return SupervisorBootstrapError(
        summary,
        failure_kind=FAILURE_KIND_BOOTSTRAP,
        provisional_pid=pid,
        proven=False,
    )


def _wait_for_exit_status(pid: int, timeout_seconds: float) -> int | None:
    deadline = time.monotonic() + max(0.0, timeout_seconds)
    while True:
        try:
            waited, status = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            return None
        if waited == pid:
            return status
        if time.monotonic() >= deadline:
            return None
        time.sleep(0.005)


def _describe_exit_status(status: int | None) -> str:
    if status is None:
        return "exit status unknown"
    if os.WIFSIGNALED(status):
        return f"killed by signal {os.WTERMSIG(status)}"
    if os.WIFEXITED(status):
        return f"exit code {os.WEXITSTATUS(status)}"
    return f"raw status {status}"


def bootstrap_event_data(agent_id: object, error: BaseException) -> dict[str, object]:
    """Build the durable event ``data`` for a ``SupervisorBootstrapError``.

    Kept alongside the error type so ``service.py``'s failure path stays a
    one-line call instead of re-deriving this shape at every call site.
    """

    data: dict[str, object] = {"agent_id": str(agent_id)}
    if not isinstance(error, SupervisorBootstrapError):
        return data
    if error.failure_stage is not None:
        data["stage"] = error.failure_stage
    if error.bootstrap_error_type is not None:
        data["type"] = error.bootstrap_error_type
    if error.bootstrap_traceback is not None:
        data["traceback"] = error.bootstrap_traceback
    if error.provisional_pid is not None:
        data["provisional_pid"] = error.provisional_pid
    data["proven"] = error.proven
    return data


def bootstrap_error_fields(error: BaseException) -> dict[str, object]:
    """Extract the enriched start()-failure fields a transport should surface.

    ``service.start`` attaches ``agent_id``/``failure_kind``/``failure_stage``/
    ``failure_text`` to the exception it raises once the durable agent row
    exists; CLI and MCP both call this to extend their existing error
    envelope instead of hiding the agent_id behind a bare error message.
    """

    fields: dict[str, object] = {}
    agent_id = getattr(error, "agent_id", None)
    if agent_id is not None:
        fields["agent_id"] = str(agent_id)
        fields["status"] = "failed"
    kind = getattr(error, "failure_kind", None)
    if kind is not None:
        fields["failure_kind"] = kind
    stage = getattr(error, "failure_stage", None)
    if stage is not None:
        fields["failure_stage"] = stage
    text = getattr(error, "failure_text", None)
    if text is not None:
        fields["failure_text"] = text
    return fields
