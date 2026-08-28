"""Fresh-interpreter entrypoint for one detached supervisor.

The parent forks and immediately execs this module. Nothing may run in the child
between fork and exec: on macOS a forked child of a process that has used SQLite
segfaults inside ``sqlite3.connect`` roughly half the time, and the start CLI
always has ``state.db`` open. Everything the supervisor needs therefore arrives
here as one JSON payload on an inherited pipe.

The module-level imports below are themselves a failure window: if this
package's own release directory was deleted while this interpreter was still
alive, importing ``.adapters``/``.config``/``.state`` here is exactly where
that shows up -- before ``main()``, and therefore before argparse, even runs.
``_bootstrap_error_fd``/``_write_early_failure`` stay stdlib-only and
self-contained (not imported from ``launch_evidence``) so they still work
when importing this package's own submodules is exactly what is failing.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import traceback
from contextlib import suppress
from pathlib import Path


def _bootstrap_error_fd(argv: list[str]) -> int | None:
    """Pull ``--error-fd`` out of argv without argparse, before risky imports run."""

    for index, item in enumerate(argv):
        if item == "--error-fd" and index + 1 < len(argv):
            with suppress(ValueError):
                return int(argv[index + 1])
    return None


def _write_early_failure(fd: int | None, stage: str, error: BaseException) -> None:
    """Best-effort, bounded, stdlib-only failure record for the pre-identity window."""

    if fd is None:
        return
    record = {
        "stage": stage,
        "type": type(error).__name__,
        "message": str(error).strip()[:500],
        "traceback": "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )[-2000:],
    }
    with suppress(OSError):
        os.write(fd, (json.dumps(record) + "\n").encode("utf-8", "replace")[:4096])


_ERROR_FD = _bootstrap_error_fd(sys.argv)
try:
    from collections.abc import Callable, Mapping

    from .adapters.base import LaunchPlan
    from .adapters.registry import AdapterRegistry
    from .config import load_config
    from .errors import ValidationError
    from .lifecycle import ReadyChannel
    from .paths import config_path, state_db_path
    from .state.store import StateStore
    from .supervisor import Supervisor, SupervisorSettings
except BaseException as _import_error:  # fail closed with evidence, no partial module
    _write_early_failure(_ERROR_FD, "import", _import_error)
    os._exit(1)


def _arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="agent-run-supervisor")
    parser.add_argument("--payload-fd", type=int, required=True)
    parser.add_argument("--ready-fd", type=int, required=True)
    parser.add_argument("--identity-fd", type=int, required=True)
    parser.add_argument("--error-fd", type=int, required=True)
    return parser.parse_args(sys.argv[1:] if argv is None else argv)


def _report_identity(fd: int) -> None:
    """Prove the exec'd process is the session leader the parent forked."""

    try:
        pid = os.getpid()
        if os.getpgrp() != pid:
            raise ValidationError("detached supervisor is not its process-group leader")
        os.write(fd, f"{pid}\n".encode("ascii"))
    finally:
        _close(fd)


def _read_payload(fd: int) -> dict[str, object]:
    chunks: list[bytes] = []
    try:
        while True:
            chunk = os.read(fd, 65536)
            if not chunk:
                break
            chunks.append(chunk)
    finally:
        _close(fd)
    try:
        payload = json.loads(b"".join(chunks).decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValidationError(f"malformed supervisor payload: {error}") from error
    if not isinstance(payload, dict):
        raise ValidationError("supervisor payload must be a JSON object")
    return payload


def _text(payload: Mapping[str, object], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ValidationError(f"supervisor payload field {key} must be a nonblank string")
    return value


def _number(payload: Mapping[str, object], key: str) -> float:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValidationError(f"supervisor payload field {key} must be a number")
    return float(value)


def _supervise(payload: Mapping[str, object], home: Path, ready: ReadyChannel) -> None:
    config = load_config(config_path(home))
    adapter = AdapterRegistry(config).load(_text(payload, "runtime"))
    plan = LaunchPlan.from_payload(payload.get("plan"))
    store = StateStore.open(state_db_path(home))
    try:
        Supervisor(
            store,
            _text(payload, "agent_id"),
            adapter,
            plan,
            answer_path=Path(_text(payload, "answer_path")),
            timeout_seconds=_number(payload, "timeout_seconds"),
            settings=SupervisorSettings(
                warning_fraction=_number(payload, "warning_fraction")
            ),
            ready=ready,
        ).run()
    finally:
        store.close()


def _dispatch(home: Path) -> None:
    from .cli import _dispatch_once

    _dispatch_once(home)


def main(argv: list[str] | None = None) -> int:
    """Run one supervisor, then its bounded post-terminal delivery dispatch."""

    args = _arguments(argv)
    ready = ReadyChannel.from_write_fd(args.ready_fd)
    error_fd = args.error_fd
    try:
        _report_identity(args.identity_fd)
    except BaseException as error:  # identity was never proven: the error pipe
        # is the only channel the parent still reads at this point.
        _write_early_failure(error_fd, "identity", error)
        ready.failed(_failure_reason(error))
        ready.close_write()
        return 1
    with suppress(OSError):
        os.close(error_fd)

    try:
        payload = _read_payload(args.payload_fd)
        home = Path(_text(payload, "home"))
        dispatch_timeout = _number(payload, "post_terminal_timeout_seconds")
    except BaseException as error:  # identity is already proven: the ready
        # pipe alone carries this reason back to the parent.
        ready.failed(_failure_reason(error))
        ready.close_write()
        return 1

    exit_code = 0
    try:
        _redirect_standard_streams()
        _supervise(payload, home, ready)
    except BaseException as error:
        ready.failed(_failure_reason(error))
        exit_code = 1
    finally:
        ready.close_write()
        try:
            _bounded(lambda: _dispatch(home), dispatch_timeout)
        except BaseException:
            exit_code = 1
    return exit_code


def _bounded(callback: Callable[[], object], timeout_seconds: float) -> None:
    def expired(_number: int, _frame: object) -> None:
        raise TimeoutError("post-terminal callback exceeded its deadline")

    previous = signal.getsignal(signal.SIGALRM)
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        callback()
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def _redirect_standard_streams() -> None:
    descriptor = os.open(os.devnull, os.O_RDWR)
    try:
        for target in (0, 1, 2):
            os.dup2(descriptor, target)
    finally:
        if descriptor > 2:
            os.close(descriptor)


def _failure_reason(error: BaseException) -> str:
    detail = str(error).strip()
    return (f"{type(error).__name__}: {detail}" if detail else type(error).__name__)[:300]


def _close(fd: int) -> None:
    with suppress(OSError):
        os.close(fd)


if __name__ == "__main__":  # pragma: no cover - exercised through the exec path
    os._exit(main())
