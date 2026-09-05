"""Process and log plumbing shared by the Claude-family runtime sessions.

Three concerns that surround a launched child rather than decode it: aborting
a child whose session never came up, opening its private runtime log, and
resolving the literal secret values that must be redacted out of everything it
writes. They live here so :mod:`agent_run.adapters.claude.adapter` stays inside
the per-file adapter size gate; behaviour is unchanged and nothing outside the
Claude family imports them.
"""

from __future__ import annotations

import os
import signal
import subprocess
from pathlib import Path
from typing import IO

from ..base import LaunchPlan

__all__ = ["abort_launch", "known_secrets", "open_runtime_log"]


def abort_launch(process: "subprocess.Popen[str]") -> None:
    """Native-cancel, reap, and close pipes for a child that never got a session.

    Runs when opening the runtime log, wiring the reader thread, or writing
    the initial prompt fails during ``ClaudeSession`` construction, so the
    child never outlives a failed ``launch``.

    :param process: The launched child; killed by process group when still
        running, then waited for up to 5 seconds. Its stdin and stdout pipes
        are closed here.
    :returns: ``None``. Every OS error on the kill/close paths is swallowed:
        the caller is already unwinding a launch failure it must re-raise.
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


def open_runtime_log(path: Path) -> IO[str]:
    """Open the runtime stream log privately: O_CREAT|O_APPEND|O_WRONLY, mode 0600.

    :param path: Destination log file; created when missing, appended when it
        already exists, and forced to owner-only permissions either way.
    :returns: A UTF-8 text stream in append mode. The caller owns closing it.
    :raises OSError: When the file cannot be created, opened, or chmod'ed.
    """

    descriptor = os.open(str(path), os.O_CREAT | os.O_APPEND | os.O_WRONLY, 0o600)
    os.fchmod(descriptor, 0o600)
    return os.fdopen(descriptor, "a", encoding="utf-8")


def known_secrets(plan: LaunchPlan) -> frozenset[str]:
    """Literal secret values that must never reach the decoder or the disk log.

    :param plan: The launch plan; ``adapter_state["secret_env_names"]`` names
        the environment variables whose live values are secrets.
    :returns: The nonempty values of those variables. An absent, non-sequence,
        or non-string entry contributes nothing rather than raising: the state
        is a rebuilt payload (:meth:`LaunchPlan.from_payload`) and redaction
        must not be the thing that fails a run.
    """

    names = plan.adapter_state.get("secret_env_names", ())
    if not isinstance(names, (tuple, list)):
        return frozenset()
    return frozenset(
        value
        for name in names
        if isinstance(name, str) and (value := plan.environment.get(name))
    )
