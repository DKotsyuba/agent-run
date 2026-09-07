"""Bounded local executable-version observation for runtime adapters."""

from __future__ import annotations

import os
import signal
import subprocess
import threading
from pathlib import Path
from typing import BinaryIO


_VERSION_TIMEOUT_SECONDS = 2.0
_VERSION_OUTPUT_BYTES = 4096


def _stop_process(process: subprocess.Popen[bytes]) -> None:
    """Stop the owned process group and reap its leader without raising."""

    if process.poll() is not None:
        return
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        return
    except OSError:
        process.kill()
    process.wait()


def _read_bounded(
    stream: BinaryIO,
    process: subprocess.Popen[bytes],
    output: bytearray,
    oversized: threading.Event,
) -> None:
    """Read at most one byte beyond the output limit, then stop the process."""

    while chunk := stream.read(1024):
        remaining = _VERSION_OUTPUT_BYTES + 1 - len(output)
        output.extend(chunk[:remaining])
        if len(output) > _VERSION_OUTPUT_BYTES:
            oversized.set()
            _stop_process(process)
            return


def observe_binary_version(binary: Path, home: Path) -> tuple[str | None, str | None]:
    """Run ``binary --version`` once with bounded time, output, and environment.

    The first nonblank standard-output line is returned as the fresh version
    observation. Failures return ``(None, diagnostic)``; diagnostics never
    contain child output, inherited secrets, or raw environment values.
    The owned process group is killed and reaped on timeout or oversized output.
    """

    environment = {
        "HOME": str(home),
        "PATH": os.environ.get("PATH", os.defpath),
    }
    try:
        process = subprocess.Popen(
            (str(binary), "--version"),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            env=environment,
            start_new_session=True,
        )
    except OSError:
        return None, "version command could not be started"

    assert process.stdout is not None
    output = bytearray()
    oversized = threading.Event()
    reader = threading.Thread(
        target=_read_bounded,
        args=(process.stdout, process, output, oversized),
        daemon=True,
        name="agent-run-version-reader",
    )
    reader.start()
    try:
        returncode = process.wait(timeout=_VERSION_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired:
        _stop_process(process)
        diagnostic = f"version command timed out after {_VERSION_TIMEOUT_SECONDS:g} seconds"
    else:
        diagnostic = None
    finally:
        reader.join(timeout=1)
        process.stdout.close()

    if reader.is_alive():
        _stop_process(process)
        return None, "version command output did not close"
    if oversized.is_set():
        return None, f"version command output exceeds {_VERSION_OUTPUT_BYTES} bytes"
    if diagnostic is not None:
        return None, diagnostic
    if returncode != 0:
        return None, f"version command exited with status {returncode}"
    version = next(
        (line.strip() for line in output.decode("utf-8", errors="replace").splitlines() if line.strip()),
        None,
    )
    if version is None:
        return None, "version command returned no version"
    return version, None
