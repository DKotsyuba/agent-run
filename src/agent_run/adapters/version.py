"""Bounded local executable-version observation for runtime adapters."""

from __future__ import annotations

import os
import select
import signal
import subprocess
import time
from pathlib import Path

from ..lifecycle import checked_pgid

_VERSION_TIMEOUT_SECONDS = 2.0
_VERSION_OUTPUT_BYTES = 4096


def _stop_process(process: subprocess.Popen[bytes], process_group: int) -> None:
    """Stop the verified owned process group and reap its retained leader."""

    try:
        os.killpg(checked_pgid(process_group), signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=0.2)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def observe_binary_version(binary: Path, home: Path) -> tuple[str | None, str | None]:
    """Run ``binary --version`` once with bounded time, output, and environment.

    The first nonblank standard-output line is returned as the fresh version
    observation. Failures return ``(None, diagnostic)``; diagnostics never
    contain child output, inherited secrets, or raw environment values.
    The owned process group is killed and reaped on timeout or oversized output.
    """

    deadline = time.monotonic() + _VERSION_TIMEOUT_SECONDS
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
    process_group = checked_pgid(process.pid)
    descriptor = process.stdout.fileno()
    os.set_blocking(descriptor, False)
    output = bytearray()
    diagnostic: str | None = None
    returncode: int | None = None
    try:
        while True:
            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                diagnostic = (
                    f"version command timed out after {_VERSION_TIMEOUT_SECONDS:g} seconds"
                )
                break
            try:
                readable, _, _ = select.select(
                    (descriptor,), (), (), remaining_seconds
                )
            except InterruptedError:
                continue
            if not readable:
                diagnostic = (
                    f"version command timed out after {_VERSION_TIMEOUT_SECONDS:g} seconds"
                )
                break
            try:
                chunk = os.read(descriptor, 1024)
            except BlockingIOError:
                continue
            if not chunk:
                try:
                    returncode = process.wait(
                        timeout=max(0, deadline - time.monotonic())
                    )
                except subprocess.TimeoutExpired:
                    diagnostic = (
                        f"version command timed out after {_VERSION_TIMEOUT_SECONDS:g} seconds"
                    )
                break
            remaining_bytes = _VERSION_OUTPUT_BYTES + 1 - len(output)
            output.extend(chunk[:remaining_bytes])
            if len(output) > _VERSION_OUTPUT_BYTES:
                diagnostic = (
                    f"version command output exceeds {_VERSION_OUTPUT_BYTES} bytes"
                )
                break
    finally:
        if diagnostic is not None:
            _stop_process(process, process_group)
        process.stdout.close()

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
