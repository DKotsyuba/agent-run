"""A payload-driven stand-in for agent_run.supervisor_main.

The detached launch tests exec this module instead of the real supervisor so the
parent-side contract (identity proof, READY gating, cleanup, reaping, bounded
post-terminal dispatch) can be exercised without a runtime or a state database.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from agent_run.lifecycle import ReadyChannel
from agent_run.supervisor_main import _bounded, _failure_reason


def _arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="detached-child")
    parser.add_argument("--payload-fd", type=int, required=True)
    parser.add_argument("--ready-fd", type=int, required=True)
    parser.add_argument("--identity-fd", type=int, required=True)
    return parser.parse_args(sys.argv[1:] if argv is None else argv)


def _read_payload(fd: int) -> dict:
    chunks: list[bytes] = []
    while True:
        chunk = os.read(fd, 65536)
        if not chunk:
            break
        chunks.append(chunk)
    os.close(fd)
    return json.loads(b"".join(chunks).decode("utf-8"))


def _wait(payload: dict, gate: str | None) -> None:
    expires = time.monotonic() + float(payload.get("fail_safe_seconds", 10.0))
    while time.monotonic() < expires:
        if gate is not None and Path(gate).exists():
            return
        time.sleep(0.01)


def _append(path: str, text: str) -> None:
    with Path(path).open("a", encoding="utf-8") as stream:
        stream.write(text)


def _body(payload: dict, ready: ReadyChannel) -> None:
    gate = payload.get("gate")
    grandchild = None
    if payload.get("grandchild"):
        grandchild = os.fork()
        if grandchild == 0:
            _wait(payload, gate)
            os._exit(0)
    if payload.get("evidence"):
        Path(payload["evidence"]).write_text(
            f"{os.getpid()}" + ("" if grandchild is None else f" {grandchild}"),
            encoding="utf-8",
        )
    if payload.get("events"):
        Path(payload["events"]).write_text("before-ready\n", encoding="utf-8")
    if payload.get("fail"):
        raise RuntimeError(str(payload["fail"]))
    if payload.get("ready", True):
        ready.ready()
    if gate is not None or grandchild is not None:
        _wait(payload, gate)
    if grandchild is not None:
        os.waitpid(grandchild, 0)
    if payload.get("events"):
        _append(payload["events"], "terminal\n")


def _dispatch(payload: dict) -> None:
    if not payload.get("dispatch"):
        return
    _append(str(payload["dispatch"]), "once\n")
    time.sleep(float(payload.get("dispatch_sleep", 0.0)))


def main(argv: list[str] | None = None) -> int:
    args = _arguments(argv)
    ready = ReadyChannel.from_write_fd(args.ready_fd)
    payload = _read_payload(args.payload_fd)
    reported = payload.get("report_pid")
    os.write(
        args.identity_fd,
        f"{os.getpid() if reported is None else int(reported)}\n".encode("ascii"),
    )
    os.close(args.identity_fd)

    exit_code = 0
    try:
        _body(payload, ready)
    except BaseException as error:
        ready.failed(_failure_reason(error))
        exit_code = 1
    finally:
        ready.close_write()
        try:
            _bounded(
                lambda: _dispatch(payload),
                float(payload["post_terminal_timeout_seconds"]),
            )
        except BaseException:
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    os._exit(main())
