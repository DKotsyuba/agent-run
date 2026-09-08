"""Bounded stream-session ownership for the Claude-compatible adapters."""

from __future__ import annotations

import contextlib
import io
import json
import os
import select
import signal
import subprocess
import threading
import time
from dataclasses import replace

from ...domain import AgentStatus, Outcome
from ...errors import ValidationError
from ..base import EventSink, LaunchPlan
from ..home import seal_answer
from .launch_io import known_secrets, open_runtime_log
from .stderr import StderrTail
from .stream import StreamDecoder, classify_failure, sanitize_line, terminal_event_data

_INPUT_WRITE_TIMEOUT_SECONDS = 1.0
_INPUT_WRITE_POLL_SECONDS = 0.05


class ClaudeSession:
    """Own one Claude-compatible child and its decoded JSON stream.

    The session owns the text-mode process pipes from construction through
    :meth:`wait`. Real pipe writes use a bounded nonblocking descriptor loop;
    descriptorless in-memory streams used by injected tests receive normal
    synchronous writes. Reader failures are retained and re-raised by
    :meth:`wait`, while terminal output is sealed into the planned answer path.
    """

    def __init__(
        self, process: "subprocess.Popen[str]", plan: LaunchPlan, sink: EventSink
    ) -> None:
        """Start stdout and stderr readers for ``process``.

        ``plan`` supplies the initial input, output paths, resume identity, and
        secret names. ``sink`` receives decoded events. Wiring or initial-input
        failures close the runtime log and propagate so the caller can stop the
        child. The process must expose text streams; in-memory test streams may
        omit a file descriptor. A real freshly launched process whose PID is
        also its process-group ID records that group for native cancellation.
        """
        self._process = process
        self._owned_process_group: int | None = None
        if type(process.pid) is int and process.pid > 1:
            try:
                process_group = os.getpgid(process.pid)
            except OSError:
                pass
            else:
                if process_group == process.pid:
                    self._owned_process_group = process_group
        self._plan = plan
        self._sink = sink
        self._decoder = StreamDecoder()
        self._lock = threading.Lock()
        self._write_lock = threading.Lock()
        self._write_cancelled = threading.Event()
        self._cancelled = False
        self._reader_error: BaseException | None = None
        self._settled = threading.Event()
        self._force_stopped = False
        self._reported_session_id: str | None = None
        self._secrets = known_secrets(plan)
        self._stderr = StderrTail(process.stderr, self._secrets)
        self._raw_stream = open_runtime_log(plan.runtime_stream_path)
        try:
            if process.stdin is not None:
                try:
                    os.set_blocking(process.stdin.fileno(), False)
                except (AttributeError, io.UnsupportedOperation):
                    pass
            self._stderr_reader = threading.Thread(target=self._stderr.drain, daemon=True)
            self._stderr_reader.start()
            self._reader = threading.Thread(target=self._read_stdout, daemon=True)
            self._reader.start()
            if plan.initial_input:
                self._write_input(plan.initial_input, "initial input")
        except BaseException:
            self._raw_stream.close()
            raise

    def _write_input(self, text: str, label: str) -> None:
        """Write one prompt or steer frame, bounded for real pipes.

        ``text`` is the complete UTF-8 frame and ``label`` identifies it in a
        raised error. Real descriptors use one-second cancellable polling to
        handle backpressure. Streams without ``fileno`` are intentional
        in-memory doubles and use their synchronous ``write``/``flush`` API;
        their I/O errors still surface as ``ConnectionError``.
        """
        frame = text.encode("utf-8")
        deadline = time.monotonic() + _INPUT_WRITE_TIMEOUT_SECONDS
        with self._write_lock:
            stdin = self._process.stdin
            if stdin is None:
                raise ConnectionError(f"claude closed stdin while writing {label}")
            try:
                fd = stdin.fileno()
            except (AttributeError, io.UnsupportedOperation):
                try:
                    stdin.write(text)
                    stdin.flush()
                except (OSError, ValueError) as error:
                    raise ConnectionError(
                        f"claude closed stdin while writing {label}"
                    ) from error
                return
            except ValueError as error:
                raise ConnectionError(
                    f"claude closed stdin while writing {label}"
                ) from error
            sent = 0
            while sent < len(frame):
                if self._write_cancelled.is_set():
                    raise InterruptedError(f"claude cancelled while writing {label}")
                if self._process.poll() is not None:
                    raise ConnectionError(f"claude exited while writing {label}")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"claude timed out writing {label}")
                try:
                    _, writable, _ = select.select(
                        [], [fd], [], min(remaining, _INPUT_WRITE_POLL_SECONDS)
                    )
                except (OSError, ValueError) as error:
                    raise ConnectionError(
                        f"claude closed stdin while writing {label}"
                    ) from error
                if not writable:
                    continue
                try:
                    count = os.write(fd, frame[sent:])
                except BlockingIOError:
                    continue
                except OSError as error:
                    raise ConnectionError(
                        f"claude closed stdin while writing {label}"
                    ) from error
                if count <= 0:
                    raise ConnectionError(f"claude closed stdin while writing {label}")
                sent += count

    @property
    def pid(self) -> int | None:
        """Return the child's process identifier, when the process exposes one."""
        return self._process.pid

    @property
    def owns_process_group(self) -> bool:
        """Report that this session owns the process group created at launch."""
        return True

    def _error_only_result_line(self, result_text: str) -> str | None:
        """Return a provider-error result line, or ``None`` for Claude output."""
        del result_text
        return None

    def _read_stdout(self) -> None:
        """Redact, persist, decode, and publish stdout until EOF.

        Resume streams must repeat the expected native session id. Exceptions
        are retained for :meth:`wait` while the reader continues draining and
        always wakes waiting callers on terminal output, error, or EOF.
        """
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
                    expected = self._plan.resume_session_id
                    if expected is not None and result.session_id and result.session_id != expected:
                        raise ValidationError("runtime resumed a different native session")
                    if expected is not None and result.terminal and result.terminal.runtime_session_id != expected:
                        raise ValidationError("runtime did not confirm the resumed native session")
                    if result.session_id and result.session_id != self._reported_session_id:
                        self._reported_session_id = result.session_id
                        self._sink.session(result.session_id)
                    for message in result.messages:
                        self._sink.message(message)
                    if result.event:
                        self._sink.event(*result.event)
                    if result.warning:
                        with contextlib.suppress(Exception):
                            self._sink.event("stream_diagnostic", {"reason": result.warning})
                    if result.terminal:
                        self._sink.event("runtime_result", terminal_event_data(result.terminal))
                        self._settled.set()
                except BaseException as error:
                    if self._reader_error is None:
                        self._reader_error = error
                    self._settled.set()
        finally:
            self._settled.set()

    def steer(self, text: str) -> None:
        """Send one nonblank user steer frame through the input contract."""
        if not isinstance(text, str) or not text.strip():
            raise ValidationError("steer text must be nonblank")
        line = (
            json.dumps(
                {"type": "user", "message": {"role": "user", "content": [{"type": "text", "text": text}]}},
                sort_keys=True,
            )
            + "\n"
        )
        self._write_input(line, "steer")

    def cancel(self, grace_seconds: float) -> None:
        """Interrupt the owned process group and bound the leader's exit wait.

        Grace seconds is a nonnegative group-exit budget after SIGINT. A real
        adapter launch records its freshly created PID-equals-PGID group during
        construction and interrupts the whole group before the leader can leave
        descendants behind. The leader is deliberately not polled/reaped while
        group liveness is checked, so its PID/PGID cannot be reused before a
        surviving group receives SIGKILL at the deadline. Injected processes
        without that proof retain the leader-only fallback. Signal failures
        return without weakening the supervisor's later independent PID/birth
        verification.
        """
        self._cancelled = True
        self._write_cancelled.set()
        deadline = time.monotonic() + max(grace_seconds, 0.0)
        if self._owned_process_group is None:
            if self._process.poll() is not None:
                return
            try:
                self._process.send_signal(signal.SIGINT)
            except OSError:
                return
            while time.monotonic() < deadline and self._process.poll() is None:
                time.sleep(0.05)
            return
        try:
            os.killpg(self._owned_process_group, signal.SIGINT)
        except OSError:
            return
        while time.monotonic() < deadline:
            try:
                os.killpg(self._owned_process_group, 0)
            except ProcessLookupError:
                return
            except OSError:
                break
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        try:
            os.killpg(self._owned_process_group, signal.SIGKILL)
        except OSError:
            pass

    def _stop_process(self) -> None:
        """End a child that stayed alive after producing its terminal result."""
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
        """Return the sealed terminal outcome, or ``None`` before the timeout.

        A terminal stream result settles the one-shot session even if the CLI
        waits for another turn. Resume identity errors and reader failures
        propagate after pipes have been closed. Successful results seal the
        answer; other outcomes preserve bounded failure details.
        """
        if not self._settled.wait(timeout=timeout_seconds):
            return None
        try:
            exit_code = self._process.wait(timeout=0.5)
        except subprocess.TimeoutExpired:
            self._stop_process()
            try:
                exit_code = self._process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                return None
        self._reader.join(timeout=5 if timeout_seconds is None else timeout_seconds)
        self._stderr_reader.join(timeout=5 if timeout_seconds is None else timeout_seconds)
        if self._reader.is_alive() or self._stderr_reader.is_alive():
            return None
        with self._lock:
            self._raw_stream.close()
        for pipe in (self._process.stdin, self._process.stdout, self._process.stderr):
            if pipe is not None:
                try:
                    pipe.close()
                except OSError:
                    pass
        if self._reader_error is not None:
            raise self._reader_error
        metadata = self._decoder.finalize()
        stderr_text = self._stderr.text()
        exit_ok = exit_code == 0 or self._force_stopped
        error_only_line = (
            self._error_only_result_line(metadata.result_text) if metadata.result_text else None
        )
        succeeded = (
            exit_ok
            and not metadata.is_error
            and metadata.subtype != "no_answer"
            and bool(metadata.result_text)
            and error_only_line is None
        )
        if self._cancelled:
            status = AgentStatus.CANCELLED
        elif succeeded:
            status = AgentStatus.SUCCEEDED
        else:
            status = AgentStatus.FAILED
        if status is AgentStatus.SUCCEEDED:
            failure_kind = None
            failure_text = None
        elif error_only_line is not None:
            failure_kind = "provider_error"
            failure_text = error_only_line
        elif metadata.subtype == "no_answer" and stderr_text:
            classified = classify_failure(replace(metadata, subtype="", result_text=stderr_text))
            failure_kind = "provider_error" if classified == "engine_error" else classified
            failure_text = stderr_text
        else:
            empty_result = (
                exit_ok
                and not metadata.is_error
                and metadata.subtype != "no_answer"
                and not metadata.result_text
            )
            failure_kind = "empty_result" if empty_result else classify_failure(metadata)
            failure_text = metadata.result_text
        answer_path = None
        answer_bytes = None
        answer_sha256 = None
        if status is AgentStatus.SUCCEEDED:
            answer_path = self._plan.answer_path or self._plan.runtime_stream_path.with_name(
                "answer.md"
            )
            answer_bytes, answer_sha256 = seal_answer(answer_path, metadata.result_text or "")
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
