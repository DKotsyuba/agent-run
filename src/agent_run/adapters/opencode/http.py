"""Private-file HTTP transport for the managed OpenCode v2 service.

Three rules shape this module. Every reply is streamed to a private file before
it is decoded, so no large API response passes through a transport that may
truncate it; message state is read by short bounded polls, never by the long
``wait`` endpoint; and every capture is deleted as soon as it has been decoded,
so only the final transcript a ``raw_ref`` points at survives the run.

Endpoint constants below are the shapes proven against a local OpenCode v2
service; nothing here performs a live call on import.
"""

from __future__ import annotations

import base64
import json
import os
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlsplit

from ...errors import AgentRunError, ValidationError
from .service import PASSWORD_ENV, SERVICE_HOST, ServiceDescriptor, require_server_password


HEALTH_PATH = "/global/health"
CONFIG_PATH = "/config"
PROVIDERS_PATH = "/config/providers"
SESSION_PATH = "/session"
SESSION_STATUS_PATH = "/session/status"

MAX_RESPONSE_BYTES = 32 * 1024 * 1024
CHUNK_BYTES = 64 * 1024
REQUEST_TIMEOUT_SECONDS = 15.0
POLL_INTERVAL_SECONDS = 0.25
MAX_POLL_INTERVAL_SECONDS = 1.0
NO_CONTENT = 204

#: Statuses an adapter can prove are transient. 500 stays out: it is ambiguous.
TRANSIENT_STATUSES = frozenset({429, 502, 503, 504})
_AUTH_STATUSES = frozenset({401, 403})
_RETRYABLE_METHODS = frozenset({"GET"})
_WAIT_SEGMENTS = frozenset({"wait", "longpoll"})
_WAIT_QUERY_KEYS = frozenset({"wait", "longpoll", "long_wait"})


class HttpError(AgentRunError):
    """The service answered with a status the adapter cannot use."""

    def __init__(self, status: int, path: str, detail: str = "") -> None:
        suffix = f": {detail}" if detail else ""
        super().__init__(f"opencode service returned {status} for {path}{suffix}")
        self.status = status
        self.path = path


class TransientHttpError(HttpError):
    """A failure the adapter can prove is transient and may retry."""


class PollTimeout(AgentRunError):
    """Bounded polling ended before the awaited state appeared."""


@dataclass(frozen=True)
class RetryPolicy:
    attempts: int = 3
    base_seconds: float = 0.05
    cap_seconds: float = 1.0

    def __post_init__(self) -> None:
        if isinstance(self.attempts, bool) or not isinstance(self.attempts, int):
            raise ValidationError("retry attempts must be an integer >= 1")
        if self.attempts < 1:
            raise ValidationError("retry attempts must be an integer >= 1")
        if self.base_seconds <= 0 or self.cap_seconds < self.base_seconds:
            raise ValidationError("retry backoff must be positive and bounded")

    def delay(self, attempt: int) -> float:
        """Deterministic capped backoff; attempt is 1-based."""

        return min(self.cap_seconds, self.base_seconds * (2 ** (attempt - 1)))


@dataclass(eq=False)
class HttpResponse:
    """A reply already captured on disk; decoding reads the file, not a pipe.

    A capture is a private temporary file. ``release`` deletes it and may be
    called any number of times, so a caller can free a reply in a ``finally``
    without knowing whether an earlier step already did.
    """

    status: int
    path: str
    body_path: Path
    body_bytes: int
    released: bool = field(default=False)

    @property
    def raw_ref(self) -> str:
        return str(self.body_path)

    def release(self) -> None:
        """Delete the capture; idempotent and safe after a partial failure."""

        if self.released:
            return
        self.released = True
        try:
            self.body_path.unlink(missing_ok=True)
        except OSError:
            pass

    def __enter__(self) -> "HttpResponse":
        return self

    def __exit__(self, *_exception: object) -> None:
        self.release()

    def _readable(self) -> None:
        if self.released:
            raise ValidationError(f"captured reply for {self.path} was already released")

    def text(self) -> str:
        self._readable()
        try:
            return self.body_path.read_text(encoding="utf-8")
        except OSError as error:
            raise ValidationError(f"cannot read captured reply {self.body_path}: {error}") from error

    def json(self) -> object:
        """Decode the capture; a 204 carries no body and decodes to None."""

        self._readable()
        if self.status == NO_CONTENT:
            if self.body_bytes:
                raise ValidationError(
                    f"opencode service sent {self.body_bytes} bytes with a 204 for {self.path}"
                )
            return None
        try:
            with self.body_path.open("rb") as stream:
                return json.load(stream)
        except OSError as error:
            raise ValidationError(f"cannot read captured reply {self.body_path}: {error}") from error
        except ValueError as error:
            raise ValidationError(
                f"opencode service reply for {self.path} is not valid JSON"
            ) from error

    def mapping(self) -> Mapping[str, object]:
        payload = self.json()
        if payload is None and self.status == NO_CONTENT:
            return {}
        if not isinstance(payload, dict):
            raise ValidationError(f"opencode service reply for {self.path} must be a JSON object")
        return payload


def _check_path(path: str) -> str:
    if not isinstance(path, str) or not path.startswith("/"):
        raise ValidationError("opencode request path must start with '/'")
    split = urlsplit(path)
    segments = [segment.lower() for segment in split.path.split("/") if segment]
    keys = {
        item.split("=", 1)[0].lower()
        for item in split.query.split("&")
        if item
    }
    if _WAIT_SEGMENTS.intersection(segments) or _WAIT_QUERY_KEYS.intersection(keys):
        raise ValidationError(
            f"opencode long wait endpoint is refused: {path}; poll message state instead"
        )
    return path


def _capture(
    response: object, directory: Path, status: int, path: str, max_bytes: int
) -> HttpResponse:
    descriptor, name = tempfile.mkstemp(dir=str(directory), prefix="reply.", suffix=".json")
    body_path = Path(name)
    written = 0
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as sink:
            descriptor = -1
            while True:
                chunk = response.read(CHUNK_BYTES)
                if not chunk:
                    break
                written += len(chunk)
                if written > max_bytes:
                    raise ValidationError(
                        f"opencode reply for {path} exceeds {max_bytes} bytes; "
                        "refusing to decode a partial body"
                    )
                sink.write(chunk)
            sink.flush()
            os.fsync(sink.fileno())
    except BaseException:
        if descriptor >= 0:
            os.close(descriptor)
        body_path.unlink(missing_ok=True)
        raise
    return HttpResponse(status=status, path=path, body_path=body_path, body_bytes=written)


class OpenCodeHttpClient:
    """Bounded client for one proven-isolated managed service."""

    def __init__(
        self,
        base_url: str,
        response_dir: str | Path,
        *,
        opener: Callable[..., object] | None = None,
        retry: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        timeout_seconds: float = REQUEST_TIMEOUT_SECONDS,
        max_bytes: int = MAX_RESPONSE_BYTES,
        password: str | None = None,
    ) -> None:
        self._base_url = _check_base_url(base_url)
        self._response_dir = _response_dir(response_dir)
        candidate = os.environ.get(PASSWORD_ENV) if password is None else password
        if candidate is None and opener is not None:
            self._authorization: str | None = None
        else:
            checked = require_server_password(candidate)
            encoded = base64.b64encode(f"opencode:{checked}".encode("utf-8")).decode("ascii")
            self._authorization = f"Basic {encoded}"
        self._opener = opener if opener is not None else urllib.request.urlopen
        self._retry = retry if retry is not None else RetryPolicy()
        self._sleep = sleep
        self._monotonic = monotonic
        self._timeout = float(timeout_seconds)
        self._max_bytes = int(max_bytes)
        self.attempts = 0

    @classmethod
    def for_descriptor(
        cls, descriptor: ServiceDescriptor, response_dir: str | Path, **kwargs: object
    ) -> "OpenCodeHttpClient":
        return cls(descriptor.base_url, response_dir, **kwargs)  # type: ignore[arg-type]

    @property
    def base_url(self) -> str:
        return self._base_url

    def request(
        self,
        method: str,
        path: str,
        payload: Mapping[str, object] | None = None,
        *,
        retry: bool | None = None,
    ) -> HttpResponse:
        """Perform one request; retry only proven-transient idempotent failures."""

        checked = _check_path(path)
        verb = method.upper()
        allow_retry = verb in _RETRYABLE_METHODS if retry is None else bool(retry)
        attempts = self._retry.attempts if allow_retry else 1
        last: HttpError | None = None
        for attempt in range(1, attempts + 1):
            self.attempts += 1
            try:
                return self._once(verb, checked, payload)
            except TransientHttpError as error:
                last = error
                if attempt == attempts:
                    break
                self._sleep(self._retry.delay(attempt))
        assert last is not None
        raise last

    def _once(
        self, verb: str, path: str, payload: Mapping[str, object] | None
    ) -> HttpResponse:
        body = None if payload is None else json.dumps(payload).encode("utf-8")
        request = urllib.request.Request(self._base_url + path, data=body, method=verb)
        request.add_header("Accept", "application/json")
        if self._authorization is not None:
            request.add_header("Authorization", self._authorization)
        if body is not None:
            request.add_header("Content-Type", "application/json")
        try:
            response = self._opener(request, timeout=self._timeout)
        except urllib.error.HTTPError as error:
            status = int(error.code)
            if status in TRANSIENT_STATUSES:
                raise TransientHttpError(status, path) from error
            detail = error.reason if isinstance(error.reason, str) else ""
            raise HttpError(status, path, self._safe_detail(status, detail)) from error
        except (urllib.error.URLError, OSError) as error:
            raise TransientHttpError(503, path, str(error)) from error
        try:
            status = int(getattr(response, "status", None) or response.getcode())
            captured = _capture(response, self._response_dir, status, path, self._max_bytes)
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()
        # A failed capture is never kept: only a final transcript survives a run.
        if status in TRANSIENT_STATUSES:
            captured.release()
            raise TransientHttpError(status, path)
        if status >= 400:
            try:
                detail = captured.text()[:200]
            finally:
                captured.release()
            raise HttpError(status, path, self._safe_detail(status, detail))
        return captured

    def _safe_detail(self, status: int, detail: str) -> str:
        if status in _AUTH_STATUSES:
            return "authentication failed"
        if self._authorization is not None and self._authorization in detail:
            return "response detail redacted"
        return detail

    def get(self, path: str) -> HttpResponse:
        return self.request("GET", path)

    def post(
        self, path: str, payload: Mapping[str, object] | None = None, *, retry: bool = False
    ) -> HttpResponse:
        return self.request("POST", path, payload, retry=retry)

    def _decoded(self, response: HttpResponse) -> Mapping[str, object]:
        """Decode and immediately drop a capture nobody will reference again."""

        try:
            return response.mapping()
        finally:
            response.release()

    def poll(
        self,
        path: str,
        ready: Callable[[Mapping[str, object]], bool],
        *,
        deadline_seconds: float,
        interval_seconds: float = POLL_INTERVAL_SECONDS,
    ) -> Mapping[str, object]:
        """Poll one endpoint with short bounded sleeps until ``ready`` accepts."""

        if interval_seconds <= 0 or interval_seconds > MAX_POLL_INTERVAL_SECONDS:
            raise ValidationError(
                f"opencode poll interval must be in (0, {MAX_POLL_INTERVAL_SECONDS}] seconds"
            )
        if deadline_seconds <= 0:
            raise ValidationError("opencode poll deadline must be positive")
        started = self._monotonic()
        while True:
            payload = self._decoded(self.get(path))
            if ready(payload):
                return payload
            if self._monotonic() - started >= deadline_seconds:
                raise PollTimeout(
                    f"opencode state at {path} did not settle within {deadline_seconds} seconds"
                )
            self._sleep(interval_seconds)

    # --- service and session endpoints -----------------------------------

    def isolation_report(self) -> Mapping[str, object]:
        return self._decoded(self.get(CONFIG_PATH))

    def health(self) -> Mapping[str, object]:
        return self._decoded(self.get(HEALTH_PATH))

    def providers(self) -> Mapping[str, object]:
        return self._decoded(self.get(PROVIDERS_PATH))

    def session_status(self) -> Mapping[str, object]:
        """The whole service's status map, keyed by session id."""

        return self._decoded(self.get(SESSION_STATUS_PATH))

    def create_session(self, payload: Mapping[str, object]) -> Mapping[str, object]:
        return self._decoded(self.post(SESSION_PATH, payload))

    def prompt_async(self, session_id: str, payload: Mapping[str, object]) -> Mapping[str, object]:
        # A prompt is never retried: a failed model turn is ambiguous.
        return self._decoded(self.post(f"{SESSION_PATH}/{_session(session_id)}/prompt_async", payload))

    def abort(self, session_id: str) -> Mapping[str, object]:
        return self._decoded(self.post(f"{SESSION_PATH}/{_session(session_id)}/abort"))

    def messages(self, session_id: str) -> HttpResponse:
        """The final transcript capture; the caller owns and releases it."""

        return self.get(f"{SESSION_PATH}/{_session(session_id)}/message")

    def permissions(self, session_id: str) -> HttpResponse:
        return self.get(f"{SESSION_PATH}/{_session(session_id)}/permission")

    def answer_permission(
        self, session_id: str, permission_id: str, payload: Mapping[str, object]
    ) -> Mapping[str, object]:
        path = (
            f"{SESSION_PATH}/{_session(session_id)}/permissions/{_session(permission_id)}"
        )
        return self._decoded(self.post(path, payload))


def _session(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValidationError("opencode session identifier must be a nonblank string")
    if any(character in value for character in "/?#%"):
        raise ValidationError(f"opencode identifier must not contain path syntax: {value!r}")
    return value


def _check_base_url(base_url: str) -> str:
    if not isinstance(base_url, str):
        raise ValidationError("opencode base url must be a string")
    split = urlsplit(base_url)
    if split.scheme != "http" or split.hostname != SERVICE_HOST or not split.port:
        raise ValidationError(
            f"opencode base url must be a private loopback endpoint, not {base_url!r}"
        )
    if split.path.rstrip("/") or split.query or split.fragment:
        raise ValidationError(f"opencode base url must have no path or query: {base_url!r}")
    return f"http://{split.hostname}:{split.port}"


def _response_dir(value: str | Path) -> Path:
    try:
        path = Path(value)
    except TypeError as error:
        raise ValidationError("response directory must be an absolute existing directory") from error
    if not path.is_absolute() or not path.is_dir():
        raise ValidationError(f"response directory must be an absolute existing directory: {path}")
    return path
