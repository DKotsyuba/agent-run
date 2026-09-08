"""Bounded, secret-safe stderr capture for Claude-family runtime children."""

from __future__ import annotations

from collections.abc import Iterable
from typing import IO

from .stream import sanitize_line

_DEFAULT_TAIL_BYTES = 4096
_READ_CHARS = 4096


class StderrTail:
    """Drain one text stream while retaining only its final redacted bytes.

    ``stream`` is the child stderr pipe, or ``None`` when unavailable. Every
    literal in ``secrets`` is redacted before retention. ``limit_bytes`` is a
    positive storage ceiling; :meth:`text` returns the stripped UTF-8 tail or
    ``None``. Call :meth:`drain` on one reader thread and :meth:`text` only
    after joining it. Read errors end capture without masking the child result.
    """

    def __init__(
        self,
        stream: IO[str] | None,
        secrets: Iterable[str],
        *,
        limit_bytes: int = _DEFAULT_TAIL_BYTES,
    ) -> None:
        """Create an empty capture for ``stream`` with a byte storage limit."""

        if isinstance(limit_bytes, bool) or not isinstance(limit_bytes, int) or limit_bytes < 1:
            raise ValueError("stderr tail limit must be a positive integer")
        self._stream = stream
        self._secrets = tuple(secrets)
        self._limit_bytes = limit_bytes
        self._tail = bytearray()

    def drain(self) -> None:
        """Read bounded chunks to EOF and retain only the redacted byte tail.

        Complete lines use structural JSON redaction. A newline-free suffix is
        capped to the output limit plus the longest literal-secret overlap, so a
        secret split across read boundaries is still replaced without buffering
        an unbounded child write.
        """

        if self._stream is None:
            return
        pending = ""
        overlap = max((len(secret) for secret in self._secrets), default=0)
        pending_limit = self._limit_bytes + overlap + _READ_CHARS
        try:
            while True:
                chunk = self._stream.read(_READ_CHARS)
                if not chunk:
                    break
                pending += chunk
                while "\n" in pending:
                    line, pending = pending.split("\n", 1)
                    self._retain(sanitize_line(line + "\n", self._secrets))
                if len(pending) > pending_limit:
                    pending = pending[-pending_limit:]
            if pending:
                self._retain(sanitize_line(pending, self._secrets))
        except (OSError, ValueError):
            return

    def _retain(self, text: str) -> None:
        """Append sanitized text while keeping at most the configured bytes."""

        self._tail.extend(text.encode("utf-8", errors="replace"))
        if len(self._tail) > self._limit_bytes:
            del self._tail[:-self._limit_bytes]

    def text(self) -> str | None:
        """Return the stripped redacted stderr tail, or ``None`` when empty."""

        text = self._tail.decode("utf-8", errors="replace").strip()
        return text or None
