"""Bounded, secret-safe stderr capture for Claude-family runtime children."""

from __future__ import annotations

from collections.abc import Iterable
from typing import TextIO

from .stream import sanitize_line

_DEFAULT_TAIL_BYTES = 4096


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
        stream: TextIO | None,
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
        """Read stderr to EOF and retain only the bounded redacted tail."""

        if self._stream is None:
            return
        try:
            for raw_line in self._stream:
                self._tail.extend(
                    sanitize_line(raw_line, self._secrets).encode(
                        "utf-8", errors="replace"
                    )
                )
                if len(self._tail) > self._limit_bytes:
                    del self._tail[:-self._limit_bytes]
        except (OSError, ValueError):
            return

    def text(self) -> str | None:
        """Return the stripped redacted stderr tail, or ``None`` when empty."""

        text = self._tail.decode("utf-8", errors="replace").strip()
        return text or None
