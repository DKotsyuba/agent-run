"""Best-effort host-session title and working-directory resolution."""

from __future__ import annotations

import json
import re
from pathlib import Path


class TitleResolver:
    """Resolve session metadata from append-only local Claude and Codex indexes."""

    def __init__(self, claude_home: Path | None = None, codex_home: Path | None = None) -> None:
        """Set optional homes, defaulting to the current user's runtime homes."""
        self.claude_home = claude_home or Path.home() / ".claude"
        self.codex_home = codex_home or Path.home() / ".codex"
        self._positions: dict[str, tuple[int, int]] = {}
        self._records_by_path: dict[str, list[dict]] = {}

    def resolve(self, transport: str, external_session_id: str) -> tuple[str, str | None]:
        """Return ``(title, cwd)`` or a safe fallback without unsafe path access.

        Session ids are validated before they are interpolated into glob paths.
        Metadata files are append-only: unchanged bytes are never re-read,
        growth is parsed from its saved offset, and a shrink restarts parsing.
        """
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", external_session_id) or ".." in external_session_id:
            return external_session_id[:8] or "unbound", None
        files = self._files(transport, external_session_id)
        if transport == "claude_uds":
            result = self._claude(external_session_id, files)
        elif transport == "codex_queue":
            result = self._codex(external_session_id, files)
        else:
            result = (external_session_id[:8] or "unbound", None)
        return result

    def _files(self, transport: str, external_id: str) -> list[Path]:
        """Return relevant candidate files whose mtimes invalidate cached metadata."""
        if transport == "claude_uds":
            return list(self.claude_home.glob(f"projects/*/{external_id}.jsonl")) + [self.claude_home / "history.jsonl"]
        if transport == "codex_queue":
            return [self.codex_home / "session_index.jsonl"] + list(self.codex_home.glob(f"sessions/**/rollout-*-{external_id}.jsonl"))
        return []

    def _records(self, path: Path):
        """Yield cached records, parsing only appended bytes up to 64 KiB per line."""
        key = str(path)
        try:
            size = path.stat().st_size
            offset, previous_size = self._positions.get(key, (0, 0))
            if size < previous_size:
                offset = 0
                self._records_by_path[key] = []
            records = self._records_by_path.setdefault(key, [])
            if size > offset:
                with path.open(encoding="utf-8") as stream:
                    stream.seek(offset)
                    for line in stream:
                        if len(line.encode("utf-8")) > 65536:
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if isinstance(record, dict):
                            records.append(record)
                offset = size
            self._positions[key] = (offset, size)
            for record in records:
                yield record
        except OSError:
            return

    def _claude(self, external_id: str, files: list[Path]) -> tuple[str, str | None]:
        """Read last Claude custom title and cwd, then the history fallback."""
        title, cwd = None, None
        for path in files[:-1]:
            for record in self._records(path):
                cwd = record["cwd"] if isinstance(record.get("cwd"), str) else cwd
                if record.get("type") == "custom-title" and isinstance(record.get("customTitle"), str):
                    title = record["customTitle"]
        if title is None:
            for record in self._records(self.claude_home / "history.jsonl"):
                if record.get("sessionId") == external_id and isinstance(record.get("display"), str):
                    title = record["display"]
                    break
        return title or external_id[:8] or "unbound", cwd

    def _codex(self, external_id: str, files: list[Path]) -> tuple[str, str | None]:
        """Read a Codex index title and optional rollout metadata working directory."""
        title, cwd = None, None
        for record in self._records(self.codex_home / "session_index.jsonl"):
            if record.get("id") == external_id and isinstance(record.get("thread_name"), str):
                title = record["thread_name"]
                break
        for path in files[1:]:
            for record in self._records(path):
                payload = record.get("payload")
                if record.get("type") == "session_meta" and isinstance(payload, dict) and isinstance(payload.get("cwd"), str):
                    cwd = payload["cwd"]
                    break
            if cwd:
                break
        return title or external_id[:8] or "unbound", cwd
