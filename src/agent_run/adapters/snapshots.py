"""Immutable managed-tree publication and recovery inspection."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ..errors import PathEscapeError, ValidationError
from .home import (
    MANAGED_TEMP_PREFIX,
    content_hash,
    ensure_managed_directory,
    write_managed_file,
)

SNAPSHOT_MANIFEST = ".agent-run-snapshot.json"
"""Metadata filename published after a managed snapshot's content."""


@dataclass(frozen=True, slots=True)
class TreeSnapshot:
    """Published tree identity: manifest hash, path, and canonical entry paths."""

    sha256: str
    manifest_path: Path
    entries: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SnapshotInspection:
    """Non-destructive recovery classification for one managed snapshot.

    Verification requires exact agreement with the manifest. Reserved publisher
    temporaries, unreferenced orphans, missing references, and type/content
    mismatches remain separate and inspection never removes them.
    """

    verified: bool
    owned_temps: tuple[str, ...]
    orphans: tuple[str, ...]
    referenced_missing: tuple[str, ...]
    mismatched: tuple[str, ...]


def _relative(value: str | Path, label: str) -> Path:
    """Return a nonempty managed relative path or raise ``PathEscapeError``."""

    try:
        path = Path(value)
    except TypeError as error:
        raise PathEscapeError(f"{label} must be a relative path") from error
    if path.is_absolute() or not path.parts or ".." in path.parts or path == Path("."):
        raise PathEscapeError(f"{label} must be a relative path without '..'")
    return path


def _read_tree(source: Path) -> tuple[list[dict[str, object]], dict[str, bytes]]:
    """Read path/type/content evidence through no-follow descriptors.

    ``source`` must be an absolute real directory. Descendants may only be real
    directories or regular files; symlinks and special files fail closed.
    """

    if not isinstance(source, Path) or not source.is_absolute():
        raise ValidationError("snapshot source must be an absolute directory")
    directory_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    try:
        root = os.open(source, directory_flags)
    except OSError as error:
        raise ValidationError(f"snapshot source must be a real directory: {source}") from error
    entries: list[dict[str, object]] = []
    files: dict[str, bytes] = {}

    def walk(descriptor: int, prefix: PurePosixPath) -> None:
        """Read one opened directory and recurse only through opened children."""

        try:
            names = sorted(os.listdir(descriptor))
        except OSError as error:
            raise ValidationError(f"cannot list snapshot source: {source}") from error
        for name in names:
            relative = prefix / name
            portable = relative.as_posix()
            try:
                metadata = os.stat(name, dir_fd=descriptor, follow_symlinks=False)
            except OSError as error:
                raise ValidationError(f"cannot inspect snapshot entry: {portable}") from error
            if stat.S_ISDIR(metadata.st_mode):
                try:
                    child = os.open(name, directory_flags, dir_fd=descriptor)
                except OSError as error:
                    raise ValidationError(f"snapshot directory must not be a symlink: {portable}") from error
                entries.append({"path": portable, "type": "directory"})
                try:
                    walk(child, relative)
                finally:
                    os.close(child)
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise ValidationError(f"snapshot entry must be regular: {portable}")
            try:
                opened = os.open(
                    name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=descriptor
                )
                try:
                    current = os.fstat(opened)
                    if not stat.S_ISREG(current.st_mode):
                        raise ValidationError(f"snapshot entry must be regular: {portable}")
                    with os.fdopen(opened, "rb") as stream:
                        payload = stream.read()
                except BaseException:
                    try:
                        os.close(opened)
                    except OSError:
                        pass
                    raise
            except OSError as error:
                raise ValidationError(f"cannot read snapshot file: {portable}") from error
            files[portable] = payload
            entries.append(
                {
                    "path": portable,
                    "type": "file",
                    "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            )

    try:
        walk(root, PurePosixPath())
    finally:
        os.close(root)
    entries.sort(key=lambda entry: str(entry["path"]))
    return entries, files


def _manifest(entries: list[dict[str, object]]) -> bytes:
    """Serialize version-one entries into canonical UTF-8 JSON bytes."""

    return (
        json.dumps(
            {"snapshot_version": 1, "entries": entries},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def _load_manifest(path: Path) -> list[dict[str, object]] | None:
    """Load one no-follow manifest, or return ``None`` when it is absent."""

    try:
        descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise ValidationError("snapshot manifest is unreadable") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise ValidationError("snapshot manifest must be a regular file")
        with os.fdopen(descriptor, "rb") as stream:
            raw = stream.read()
    except OSError as error:
        raise ValidationError("snapshot manifest is unreadable") from error
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValidationError("snapshot manifest is malformed") from error
    if not isinstance(document, dict) or document.get("snapshot_version") != 1:
        raise ValidationError("snapshot manifest version is unsupported")
    entries = document.get("entries")
    if not isinstance(entries, list) or any(not isinstance(entry, dict) for entry in entries):
        raise ValidationError("snapshot manifest entries are malformed")
    return entries


def _entry_map(entries: list[dict[str, object]]) -> dict[str, dict[str, object]]:
    """Validate manifest entries and return their unique path mapping."""

    mapped: dict[str, dict[str, object]] = {}
    for entry in entries:
        path = entry.get("path")
        kind = entry.get("type")
        if not isinstance(path, str) or not path or kind not in {"directory", "file"}:
            raise ValidationError("snapshot manifest entries are malformed")
        relative = PurePosixPath(path)
        if relative.is_absolute() or ".." in relative.parts or path == SNAPSHOT_MANIFEST:
            raise ValidationError("snapshot manifest path is invalid")
        if path in mapped:
            raise ValidationError("snapshot manifest path is duplicated")
        mapped[path] = entry
    return mapped


def inspect_managed_snapshot(home: Path, relative_root: str | Path) -> SnapshotInspection:
    """Classify snapshot recovery state without deleting or repairing paths."""

    root = Path(home) / _relative(relative_root, "snapshot destination")
    if not root.exists():
        return SnapshotInspection(False, (), (), (SNAPSHOT_MANIFEST,), ())
    if root.is_symlink() or not root.is_dir():
        raise ValidationError("snapshot destination must be a real directory")
    expected_entries = _load_manifest(root / SNAPSHOT_MANIFEST)
    actual_entries, _ = _read_tree(root)
    actual: dict[str, dict[str, object]] = {}
    owned_temps: list[str] = []
    for entry in actual_entries:
        path = str(entry["path"])
        name = PurePosixPath(path).name
        if path == SNAPSHOT_MANIFEST:
            continue
        if name.startswith(MANAGED_TEMP_PREFIX) and name.endswith(".tmp"):
            owned_temps.append(path)
        else:
            actual[path] = entry
    if expected_entries is None:
        return SnapshotInspection(
            False, tuple(sorted(owned_temps)), tuple(sorted(actual)), (SNAPSHOT_MANIFEST,), ()
        )
    expected = _entry_map(expected_entries)
    missing = tuple(sorted(set(expected) - set(actual)))
    orphans = tuple(sorted(set(actual) - set(expected)))
    mismatched = tuple(
        path for path in sorted(set(expected) & set(actual)) if expected[path] != actual[path]
    )
    temps = tuple(sorted(owned_temps))
    return SnapshotInspection(
        not (temps or missing or orphans or mismatched), temps, orphans, missing, mismatched
    )


def snapshot_managed_tree(
    home: Path, relative_root: str | Path, source: Path
) -> TreeSnapshot:
    """Publish a full tree and its canonical manifest last without deletion.

    A destination must be empty or a verified snapshot with identical topology;
    publication never deletes content. Source symlinks and special files are
    rejected. Every source directory and regular file is durably published before
    final metadata, so interruption remains unverified and recoverable.
    """

    relative = _relative(relative_root, "snapshot destination")
    entries, files = _read_tree(source)
    if any(str(entry["path"]) == SNAPSHOT_MANIFEST for entry in entries):
        raise ValidationError(f"snapshot source uses reserved name: {SNAPSHOT_MANIFEST}")
    root = ensure_managed_directory(home, relative)
    if any(root.iterdir()):
        inspection = inspect_managed_snapshot(Path(home), relative)
        previous = _load_manifest(root / SNAPSHOT_MANIFEST)
        if not inspection.verified or previous is None:
            raise ValidationError("existing managed snapshot requires recovery")
        old = _entry_map(previous)
        new = _entry_map(entries)
        if old.keys() != new.keys() or any(old[path]["type"] != new[path]["type"] for path in new):
            raise ValidationError("snapshot publication cannot change existing topology")
    for entry in entries:
        if entry["type"] == "directory":
            ensure_managed_directory(home, relative / str(entry["path"]))
    for path, payload in sorted(files.items()):
        write_managed_file(home, relative / path, payload)
    document = _manifest(entries)
    write_managed_file(home, relative / SNAPSHOT_MANIFEST, document)
    return TreeSnapshot(
        content_hash(document),
        root / SNAPSHOT_MANIFEST,
        tuple(str(entry["path"]) for entry in entries),
    )
