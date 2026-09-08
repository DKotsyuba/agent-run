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

RUNTIME_SNAPSHOT_INDEX = ".agent-run-snapshots.json"
"""Generated-home index of every managed snapshot root required on resume."""

_MAX_SNAPSHOT_METADATA_BYTES = 64 * 1024
"""Independent read bound for runtime/config snapshot metadata."""


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
    type_mismatches: tuple[str, ...] = ()
    hash_mismatches: tuple[str, ...] = ()


def tree_revision(source: Path) -> str:
    """Return a no-follow canonical content revision for one source tree."""

    entries, _files = _read_tree(source)
    return content_hash(_manifest(entries))


def _relative(value: str | Path, label: str) -> Path:
    """Return a nonempty managed relative path or raise ``PathEscapeError``."""

    try:
        path = Path(value)
    except TypeError as error:
        raise PathEscapeError(f"{label} must be a relative path") from error
    if path.is_absolute() or not path.parts or ".." in path.parts or path == Path("."):
        raise PathEscapeError(f"{label} must be a relative path without '..'")
    return path


def _read_tree(
    source: Path,
    selected: tuple[PurePosixPath, ...] | None = None,
    *,
    allow_special: bool = False,
) -> tuple[list[dict[str, object]], dict[str, bytes]]:
    """Read path/type/content evidence through no-follow descriptors.

    ``source`` must be an absolute real directory. ``selected`` optionally names
    the exact relative files/directories to traverse; their parent directories
    are retained for a self-contained layout. Selected descendants may only be
    real directories or regular files, so links and special files fail closed.
    ``allow_special`` is inspection-only: it records an unexpected symlink or
    special entry's type without following or reading it.
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
            included = selected is None or any(
                relative == item or item in relative.parents for item in selected
            )
            traversed = included or (
                selected is not None and any(relative in item.parents for item in selected)
            )
            if not traversed:
                continue
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
                if allow_special:
                    entries.append(
                        {
                            "path": portable,
                            "type": "symlink"
                            if stat.S_ISLNK(metadata.st_mode)
                            else "special",
                        }
                    )
                    continue
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
            mode = 0o700 if current.st_mode & 0o111 else 0o600
            entries.append(
                {
                    "path": portable,
                    "type": "file",
                    "mode": mode,
                    "bytes": len(payload),
                    "sha256": hashlib.sha256(payload).hexdigest(),
                }
            )

    try:
        walk(root, PurePosixPath())
    finally:
        os.close(root)
    entries.sort(key=lambda entry: str(entry["path"]))
    if selected is not None:
        found = {PurePosixPath(str(entry["path"])) for entry in entries}
        missing = sorted(item.as_posix() for item in selected if item not in found)
        if missing:
            raise ValidationError(f"snapshot assets are missing: {', '.join(missing)}")
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
        with os.fdopen(descriptor, "rb") as stream:
            metadata = os.fstat(stream.fileno())
            if not stat.S_ISREG(metadata.st_mode):
                raise ValidationError("snapshot manifest must be a regular file")
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


def _read_metadata(directory: Path, name: str) -> bytes:
    """Read one no-follow regular metadata file relative to an owned directory."""

    directory_fd = os.open(
        directory, os.O_RDONLY | os.O_CLOEXEC | os.O_DIRECTORY | os.O_NOFOLLOW
    )
    try:
        descriptor = os.open(
            name, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW, dir_fd=directory_fd
        )
    finally:
        os.close(directory_fd)
    with os.fdopen(descriptor, "rb") as stream:
        metadata = os.fstat(stream.fileno())
        if not stat.S_ISREG(metadata.st_mode):
            raise ValidationError(f"{name} must be a regular file")
        if metadata.st_size > _MAX_SNAPSHOT_METADATA_BYTES:
            raise ValidationError(f"{name} exceeds the snapshot metadata bound")
        payload = stream.read(_MAX_SNAPSHOT_METADATA_BYTES + 1)
        if len(payload) > _MAX_SNAPSHOT_METADATA_BYTES:
            raise ValidationError(f"{name} exceeds the snapshot metadata bound")
        return payload


def _register_snapshot(home: Path, relative: Path) -> None:
    """Durably add one canonical managed root to the runtime snapshot index."""

    try:
        raw = _read_metadata(home, RUNTIME_SNAPSHOT_INDEX)
    except FileNotFoundError:
        roots: list[str] = []
    else:
        try:
            document = json.loads(raw)
            roots = document["roots"]
        except (UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as error:
            raise ValidationError("runtime snapshot index is malformed") from error
        if document.get("snapshot_index_version") != 1 or not isinstance(roots, list):
            raise ValidationError("runtime snapshot index is malformed")
    root = relative.as_posix()
    if root not in roots:
        roots.append(root)
    payload = (
        json.dumps(
            {"snapshot_index_version": 1, "roots": sorted(roots)},
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    write_managed_file(home, RUNTIME_SNAPSHOT_INDEX, payload)


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
    actual_entries, _ = _read_tree(root, allow_special=True)
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
    shared = sorted(set(expected) & set(actual))
    type_mismatches = tuple(
        path for path in shared if expected[path].get("type") != actual[path].get("type")
    )
    hash_mismatches = tuple(
        path
        for path in shared
        if path not in type_mismatches and expected[path] != actual[path]
    )
    mismatched = tuple(sorted((*type_mismatches, *hash_mismatches)))
    temps = tuple(sorted(owned_temps))
    return SnapshotInspection(
        not (temps or missing or orphans or mismatched),
        temps,
        orphans,
        missing,
        mismatched,
        type_mismatches,
        hash_mismatches,
    )


def _publish_snapshot(
    home: Path,
    relative: Path,
    entries: list[dict[str, object]],
    files: dict[str, bytes],
) -> TreeSnapshot:
    """Publish prepared snapshot entries and canonical metadata without deletion."""

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
    by_path = _entry_map(entries)
    for path, payload in sorted(files.items()):
        write_managed_file(
            home, relative / path, payload, mode=int(by_path[path]["mode"])
        )
    document = _manifest(entries)
    write_managed_file(home, relative / SNAPSHOT_MANIFEST, document)
    _register_snapshot(Path(home), relative)
    return TreeSnapshot(
        content_hash(document),
        root / SNAPSHOT_MANIFEST,
        tuple(str(entry["path"]) for entry in entries),
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

    return _publish_snapshot(
        home,
        _relative(relative_root, "snapshot destination"),
        *_read_tree(source),
    )


def snapshot_selected_assets(
    home: Path,
    relative_root: str | Path,
    source: Path,
    assets: tuple[str, ...],
) -> TreeSnapshot:
    """Publish only explicitly declared non-secret plugin assets.

    ``assets`` contains unique relative POSIX files or directories beneath the
    configured ``source`` plugin root. No globs, discovery, import tracing, or
    filename denylist is applied: the trusted declaration owns completeness and
    secrecy. Containment, presence, no-follow types, and bytes are validated by
    the same descriptor and manifest path as full-tree snapshots.
    """

    selected: list[PurePosixPath] = []
    for value in assets:
        if not isinstance(value, str) or not value or "\0" in value:
            raise ValidationError("plugin snapshot assets must be nonempty relative paths")
        path = PurePosixPath(value)
        if path.is_absolute() or "." in path.parts or ".." in path.parts:
            raise PathEscapeError(f"plugin snapshot asset escapes its root: {value}")
        if any(character in value for character in "*?[]{}"):
            raise ValidationError(f"plugin snapshot asset must not use glob syntax: {value}")
        if path in selected:
            raise ValidationError(f"plugin snapshot asset is duplicated: {value}")
        selected.append(path)
    if not selected:
        raise ValidationError("plugin snapshot assets must not be empty")
    return _publish_snapshot(
        home,
        _relative(relative_root, "snapshot destination"),
        *_read_tree(source, tuple(selected)),
    )
