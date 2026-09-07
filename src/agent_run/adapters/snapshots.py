"""Immutable managed-tree publication and recovery inspection."""

from __future__ import annotations

import hashlib
import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from ..config import EnvironmentConfig, RuntimeConfig
from ..errors import PathEscapeError, ValidationError
from ..profiles import AgentProfile
from .home import (
    MANAGED_TEMP_PREFIX,
    content_hash,
    ensure_managed_directory,
    write_managed_file,
)

SNAPSHOT_MANIFEST = ".agent-run-snapshot.json"
"""Metadata filename published after a managed snapshot's content."""

CONFIG_SNAPSHOT_FILENAME = "config-snapshot.json"
"""Attempt-relative filename for effective configuration evidence."""


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


@dataclass(frozen=True, slots=True)
class ConfigSnapshot:
    """Canonical credential-free effective configuration bytes and SHA-256."""

    document: bytes
    sha256: str


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


def _environment_document(environment: EnvironmentConfig | None) -> object:
    """Return deterministic environment evidence without raw variable values."""

    if environment is None:
        return None
    return {
        "path": [str(path) for path in environment.path],
        "variable_sha256": {
            name: content_hash(value) for name, value in sorted(environment.variables.items())
        },
        "required_commands": list(environment.required_commands),
        "denied_commands": list(environment.denied_commands),
        "rust": None
        if environment.rust is None
        else [str(environment.rust.rustup_home), str(environment.rust.cargo_bin)],
    }


def _runtime_document(config: RuntimeConfig) -> dict[str, object]:
    """Return complete deterministic runtime declarations without credential bytes."""

    auth = None
    if config.auth is not None:
        auth = {
            "kind": config.auth.kind,
            "source": None if config.auth.source is None else str(config.auth.source),
            "target": config.auth.target,
            "names": list(config.auth.names),
        }
    return {
        "enabled": config.enabled,
        "adapter": config.adapter,
        "binary": str(config.binary),
        "home": str(config.home),
        "models": list(config.models),
        "skills": list(config.skills),
        "mcp": list(config.mcp),
        "max_active_agents": config.max_active_agents,
        "auth": auth,
        "hooks": [[hook.event, list(hook.command), hook.matcher] for hook in config.hooks],
        "plugins": [str(path) for path in config.plugins],
        "limits_source": config.limits_source,
        "accounts": list(config.accounts),
        "default_account": config.default_account,
        "priority_multiplier": config.priority_multiplier,
        "priority_account_multipliers": dict(sorted(config.priority_account_multipliers.items())),
        "priority_lane_multipliers": dict(sorted(config.priority_lane_multipliers.items())),
        "rust": None
        if config.rust is None
        else [str(config.rust.rustup_home), str(config.rust.cargo_bin)],
        "environment": _environment_document(config.environment),
    }


def _profile_document(profile: AgentProfile) -> dict[str, object]:
    """Return the effective profile bytes and every current grant field."""

    return {
        name: (
            [str(item) for item in value]
            if isinstance(value, tuple) and all(isinstance(item, Path) for item in value)
            else value
        )
        for name, value in vars(profile).items()
    }


def build_config_snapshot(
    *,
    runtime: str,
    adapter_api_version: int,
    schema_version: int,
    materialize_revision: str,
    config: RuntimeConfig,
    profile: AgentProfile,
) -> ConfigSnapshot:
    """Build canonical effective configuration evidence for one attempt.

    The document binds runtime, adapter/config versions, all runtime declarations
    through a credential-free hash, the materialized-file revision, and the
    effective profile body and grants. Environment values are hashed rather than
    stored. Identical inputs yield identical bytes; content-only changes alter
    the returned SHA-256.
    """

    if not isinstance(runtime, str) or not runtime.strip():
        raise ValidationError("snapshot runtime must be nonblank")
    if type(adapter_api_version) is not int or adapter_api_version < 1:
        raise ValidationError("snapshot adapter_api_version must be positive")
    if type(schema_version) is not int or schema_version < 1:
        raise ValidationError("snapshot schema_version must be positive")
    if not isinstance(materialize_revision, str) or not materialize_revision.strip():
        raise ValidationError("snapshot materialize_revision must be nonblank")
    if not isinstance(config, RuntimeConfig) or not isinstance(profile, AgentProfile):
        raise ValidationError("snapshot requires RuntimeConfig and AgentProfile")
    runtime_bytes = json.dumps(
        _runtime_document(config), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    document = (
        json.dumps(
            {
                "snapshot_version": 1,
                "runtime": runtime,
                "adapter_api_version": adapter_api_version,
                "config_schema_version": schema_version,
                "runtime_config_sha256": content_hash(runtime_bytes),
                "materialize_revision": materialize_revision,
                "profile": _profile_document(profile),
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )
    return ConfigSnapshot(document, content_hash(document))
