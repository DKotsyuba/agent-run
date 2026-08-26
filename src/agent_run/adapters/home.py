"""Safe writes beneath an adapter's generated home."""

from __future__ import annotations

import hashlib
import os
import secrets
import tempfile
from pathlib import Path

from ..errors import PathEscapeError, ValidationError
from ..verify import DEFAULT_SENTINEL


def content_hash(content: str | bytes) -> str:
    data = content.encode("utf-8") if isinstance(content, str) else content
    if not isinstance(data, bytes):
        raise ValidationError("managed content must be str or bytes")
    return hashlib.sha256(data).hexdigest()


def _root(home: str | Path) -> Path:
    try:
        path = Path(home).expanduser()
    except (TypeError, RuntimeError) as error:
        raise ValidationError("generated home must be an absolute path") from error
    if not path.is_absolute():
        raise ValidationError("generated home must be an absolute path")
    if path.is_symlink():
        raise PathEscapeError(f"generated home must not be a symlink: {path}")
    try:
        path.mkdir(mode=0o700, parents=True, exist_ok=True)
        root = path.resolve(strict=True)
    except OSError as error:
        raise ValidationError(f"cannot create generated home {path}: {error}") from error
    if not root.is_dir():
        raise ValidationError(f"generated home is not a directory: {root}")
    return root


def _managed_path(home: str | Path, relative_path: str | Path) -> Path:
    root = _root(home)
    try:
        relative = Path(relative_path)
    except TypeError as error:
        raise PathEscapeError(f"invalid managed path: {relative_path!r}") from error
    if relative.is_absolute() or not relative.parts or ".." in relative.parts:
        raise PathEscapeError(f"managed path escapes generated home: {relative_path}")
    parent = root
    for part in relative.parts[:-1]:
        parent /= part
        if parent.is_symlink():
            raise PathEscapeError(f"managed path crosses a symlink: {relative_path}")
        try:
            parent.mkdir(mode=0o700, exist_ok=True)
        except OSError as error:
            raise ValidationError(f"cannot create managed directory {parent}: {error}") from error
        if not parent.resolve(strict=True).is_relative_to(root):
            raise PathEscapeError(f"managed path escapes generated home: {relative_path}")
    candidate = parent / relative.name
    if not candidate.parent.resolve(strict=True).is_relative_to(root):
        raise PathEscapeError(f"managed path escapes generated home: {relative_path}")
    return candidate


def write_managed_file(
    home: str | Path, relative_path: str | Path, content: str | bytes
) -> str:
    """Atomically replace one private regular file and return its SHA-256."""

    data = content.encode("utf-8") if isinstance(content, str) else content
    digest = content_hash(data)
    candidate = _managed_path(home, relative_path)
    if candidate.is_symlink():
        raise PathEscapeError(f"managed file must not replace a symlink: {relative_path}")
    if candidate.exists() and not candidate.is_file():
        raise ValidationError(f"managed path is not a file: {relative_path}")
    descriptor, temporary_name = tempfile.mkstemp(
        dir=candidate.parent, prefix=f".{candidate.name}.", suffix=".tmp"
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            descriptor = -1
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, candidate)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary.unlink(missing_ok=True)
    return digest


def seal_answer(path: Path, text: str) -> tuple[int, str]:
    if not isinstance(path, Path) or not path.is_absolute():
        raise ValidationError("answer path must be absolute")
    if not isinstance(text, str) or not text.strip():
        raise ValidationError("answer text must be nonblank")
    separator = "" if text.endswith("\n") else "\n"
    data = f"{text}{separator}{DEFAULT_SENTINEL}\n".encode("utf-8")
    digest = write_managed_file(path.parent, path.name, data)
    return len(data), digest


def create_symlink_bridge(
    home: str | Path, relative_path: str | Path, source: str | Path
) -> Path:
    """Atomically create an explicit bridge at a validated location."""

    try:
        source_path = Path(source).expanduser()
    except (TypeError, RuntimeError) as error:
        raise ValidationError("symlink bridge source must be an absolute existing path") from error
    if not source_path.is_absolute():
        raise ValidationError("symlink bridge source must be an absolute existing path")
    try:
        source_path = source_path.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ValidationError("symlink bridge source must be an absolute existing path") from error
    candidate = _managed_path(home, relative_path)
    if candidate.exists() and not candidate.is_symlink():
        raise ValidationError(f"symlink bridge would replace a managed file: {relative_path}")
    temporary = candidate.parent / f".{candidate.name}.{secrets.token_hex(8)}.link.tmp"
    try:
        temporary.symlink_to(source_path, target_is_directory=source_path.is_dir())
        os.replace(temporary, candidate)
    finally:
        temporary.unlink(missing_ok=True)
    return candidate
