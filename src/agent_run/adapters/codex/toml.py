"""Small TOML literal renderers used by the isolated Codex config writer."""

from __future__ import annotations

import math
import tomllib
from importlib.resources import files
from typing import Mapping

from ...errors import ValidationError

#: Basic-string short escapes; every other control character uses ``\\uXXXX``
#: because raw control bytes are invalid inside TOML basic strings.
_STRING_ESCAPES = {
    "\\": "\\\\",
    '"': '\\"',
    "\b": "\\b",
    "\t": "\\t",
    "\n": "\\n",
    "\f": "\\f",
    "\r": "\\r",
}


def toml_string(value: str) -> str:
    """Return ``value`` as an escaped TOML basic string.

    Backslash, quote, and whitespace control characters are escaped and all
    other control characters become ``\\uXXXX`` so any accepted string
    round-trips through ``tomllib`` unchanged.
    """

    rendered = []
    for character in str(value):
        escape = _STRING_ESCAPES.get(character)
        if escape is not None:
            rendered.append(escape)
        elif character < " " or character == "\x7f":
            rendered.append(f"\\u{ord(character):04X}")
        else:
            rendered.append(character)
    return '"' + "".join(rendered) + '"'


def toml_array(values) -> str:
    """Return values as a TOML basic-string array in their input order."""
    return "[" + ", ".join(toml_string(value) for value in values) + "]"


def toml_value(value: object) -> str:
    """Return one validated native setting as an inline TOML value.

    ``value`` must be a validated settings node: boolean, integer, finite
    float, string, array, or table. Booleans are checked before integers
    because ``bool`` subclasses ``int``; any other object raises
    ``ValidationError`` rather than emitting invalid TOML.
    """

    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValidationError("native settings floats must be finite")
        return repr(value)
    if isinstance(value, str):
        return toml_string(value)
    if isinstance(value, (list, tuple)):
        return "[" + ", ".join(toml_value(item) for item in value) + "]"
    if isinstance(value, Mapping):
        body = ", ".join(f"{key} = {toml_value(item)}" for key, item in value.items())
        return "{ " + body + " }"
    raise ValidationError(
        f"native settings values must be scalars, arrays, or tables, not {type(value).__name__}"
    )


def render_native_settings(settings: Mapping[str, object]) -> list[str]:
    """Return validated settings as ordered top-level TOML lines.

    Every setting renders as one ``key = <inline value>`` line; tables become
    inline tables. Emitting no ``[section]`` headers is load-bearing: the
    caller appends its own permission/hook/plugin sections after these lines,
    and a header here would silently capture those agent-run-owned keys into
    the operator's table. Keys are already plain identifiers (validation
    rejects dots), so inline tables cannot splice namespaces either.
    """

    return [f"{key} = {toml_value(value)}" for key, value in settings.items()]


def merge_native_settings(
    defaults: Mapping[str, object], declared: Mapping[str, object]
) -> dict[str, object]:
    """Deep-merge declared settings over packaged defaults.

    Tables merge recursively so a declared sub-key keeps default siblings;
    every other declared value replaces the default wholesale. Key order
    keeps defaults first and appends new declared keys in declaration order,
    which keeps the generated document deterministic for a given config.
    """

    merged: dict[str, object] = dict(defaults)
    for key, value in declared.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = merge_native_settings(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_native_defaults() -> dict[str, object]:
    """Return the packaged Codex default settings as a mutable table.

    Reads the packaged ``defaults.toml`` resource each call so tests and
    concurrent launches never share mutable state; the file is first-party
    data whose scalar shape mirrors the validated settings contract.
    """

    return dict(
        tomllib.loads(
            files("agent_run.adapters.codex")
            .joinpath("defaults.toml")
            .read_text(encoding="utf-8")
        )
    )


def render_effective_native_settings(
    declared: Mapping[str, object],
) -> list[str]:
    """Return packaged defaults deep-merged with ``declared`` as TOML lines.

    Composition entry point for the adapter: loads defaults, merges the
    operator's declared tree over them, and renders the result inline-only
    (see :func:`render_native_settings` for why no section headers appear).
    """

    return render_native_settings(merge_native_settings(load_native_defaults(), declared))
