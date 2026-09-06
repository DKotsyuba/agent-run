"""Small TOML literal renderers used by the isolated Codex config writer."""


def toml_string(value: str) -> str:
    """Return ``value`` as an escaped TOML basic string."""
    escaped = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def toml_array(values) -> str:
    """Return values as a TOML basic-string array in their input order."""
    return "[" + ", ".join(toml_string(value) for value in values) + "]"
