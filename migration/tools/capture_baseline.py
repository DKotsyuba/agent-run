"""Regenerate the Python golden corpus for the Rust migration P0 stage.

Run this development-only script from the worktree root with ``PYTHONPATH=src``
and the approved Python 3.14 interpreter.  It regenerates the assigned
``tests/fixtures/baseline/{cli,tools,api-methods,config,notices,capacity}``
artifacts by calling the reference implementation.  It never reads a real
agent-run home, credentials, or network state; temporary paths are normalized
to ``${TMP_HOME}`` before writing JSON.
"""

from __future__ import annotations

import argparse
import ast
import dataclasses
import json
import os
import re
import sys
import tempfile
import textwrap
from enum import Enum
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from agent_run.capacity.forecast import CapacityForecast
from agent_run.capacity.history import CapacityKey
from agent_run.capacity.ranking import rank_capacity_routes
from agent_run.capacity.snapshot import CapacityRoute, CapacityRouteSnapshot
from agent_run.capacity.topology import CapacityRouteDescriptor, PhysicalPoolDescriptor
from agent_run.config import load_config
from agent_run.delivery.base import CompletionNotice
from agent_run.delivery.completion_notice_contract import (
    completion_handling_contract,
    completion_notice_contract_text,
    failure_notice_block,
)
from agent_run.domain import AgentStatus
from agent_run.dispatch import TOOLS


ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "tests" / "fixtures" / "baseline"
TMP_HOME = "${TMP_HOME}"


def _sanitize(value: object, temporary: Path | None = None) -> object:
    """Convert reference values into deterministic JSON-safe structures.

    Dataclasses, enums, paths, mappings, tuples, and lists are recursively
    represented without object addresses.  Temporary, current-worktree, and
    conventional test-home paths are replaced with stable placeholders.
    """

    if isinstance(value, Path):
        return _sanitize(str(value), temporary)
    if isinstance(value, Enum):
        return _sanitize(value.value, temporary)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _sanitize(getattr(value, field.name), temporary)
            for field in dataclasses.fields(value)
        }
    if isinstance(value, Mapping):
        return {
            str(key): _sanitize(value[key], temporary)
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (tuple, list, frozenset, set)):
        items = [_sanitize(item, temporary) for item in value]
        return sorted(items, key=lambda item: json.dumps(item, sort_keys=True)) if isinstance(value, (frozenset, set)) else items
    if isinstance(value, str):
        result = value
        prefixes = [str(Path.home()), os.environ.get("HOME", ""), os.environ.get("AGENT_RUN_HOME", "")]
        if temporary is not None:
            prefixes.append(str(temporary))
        for prefix in filter(None, prefixes):
            result = result.replace(prefix, TMP_HOME)
        result = result.replace(str(ROOT), "${WORKTREE}")
        result = re.sub(r"(?<![A-Za-z0-9_])(?:/private)?/tmp(?=/|$)", TMP_HOME, result)
        return result
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        return str(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return _sanitize(repr(value), temporary)


def _write_json(path: Path, value: object) -> None:
    """Write one sorted, UTF-8 JSON fixture with a trailing newline."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parser_children(parser: argparse.ArgumentParser) -> list[tuple[str, argparse.ArgumentParser]]:
    """Return direct subparser names and objects in parser declaration order."""

    for action in parser._actions:
        if isinstance(action, argparse._SubParsersAction):
            return sorted(action.choices.items())
    return []


def _cli_spec(parser: argparse.ArgumentParser, prefix: tuple[str, ...] = ()) -> list[dict[str, object]]:
    """Walk the live argparse tree and collect help plus complete action metadata."""

    command = " ".join(prefix) or "agent-run"
    actions = []
    for action in parser._actions:
        choices = action.choices
        entry = {
            "dest": action.dest,
            "option_strings": list(action.option_strings),
            "positional": not bool(action.option_strings),
            "nargs": action.nargs,
            "choices": list(choices) if choices is not None else None,
            "default": _sanitize(action.default),
            "required": bool(getattr(action, "required", False)),
            "help": action.help,
            "type": getattr(action.type, "__name__", None) if action.type else None,
        }
        actions.append(entry)
    record = {"command": command, "help": parser.format_help(), "actions": actions}
    result = [record]
    for name, child in _parser_children(parser):
        result.extend(_cli_spec(child, prefix + (name,)))
    return result


def capture_cli() -> int:
    """Capture every parser node's help text and argparse-derived option schema."""

    from agent_run.cli import _parser

    records = _cli_spec(_parser())
    cli_dir = FIXTURES / "cli"
    for record in records:
        filename = "root.txt" if record["command"] == "agent-run" else record["command"].removeprefix("agent-run ").replace(" ", "__") + ".txt"
        (cli_dir / filename).write_text(str(record["help"]), encoding="utf-8")
    _write_json(FIXTURES / "cli-spec.json", records)
    return len(records)


def capture_tools() -> int:
    """Capture the single transport-neutral dispatch table in declaration order."""

    records = []
    for tool in TOOLS:
        records.append({
            "name": tool["name"],
            "description": tool["description"],
            "inputSchema": _sanitize(tool.get("inputSchema")),
            "outputSchema": _sanitize(tool.get("outputSchema")),
            "resultShape": _sanitize(tool.get("resultShape")),
        })
    _write_json(FIXTURES / "tools.json", records)
    return len(records)


def capture_api_methods() -> int:
    """Capture public and private socket methods with lane and protocol errors."""

    from agent_run import api_socket

    tool_schemas = {item["name"]: item.get("inputSchema", {}) for item in TOOLS}
    records = []
    for name in sorted(api_socket.METHOD_NAMES):
        if name in tool_schemas:
            params = tool_schemas[name]
        elif name == "wait":
            params = {"type": "object", "properties": {"agent_id": {"type": "string"}, "timeout_seconds": {"type": "number"}}, "required": ["agent_id"], "additionalProperties": False}
        else:
            params = {"type": "object", "properties": {}, "additionalProperties": False}
        errors = {"-32602": "ValidationError", "-32000": "AgentRunError", "-32603": "internal error"}
        if name in api_socket.TOOL_NAMES or name == "wait":
            errors.update({"-32001": "overloaded", "-32002": "deadline", "-32003": "dispatcher closed"})
        records.append({"method": name, "params": _sanitize(params), "lane": "control" if name in api_socket._CONTROL_METHODS else "read", "error_codes": errors})
    _write_json(FIXTURES / "api-methods.json", records)
    return len(records)


def _candidate_toml_literals(path: Path) -> list[tuple[int, str]]:
    """Extract complete constant TOML-looking literals from one reference test."""

    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    candidates: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            continue
        text = textwrap.dedent(node.value).strip()
        if "schema_version" in text and ("=" in text or "[" in text):
            candidates.append((node.lineno, text))
    unique: dict[str, int] = {}
    for line, text in candidates:
        unique.setdefault(text, line)
    return [(line, text) for text, line in unique.items()]


def _config_case(source_test: str, line: int, toml: str, temporary: Path) -> dict[str, object]:
    """Run one literal through ``load_config`` and preserve success or failure evidence."""

    path = temporary / "config.toml"
    path.write_text(toml + "\n", encoding="utf-8")
    try:
        result = load_config(path)
    except Exception as error:  # Reference failures are part of the corpus.
        return {"id": f"{source_test}:{line}", "source_test": source_test, "toml": _sanitize(toml, temporary), "outcome": "error", "error_type": type(error).__name__, "error_message": _sanitize(str(error), temporary), "normalized_result_summary": None}
    return {"id": f"{source_test}:{line}", "source_test": source_test, "toml": _sanitize(toml, temporary), "outcome": "ok", "error_type": None, "error_message": None, "normalized_result_summary": _sanitize(result, temporary)}


def _profile_cases(source: str, temporary: Path) -> list[dict[str, object]]:
    """Run representative profile frontmatter literals through the profile loader."""

    from agent_run.profiles import load_profile

    documents = [
        ("review", "+++\nwrite = false\n+++\nReview carefully.\n", "test_profiles.py:18"),
        ("implement", '+++\nrevision = "1"\nwrite = true\nnetwork = false\nallow_external_read_roots = true\nskills = ["lsp-first", "document-code"]\nmcp = ["agent-lsp"]\nrequired_constraints = ["plugin_immutability"]\n+++\nImplement and verify the requested change.\n', "test_profiles.py:73"),
        ("mixed", '+++\nwrite = false\nskills = ["code-reading"]\n+++\nReview.\n', "test_profiles.py:106"),
    ]
    records = []
    root = temporary / "profiles"
    root.mkdir()
    for name, document, test_id in documents:
        (root / f"{name}.md").write_text(document, encoding="utf-8")
        try:
            result = load_profile(root, name)
        except Exception as error:
            records.append({"id": test_id, "source_test": source, "toml": _sanitize(document, temporary), "outcome": "error", "error_type": type(error).__name__, "error_message": _sanitize(str(error), temporary), "normalized_result_summary": None})
        else:
            records.append({"id": test_id, "source_test": source, "toml": _sanitize(document, temporary), "outcome": "ok", "error_type": None, "error_message": None, "normalized_result_summary": _sanitize(result, temporary)})
    return records


def capture_config() -> int:
    """Capture config and profile literals from the three assigned reference tests."""

    cases = []
    with tempfile.TemporaryDirectory(prefix="agent-run-baseline-") as directory:
        temporary = Path(directory)
        for filename in ("tests/test_config.py", "tests/test_native_settings.py", "tests/test_profiles.py"):
            path = ROOT / filename
            for line, text in _candidate_toml_literals(path):
                cases.append(_config_case(filename, line, text, temporary))
        cases.extend(_profile_cases("tests/test_profiles.py", temporary))
        cases.sort(key=lambda item: item["id"])
        _write_json(FIXTURES / "config" / "cases.json", cases)
    return len(cases)


def _notice(agent_id: str, status: AgentStatus, **kwargs: object) -> dict[str, object]:
    """Render one validated notice and expose the exact inputs and text."""

    notice = CompletionNotice(notification_id="ntf_abc", agent_id=agent_id, status=status, **kwargs)
    return {"input": {"notification_id": notice.notification_id, "agent_id": str(notice.agent_id), "status": status.value, **{key: _sanitize(value) for key, value in kwargs.items()}}, "failure_block": failure_notice_block(status.value, notice.failure_kind), "rendered": notice.render()}


def capture_notices() -> int:
    """Capture every terminal status, failure guidance branch, and escaping edge."""

    agent_id = "ag-20260825-120000-0123456789"
    cases = []
    for status in (AgentStatus.SUCCEEDED, AgentStatus.FAILED, AgentStatus.TIMED_OUT, AgentStatus.CANCELLED, AgentStatus.LOST):
        cases.append({"id": f"status-{status.value}", **_notice(agent_id, status)})
    for kind in ("prepare_failed", "provider_overloaded", "codex_futureProviderCode", "unknown"):
        cases.append({"id": f"failed-{kind}", **_notice(agent_id, AgentStatus.FAILED, failure_kind=kind)})
    cases.append({"id": "metadata-unicode-controls", **_notice(agent_id, AgentStatus.SUCCEEDED, runtime="codex\n- ID: ag-99999999-999999-ffffffffff", model="m\r\nPWNED\x85", effort="high low end")})
    cases.append({"id": "metadata-punctuation", **_notice(agent_id, AgentStatus.SUCCEEDED, runtime="co-dex", model="claude-opus-5@anthropic/ss-1:1m", effort="med-high")})
    _write_json(FIXTURES / "notices" / "cases.json", cases)
    _write_json(FIXTURES / "notices" / "contract.json", {"handling": completion_handling_contract(), "contract_text": completion_notice_contract_text()})
    return len(cases)


def _forecast(runtime: str, lane: str, remaining: float, *, burn: float | None = None, span: float | None = None, reset_at: float | None = 4600.0, risk: str = "low", known: bool = True) -> CapacityForecast:
    """Construct the compact synthetic forecast used by ranking tests."""

    key = CapacityKey(runtime, lane, "window", None, "source")
    return CapacityForecast(key, known, remaining if known else None, reset_at if known else None, 1000.0 if known else None, burn is None, burn if known else None, None, risk if known else "unknown", span if known else None)


def _route(runtime: str, route_id: str, pool_id: str, forecasts: tuple[CapacityForecast, ...], *, account: str | None = None, quota_lane: str = "lane", reset_credits: int | None = None) -> CapacityRoute:
    """Construct one synthetic route whose physical pool owns its forecasts."""

    pool = PhysicalPoolDescriptor(pool_id, frozenset(item.key for item in forecasts))
    descriptor = CapacityRouteDescriptor(route_id, runtime, account, quota_lane, (pool_id,), reset_credits)
    return CapacityRoute(descriptor, (pool,), forecasts)


def _rank_case(case_id: str, routes: tuple[CapacityRoute, ...], multipliers: dict[str, float] | None = None, *, route_multipliers: dict[tuple[str, str], float] | None = None) -> dict[str, object]:
    """Call the real ranker at the fixed test epoch and serialize its output."""

    result = rank_capacity_routes(CapacityRouteSnapshot(routes, (), ()), multipliers or {}, now=1000.0, route_multipliers=route_multipliers)
    return {"id": case_id, "inputs": {"now": 1000.0, "multipliers": multipliers or {}, "route_multipliers": route_multipliers or {}, "routes": routes}, "output": result}


def capture_capacity() -> int:
    """Capture representative ranking projections covering the assigned capacity tests."""

    cases = [
        _rank_case("reliable-multiplier", (_route("runtime-b", "route-b", "pool-b", (_forecast("runtime-b", "lane-b", 90.0, burn=0.0, span=7200.0),)), _route("runtime-a", "route-a", "pool-a", (_forecast("runtime-a", "lane-a", 80.0, burn=20.0, span=7200.0),))), {"runtime-a": 2.0}),
        _rank_case("equal-current-forecast", (_route("runtime-a", "route-a", "pool-a", (_forecast("runtime-a", "lane-a", 80.0, burn=20.0, span=7200.0),)), _route("runtime-b", "route-b", "pool-b", (_forecast("runtime-b", "lane-b", 80.0, burn=0.0, span=7200.0),)))),
        _rank_case("fallback-markers", (_route("opaque-runtime", "route", "pool", (_forecast("opaque-runtime", "warm", 75.0), _forecast("opaque-runtime", "thin", 75.0, burn=5.0, span=1800.0), _forecast("opaque-runtime", "none", 75.0, burn=5.0, span=7200.0, reset_at=None))),)),
        _rank_case("exhaustion-and-zero-score", (_route("provider-x", "route-b", "shared-pool", (_forecast("provider-x", "empty", 0.0),), account="b"), _route("provider-z", "zero", "zero-pool", (_forecast("provider-z", "zero", 1.0, burn=200.0, span=7200.0),)), _route("provider-y", "high", "high-pool", (_forecast("provider-y", "high", 1.0, risk="high"),)), _route("provider-x", "route-a", "shared-pool", (_forecast("provider-x", "empty", 0.0),), account="a")), {"provider-z": 100.0}),
        _rank_case("alias-collapse", (_route("provider-ζ", "route-b", "pool", (_forecast("provider-ζ", "shared", 80.0),), account="account-b", quota_lane="lane-b"), _route("provider-ζ", "route-a", "pool", (_forecast("provider-ζ", "shared", 80.0),), account="account-a", quota_lane="lane-a"))),
        _rank_case("tie-break", (_route("runtime", "route-c", "pool-c", (_forecast("runtime", "c", 60.0, reset_at=3000.0),)), _route("runtime", "route-b", "pool-b", (_forecast("runtime", "b", 60.0, reset_at=2000.0),)), _route("runtime", "route-a", "pool-a", (_forecast("runtime", "a", 60.0, reset_at=2000.0),)))),
        _rank_case("route-multiplier-scope", (_route("provider-b", "shared", "pool-b", (_forecast("provider-b", "b", 80.0),)), _route("provider-a", "alias", "pool-a", (_forecast("provider-a", "a", 80.0),)), _route("provider-a", "shared", "pool-a", (_forecast("provider-a", "a", 80.0),))), {"provider-a": 1.0, "provider-b": 1.0}, route_multipliers={("provider-a", "shared"): 3.0, ("provider-a", "alias"): 2.0, ("provider-b", "shared"): 0.5}),
        _rank_case("reset-credit-bonus", (_route("codex", "one", "one", (_forecast("codex", "one", 80.0),), reset_credits=1), _route("codex", "two", "two", (_forecast("codex", "two", 80.0),), reset_credits=2), _route("codex", "empty", "empty", (_forecast("codex", "empty", 0.0),), reset_credits=2))),
    ]
    _write_json(FIXTURES / "capacity" / "cases.json", _sanitize(cases))
    return len(cases)


def main() -> None:
    """Regenerate all assigned fixtures and print stable item counts."""

    counts = {"commands": capture_cli(), "tools": capture_tools(), "api_methods": capture_api_methods(), "config_cases": capture_config(), "notice_cases": capture_notices(), "capacity_cases": capture_capacity()}
    print(json.dumps(counts, sort_keys=True))


if __name__ == "__main__":
    main()
