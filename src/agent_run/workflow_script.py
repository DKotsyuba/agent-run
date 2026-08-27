"""Validate and execute deterministic workflow scripts in a restricted namespace."""

from __future__ import annotations

import ast
import json
import math
import re
import threading
from concurrent.futures import ThreadPoolExecutor
from types import MappingProxyType, ModuleType
from typing import Callable, TypeAlias

from agent_run.state.workflow import step_key

StepResult: TypeAlias = dict[str, object]
Executor: TypeAlias = Callable[[str, dict[str, object]], StepResult]
Sink: TypeAlias = Callable[[str], None]

_ALLOWED_IMPORTS = frozenset({"json", "math", "re"})
_REFUSED_CALLS = frozenset(
    {
        "compile",
        "delattr",
        "eval",
        "exec",
        "getattr",
        "globals",
        "locals",
        "open",
        "setattr",
        "vars",
    }
)
_REFUSED_NAMES = _REFUSED_CALLS | frozenset({"random", "time"})


class ScriptValidationError(ValueError):
    """Report a source construct that is forbidden by the workflow sandbox."""


class _ScriptGuard(ast.NodeVisitor):
    """Reject syntax that can escape the restricted workflow namespace."""

    def visit_Import(self, node: ast.Import) -> None:
        """Allow imports only when every imported module is explicitly approved."""

        for alias in node.names:
            if alias.name not in _ALLOWED_IMPORTS:
                raise ScriptValidationError(f"import is not allowed: {alias.name}")
            if alias.asname in {"time", "random"}:
                raise ScriptValidationError(f"name is not allowed: {alias.asname}")
        self.generic_visit(node)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Allow absolute from-imports only from explicitly approved modules."""

        if node.level or node.module not in _ALLOWED_IMPORTS:
            raise ScriptValidationError(f"import is not allowed: {node.module or ''}")
        for alias in node.names:
            if alias.asname in {"time", "random"}:
                raise ScriptValidationError(f"name is not allowed: {alias.asname}")
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        """Reject dunder attributes, preventing object-introspection ladders."""

        if node.attr.startswith("__"):
            raise ScriptValidationError(f"dunder attribute is not allowed: {node.attr}")
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        """Reject nondeterministic modules and dangerous builtin names everywhere."""

        if node.id in _REFUSED_NAMES:
            raise ScriptValidationError(f"name is not allowed: {node.id}")
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Reject direct calls to builtins that expose code or object internals."""

        if isinstance(node.func, ast.Name) and node.func.id in _REFUSED_CALLS:
            raise ScriptValidationError(f"call is not allowed: {node.func.id}")
        self.generic_visit(node)


def validate_script(source: str) -> ast.Module:
    """Parse and validate workflow source, returning its checked syntax tree.

    ``source`` must be a string containing Python syntax.  Imports are limited
    to ``json``, ``math``, and ``re``; dangerous reflection, code execution,
    I/O, and nondeterministic names raise :class:`ScriptValidationError`.
    """

    if not isinstance(source, str):
        raise TypeError("workflow source must be a string")
    tree = ast.parse(source, filename="<workflow>", mode="exec")
    _ScriptGuard().visit(tree)
    return tree


def _safe_import(
    name: str,
    globals_: dict[str, object] | None = None,
    locals_: dict[str, object] | None = None,
    fromlist: tuple[str, ...] = (),
    level: int = 0,
) -> ModuleType:
    """Import one allowlisted pure-computation module for validated source."""

    del globals_, locals_
    if level or name not in _ALLOWED_IMPORTS:
        raise ImportError(f"import is not allowed: {name}")
    module = {"json": json, "math": math, "re": re}[name]
    if fromlist:
        for member in fromlist:
            if member.startswith("_") or not hasattr(module, member):
                raise ImportError(f"cannot import name {member!r} from {name!r}")
    return module


_SAFE_BUILTINS = MappingProxyType(
    {
        "__import__": _safe_import,
        "abs": abs,
        "all": all,
        "any": any,
        "bool": bool,
        "dict": dict,
        "enumerate": enumerate,
        "Exception": Exception,
        "filter": filter,
        "float": float,
        "int": int,
        "isinstance": isinstance,
        "len": len,
        "list": list,
        "map": map,
        "max": max,
        "min": min,
        "range": range,
        "reversed": reversed,
        "round": round,
        "set": set,
        "sorted": sorted,
        "str": str,
        "sum": sum,
        "tuple": tuple,
        "ValueError": ValueError,
        "zip": zip,
    }
)


def run_script(
    source: str,
    executor: Executor,
    phase_sink: Sink,
    log_sink: Sink,
    concurrency_cap: int,
) -> object:
    """Execute validated workflow source with only the five-function API.

    Agent specs must be plain JSON-compatible dictionaries.  Each call receives
    a deterministic key based on its canonical spec and invocation position.
    ``parallel`` and ``pipeline`` use at most ``concurrency_cap`` worker threads;
    failed parallel thunks and failed pipeline items become ``None``.  The final
    expression is returned as the run result, or ``None`` when absent.  Syntax
    and runtime failures are converted to a ``script_error`` failure mapping.
    """

    try:
        tree = validate_script(source)
        if isinstance(concurrency_cap, bool) or not isinstance(concurrency_cap, int):
            raise ValueError("concurrency_cap must be a positive integer")
        if concurrency_cap < 1:
            raise ValueError("concurrency_cap must be a positive integer")

        position = 0
        position_lock = threading.Lock()

        def agent(spec: dict[str, object]) -> StepResult:
            """Canonicalize a plain-dict step spec and delegate it to the executor."""

            nonlocal position
            if type(spec) is not dict:
                raise TypeError("agent spec must be a plain dict")
            canonical = json.loads(
                json.dumps(spec, sort_keys=True, separators=(",", ":"), allow_nan=False)
            )
            with position_lock:
                current = position
                position += 1
            return executor(step_key(canonical, current), canonical)

        def parallel(thunks: list[Callable[[], object]]) -> list[object | None]:
            """Run zero-argument thunks concurrently, replacing failures with ``None``."""

            if type(thunks) is not list or not all(callable(thunk) for thunk in thunks):
                raise TypeError("parallel expects a list of callable thunks")

            def tolerate(thunk: Callable[[], object]) -> object | None:
                """Return one thunk result, converting its exception to ``None``."""

                try:
                    return thunk()
                except BaseException:
                    return None

            with ThreadPoolExecutor(max_workers=concurrency_cap) as pool:
                return list(pool.map(tolerate, thunks))

        def pipeline(items: object, *stages: Callable[[object], object]) -> list[object | None]:
            """Run each item through all stages independently, dropping failed chains."""

            if not all(callable(stage) for stage in stages):
                raise TypeError("pipeline stages must be callable")
            materialized = list(items)  # type: ignore[arg-type]

            def chain(item: object) -> object | None:
                """Apply every stage to one item without waiting on other items."""

                value = item
                try:
                    for stage in stages:
                        value = stage(value)
                    return value
                except BaseException:
                    return None

            with ThreadPoolExecutor(max_workers=concurrency_cap) as pool:
                return list(pool.map(chain, materialized))

        def phase(name: str) -> None:
            """Forward one phase name to the injected sink in script order."""

            if not isinstance(name, str):
                raise TypeError("phase name must be a string")
            phase_sink(name)

        def log(text: str) -> None:
            """Forward one log message to the injected sink in script order."""

            if not isinstance(text, str):
                raise TypeError("log text must be a string")
            log_sink(text)

        result_name = "__workflow_result__"
        if tree.body and isinstance(tree.body[-1], ast.Expr):
            tree.body[-1] = ast.Assign(
                targets=[ast.Name(id=result_name, ctx=ast.Store())], value=tree.body[-1].value
            )
            ast.fix_missing_locations(tree)
        namespace: dict[str, object] = {
            "__builtins__": _SAFE_BUILTINS,
            "agent": agent,
            "parallel": parallel,
            "pipeline": pipeline,
            "phase": phase,
            "log": log,
        }
        exec(compile(tree, "<workflow>", "exec"), namespace, namespace)
        return namespace.get(result_name)
    except BaseException as error:
        return {
            "failure_kind": "script_error",
            "failure_params": {"exception": repr(error)},
        }
