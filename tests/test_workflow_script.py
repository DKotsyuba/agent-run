"""End-to-end tests for the restricted workflow-script API."""

from __future__ import annotations

import threading
import time

import pytest

from agent_run.workflow_script import run_script, validate_script


def _executor(step_key: str, spec: dict[str, object]) -> dict[str, object]:
    """Return a deterministic fake result containing the assigned identity."""

    return {"step_key": step_key, "value": spec["value"]}


def test_five_functions_end_to_end_and_phase_log_order() -> None:
    """Expose agent, parallel, pipeline, phase, and log with ordered sinks."""

    events: list[tuple[str, str]] = []
    result = run_script(
        """
phase("build")
log("starting")
first = agent({"value": 2})
paired = parallel([lambda: agent({"value": 3}), lambda: agent({"value": 4})])
flow = pipeline([1, 2], lambda value: value + 1, lambda value: value * 10)
log("done")
{"first": first["value"], "paired": [item["value"] for item in paired], "flow": flow}
""",
        _executor,
        lambda text: events.append(("phase", text)),
        lambda text: events.append(("log", text)),
        2,
    )
    assert result == {"first": 2, "paired": [3, 4], "flow": [20, 30]}
    assert events == [("phase", "build"), ("log", "starting"), ("log", "done")]


def test_parallel_is_truly_concurrent_and_tolerates_failure() -> None:
    """Run overlapping thunks while retaining order and replacing failures."""

    barrier = threading.Barrier(2)

    def executor(key: str, spec: dict[str, object]) -> dict[str, object]:
        """Require two executor calls to overlap before returning."""

        barrier.wait(timeout=1)
        return {"step_key": key, "value": spec["value"]}

    result = run_script(
        "parallel([lambda: agent({'value': 1}), lambda: agent({'value': 2}), lambda: 1 / 0])",
        executor,
        lambda _: None,
        lambda _: None,
        2,
    )
    assert [item["value"] if item else None for item in result] == [1, 2, None]


def test_pipeline_has_no_stage_barrier_and_drops_failed_item() -> None:
    """Allow one item into stage two before another item leaves stage one."""

    reached_second_stage = threading.Event()

    def executor(key: str, spec: dict[str, object]) -> dict[str, object]:
        """Synchronize stages so a stage barrier would time out and drop item two."""

        value = spec["value"]
        stage = spec["stage"]
        if value == 2 and stage == 1:
            if not reached_second_stage.wait(timeout=1):
                raise TimeoutError("pipeline imposed a stage barrier")
        if value == 1 and stage == 2:
            reached_second_stage.set()
        if value == 3 and stage == 2:
            raise ValueError("drop")
        return {"step_key": key, "value": value}

    result = run_script(
        """
def first(value):
    return agent({"value": value, "stage": 1})["value"]
def second(value):
    return agent({"value": value, "stage": 2})["value"] * 10
pipeline([1, 2, 3], first, second)
""",
        executor,
        lambda _: None,
        lambda _: None,
        3,
    )
    assert result == [10, 20, None]


@pytest.mark.parametrize(
    ("case", "source"),
    [
        ("import_os", "import os"),
        ("open", "open('x')"),
        ("dunder_class_ladder", "(1).__class__.__mro__"),
        ("getattr", "getattr(1, 'real')"),
        ("eval", "eval('1')"),
        ("time", "time.time()"),
        ("random", "random.random()"),
    ],
    ids=lambda value: value if isinstance(value, str) and " " not in value else None,
)
def test_escape_attempt_is_refused(case: str, source: str) -> None:
    """Reject each named escape or nondeterminism attempt during validation."""

    del case
    with pytest.raises(Exception):
        validate_script(source)


def test_determinism_produces_identical_step_key_sequence() -> None:
    """Produce the same canonical step identities on repeated executions."""

    sequences: list[list[str]] = []
    for _ in range(2):
        keys: list[str] = []

        def executor(key: str, spec: dict[str, object]) -> dict[str, object]:
            """Capture each key while returning a stable fake result."""

            keys.append(key)
            return {"value": spec["value"]}

        run_script(
            "agent({'value': 1, 'nested': {'b': 2, 'a': 1}}); agent({'value': 2})",
            executor,
            lambda _: None,
            lambda _: None,
            2,
        )
        sequences.append(keys)
    assert sequences[0] == sequences[1]


def test_script_exception_becomes_script_error() -> None:
    """Convert an uncaught script exception into structured failure details."""

    result = run_script("raise ValueError('boom')", _executor, lambda _: None, lambda _: None, 1)
    assert result == {
        "failure_kind": "script_error",
        "failure_params": {"exception": "ValueError('boom')"},
    }
