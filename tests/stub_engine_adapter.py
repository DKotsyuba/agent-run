"""A deterministic real-process adapter for exec'd supervisor tests.

The adapter materializes and prepares inside the detached supervisor, then
launches a short shell process whose answer is verified as durable evidence.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    LaunchPlan,
    ModelInfo,
    RuntimeHealth,
    RuntimeInfo,
)
from agent_run.adapters.snapshots import finalize_runtime_snapshots
from agent_run.domain import AgentStatus, Outcome
from agent_run.verify import DEFAULT_SENTINEL


ENGINE = r'printf "%s\n%s\n" "$STUB_SECRET" "$STUB_SENTINEL" > "$1"'
SECRET = "opencode-server-password-2f7c"


class StubEngineSession:
    owns_process_group = True

    def __init__(self, process: subprocess.Popen) -> None:
        self._process = process

    @property
    def pid(self) -> int | None:
        return self._process.pid

    def wait(self, timeout_seconds: float | None) -> Outcome | None:
        try:
            code = self._process.wait(timeout_seconds)
        except subprocess.TimeoutExpired:
            return None
        if code == 0:
            return Outcome(AgentStatus.SUCCEEDED, exit_code=0)
        return Outcome(
            AgentStatus.FAILED, exit_code=code, failure_kind="engine_exit"
        )

    def steer(self, text: str) -> None:
        raise NotImplementedError("stub engine is not steerable")

    def cancel(self, grace_seconds: float) -> None:
        self._process.terminate()


class StubEngineAdapter:
    def describe(self) -> RuntimeInfo:
        """Advertise the capabilities exercised by supervisor preparation."""

        return RuntimeInfo(
            "fake",
            ADAPTER_API_VERSION,
            frozenset(
                {Capability.MODEL_ROSTER, Capability.SKILLS, Capability.TRANSCRIPT}
            ),
        )

    def validate(self, config) -> None:
        return None

    def materialize(self, config, home, *, mcp_servers, skills_root) -> str:
        """Create an empty managed runtime snapshot and return its revision."""

        finalize_runtime_snapshots(Path(home), "cfg-1")
        return "cfg-1"

    def probe(self, config, home) -> RuntimeHealth:
        return RuntimeHealth(True, "1", True, None)

    def models(self, config, home):
        return (ModelInfo("model", "stub engine"),)

    def limits(self, config, home):
        return ()

    def prepare(
        self,
        request,
        role,
        config,
        home,
        agent_dir,
        *,
        resume_session_id=None,
    ) -> LaunchPlan:
        """Build the deterministic shell plan encoded by the fixture request.

        The request task is a nonnegative sleep duration in seconds. The plan
        writes the fixed secret and completion sentinel before that optional
        delay, allowing the supervisor tests to observe RUNNING when requested.
        """

        sleep_seconds = float(request.task)
        if sleep_seconds < 0:
            raise ValueError("stub sleep duration must be nonnegative")
        script = ENGINE if sleep_seconds == 0 else f"{ENGINE}; sleep {sleep_seconds}"
        answer_path = agent_dir / "answer.md"
        return LaunchPlan(
            (str(config.binary), "-c", script, "sh", str(answer_path)),
            request.workdir,
            {"STUB_SECRET": SECRET, "STUB_SENTINEL": DEFAULT_SENTINEL},
            None,
            agent_dir / "runtime.jsonl",
            {},
            answer_path,
            resume_session_id,
        )

    def launch(self, plan: LaunchPlan, sink) -> StubEngineSession:
        sink.event("stub_engine_launched", {"argv": list(plan.argv)})
        process = subprocess.Popen(
            list(plan.argv),
            cwd=str(plan.cwd),
            env=dict(plan.environment),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return StubEngineSession(process)


ADAPTER = StubEngineAdapter()
