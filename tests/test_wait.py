"""Contract tests for blocking agent ``wait`` and its CLI wiring.

Polling paths run against fakes with an injected clock and sleeper, so no test
really sleeps; the CLI wiring is exercised with runs that are already
terminal, which the wait loop answers on its first poll.
"""

from __future__ import annotations

import io
import json
import unittest
from dataclasses import dataclass
from typing import Any

from agent_run import cli
from agent_run.domain import AgentStatus
from agent_run.errors import ValidationError
from agent_run.wait import DEFAULT_POLL_SECONDS, WATCHER_TIMEOUT_EXIT, wait_for_agent

AGENT_ID = "ag-20260826-120000-0123456789"


@dataclass(frozen=True)
class _AgentView:
    """Stand-in for `service.AgentView` carrying only the polled status."""

    status: AgentStatus


@dataclass(frozen=True)
class _AnswerView:
    """Stand-in for `service.AnswerView`, the envelope the answer verb prints."""

    agent_id: str
    status: AgentStatus
    available: bool
    content: str | None


class _VirtualTime:
    """Injectable clock and sleeper, so no test really sleeps.

    ``clock()`` reports the virtual now in seconds and ``sleep(seconds)``
    records the requested interval before advancing the virtual now by it.
    """

    def __init__(self, start: float = 0.0) -> None:
        """Start the virtual clock at ``start`` seconds with nothing recorded."""

        self.now = start
        self.intervals: list[float] = []

    def clock(self) -> float:
        """Return the current virtual time in seconds."""

        return self.now

    def sleep(self, seconds: float) -> None:
        """Record one poll interval and advance the virtual clock by it."""

        self.intervals.append(seconds)
        self.now += seconds


class _ScriptedAgents:
    """Serve a scripted status sequence, then hand out the answer envelope."""

    def __init__(self, statuses: list[AgentStatus]) -> None:
        """Queue ``statuses``: one per poll, the last one repeats forever."""

        self._statuses = list(statuses)
        self._polls = 0
        self.envelope = _AnswerView(
            AGENT_ID, AgentStatus.SUCCEEDED, True, "the answer"
        )
        self.answered = 0

    def get(self, agent_id: str) -> _AgentView:
        """Return the next scripted status for ``agent_id``."""

        self._polls += 1
        index = min(self._polls, len(self._statuses)) - 1
        return _AgentView(self._statuses[index])

    def answer(self, agent_id: str) -> _AnswerView:
        """Return the sealed answer envelope for ``agent_id``."""

        self.answered += 1
        return self.envelope


class _TerminalService:
    """Facade whose agent is already terminal."""

    def __init__(self, status: AgentStatus = AgentStatus.SUCCEEDED) -> None:
        """Report ``status`` on every poll."""

        self.status = status
        self.gets = 0
        self.answers = 0

    def get(self, agent_id: str) -> _AgentView:
        """Report the terminal status and count the poll."""

        self.gets += 1
        return _AgentView(self.status)

    def answer(self, agent_id: str) -> dict[str, Any]:
        """Return the flat answer envelope and count the call."""

        self.answers += 1
        return {"agent_id": agent_id, "status": self.status.value, "available": False}

class _UnknownService:
    """Raise exactly as the store does for an id that was never started."""

    def get(self, agent_id: str) -> _AgentView:
        """Refuse the unknown agent id."""

        raise ValidationError(f"unknown agent: {agent_id}")

class WaitAgentTests(unittest.TestCase):
    """Poll an agent through the service facade with virtual time."""

    def test_running_agent_transitions_to_succeeded_and_returns_the_answer(self):
        """One running poll, then the answer envelope with a zero exit."""

        fake = _ScriptedAgents([AgentStatus.RUNNING, AgentStatus.SUCCEEDED])
        time = _VirtualTime()
        outcome = wait_for_agent(
            fake, AGENT_ID, timeout=60.0, poll=5.0, sleep=time.sleep, clock=time.clock
        )
        self.assertEqual(outcome.exit_code, 0)
        self.assertIsNone(outcome.note)
        self.assertIs(outcome.payload, fake.envelope)
        self.assertEqual(fake.answered, 1)
        self.assertEqual(time.intervals, [5.0])

    def test_failure_cancellation_and_timeout_map_to_2_3_and_4(self):
        """Terminal statuses carry their documented exit codes."""

        time = _VirtualTime()
        for status, expected in (
            (AgentStatus.FAILED, 2),
            (AgentStatus.CANCELLED, 3),
            (AgentStatus.TIMED_OUT, 4),
            (AgentStatus.LOST, 2),
        ):
            with self.subTest(status=status):
                fake = _ScriptedAgents([AgentStatus.RUNNING, status])
                outcome = wait_for_agent(
                    fake,
                    AGENT_ID,
                    timeout=60.0,
                    poll=5.0,
                    sleep=time.sleep,
                    clock=time.clock,
                )
                self.assertEqual(outcome.exit_code, expected)
                self.assertIs(outcome.payload, fake.envelope)

    def test_already_terminal_agent_returns_without_sleeping(self):
        """A terminal id answers on the first poll and never sleeps."""

        fake = _ScriptedAgents([AgentStatus.SUCCEEDED])
        time = _VirtualTime()
        outcome = wait_for_agent(
            fake, AGENT_ID, timeout=0.0, poll=5.0, sleep=time.sleep, clock=time.clock
        )
        self.assertEqual(outcome.exit_code, 0)
        self.assertEqual(time.intervals, [])
        self.assertEqual(fake.answered, 1)

    def test_watcher_gives_up_with_the_current_status_and_a_note(self):
        """An elapsed watcher budget exits 5 with the live status, no answer."""

        fake = _ScriptedAgents([AgentStatus.RUNNING])
        time = _VirtualTime()
        outcome = wait_for_agent(
            fake, AGENT_ID, timeout=10.0, poll=5.0, sleep=time.sleep, clock=time.clock
        )
        self.assertEqual(outcome.exit_code, WATCHER_TIMEOUT_EXIT)
        self.assertEqual(outcome.payload, _AgentView(AgentStatus.RUNNING))
        self.assertEqual(
            outcome.note, "wait gave up after 10s; status is still running"
        )
        self.assertEqual(fake.answered, 0)
        self.assertEqual(time.intervals, [5.0, 5.0])

    def test_no_timeout_keeps_polling_until_the_run_ends(self):
        """The default budget is unbounded: every poll is spaced and counted."""

        fake = _ScriptedAgents(
            [AgentStatus.RUNNING, AgentStatus.RUNNING, AgentStatus.SUCCEEDED]
        )
        time = _VirtualTime()
        outcome = wait_for_agent(
            fake, AGENT_ID, timeout=0.0, poll=5.0, sleep=time.sleep, clock=time.clock
        )
        self.assertEqual(outcome.exit_code, 0)
        self.assertEqual(time.intervals, [5.0, 5.0])

    def test_a_short_poll_request_is_clamped_to_one_second(self):
        """Below the one-second floor the loop stays off the store's back."""

        fake = _ScriptedAgents(
            [AgentStatus.RUNNING, AgentStatus.RUNNING, AgentStatus.SUCCEEDED]
        )
        time = _VirtualTime()
        outcome = wait_for_agent(
            fake, AGENT_ID, timeout=0.0, poll=0.1, sleep=time.sleep, clock=time.clock
        )
        self.assertEqual(outcome.exit_code, 0)
        self.assertEqual(time.intervals, [1.0, 1.0])


class WaitCliTests(unittest.TestCase):
    """Exercise the CLI wiring with runs that are already terminal."""

    def run_cli(self, argv: list[str], service) -> tuple[int, str, str]:
        """Run one CLI invocation over in-memory streams."""

        stdout = io.StringIO()
        stderr = io.StringIO()
        code = cli.main(
            argv,
            service=service,
            stdin=io.StringIO(""),
            stdout=stdout,
            stderr=stderr,
        )
        return code, stdout.getvalue(), stderr.getvalue()

    def test_wait_arguments_parse_with_the_documented_defaults(self):
        """The command accepts the positional id and polling options."""

        args = cli._parser().parse_args(["wait", AGENT_ID])
        self.assertEqual(
            (args.command, args.agent_id, args.timeout, args.poll),
            ("wait", AGENT_ID, 0.0, DEFAULT_POLL_SECONDS),
        )

    def test_wait_prints_the_answer_envelope_for_a_terminal_agent(self):
        """A terminal id prints the answer envelope once and exits zero."""

        service = _TerminalService()
        code, output, error = self.run_cli(["wait", AGENT_ID], service)
        self.assertEqual((code, error), (0, ""))
        self.assertEqual(
            json.loads(output),
            {"agent_id": AGENT_ID, "status": "succeeded", "available": False},
        )
        self.assertEqual((service.gets, service.answers), (1, 1))

    def test_agent_statuses_map_to_their_exit_codes(self):
        """Failed, cancelled, timed out, and lost keep their exit codes."""

        for status, expected in (
            (AgentStatus.FAILED, 2),
            (AgentStatus.CANCELLED, 3),
            (AgentStatus.TIMED_OUT, 4),
            (AgentStatus.LOST, 2),
        ):
            with self.subTest(status=status):
                code, output, _ = self.run_cli(
                    ["wait", AGENT_ID], _TerminalService(status)
                )
                self.assertEqual(code, expected)
                self.assertEqual(json.loads(output)["status"], status.value)

    def test_unknown_agent_fails_like_the_status_verb(self):
        """An unknown id shares the status verb's exit and error envelope."""

        wait_code, _, wait_error = self.run_cli(["wait", AGENT_ID], _UnknownService())
        status_code, _, status_error = self.run_cli(
            ["status", AGENT_ID], _UnknownService()
        )
        self.assertEqual(wait_code, status_code)
        self.assertNotEqual(wait_code, 0)
        self.assertEqual(json.loads(wait_error), json.loads(status_error))
        self.assertIn("unknown agent", wait_error)

if __name__ == "__main__":  # pragma: no cover
    unittest.main()
