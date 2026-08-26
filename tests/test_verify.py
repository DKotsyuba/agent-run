import hashlib
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.domain import AgentStatus, Outcome
from agent_run.errors import ValidationError
from agent_run.verify import (
    ANSWER_INCOMPLETE,
    ANSWER_PRESENT,
    DEFAULT_SENTINEL,
    ENGINE_VANISHED,
    GROUP_SURVIVED,
    NO_ANSWER,
    inspect_answer,
    silence_seconds,
    verify_completion,
)


class InspectAnswerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.addCleanup(self.temporary.cleanup)

    def write(self, text: str) -> Path:
        path = self.root / "answer.md"
        path.write_text(text, encoding="utf-8")
        return path

    def test_missing_file_is_no_answer(self) -> None:
        proof = inspect_answer(self.root / "absent.md")
        self.assertFalse(proof.exists)
        self.assertFalse(proof.complete)
        self.assertIsNone(proof.sha256)
        self.assertEqual(proof.evidence, NO_ANSWER)

    def test_empty_file_is_no_answer(self) -> None:
        proof = inspect_answer(self.write(""))
        self.assertTrue(proof.exists)
        self.assertEqual(proof.size_bytes, 0)
        self.assertEqual(proof.evidence, NO_ANSWER)

    def test_answer_without_sentinel_is_cut_off(self) -> None:
        proof = inspect_answer(self.write("half a thought"))
        self.assertTrue(proof.exists)
        self.assertFalse(proof.sentinel_found)
        self.assertFalse(proof.complete)
        self.assertEqual(proof.evidence, ANSWER_INCOMPLETE)

    def test_sentinel_makes_the_answer_complete_and_hashed(self) -> None:
        body = f"done\n{DEFAULT_SENTINEL}\n"
        proof = inspect_answer(self.write(body))
        self.assertTrue(proof.complete)
        self.assertEqual(proof.evidence, ANSWER_PRESENT)
        self.assertEqual(proof.size_bytes, len(body.encode("utf-8")))
        self.assertEqual(proof.sha256, hashlib.sha256(body.encode("utf-8")).hexdigest())

    def test_sentinel_split_across_a_read_boundary_is_found(self) -> None:
        head = "x" * (65536 - len(DEFAULT_SENTINEL) // 2)
        proof = inspect_answer(self.write(head + DEFAULT_SENTINEL + "tail"))
        self.assertTrue(proof.sentinel_found)

    def test_no_sentinel_required_accepts_any_nonempty_answer(self) -> None:
        proof = inspect_answer(self.write("free form"), sentinel=None)
        self.assertTrue(proof.complete)
        self.assertEqual(inspect_answer(self.write(""), sentinel=None).evidence, NO_ANSWER)

    def test_blank_sentinel_is_refused(self) -> None:
        with self.assertRaises(ValidationError):
            inspect_answer(self.write("body"), sentinel="   ")


class SilenceTests(unittest.TestCase):
    def test_silence_is_measured_from_the_last_progress(self) -> None:
        self.assertIsNone(silence_seconds(None, 10.0))
        self.assertEqual(silence_seconds(4.0, 10.0), 6.0)
        self.assertEqual(silence_seconds(12.0, 10.0), 0.0)

    def test_non_finite_progress_is_refused(self) -> None:
        with self.assertRaises(ValidationError):
            silence_seconds(float("nan"), 1.0)


class VerifyCompletionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.addCleanup(self.temporary.cleanup)

    def proof(self, text: str | None):
        path = self.root / "answer.md"
        if text is not None:
            path.write_text(text, encoding="utf-8")
        return inspect_answer(path)

    def test_a_surviving_group_is_never_reported_as_finished(self) -> None:
        outcome = verify_completion(
            session_outcome=Outcome(AgentStatus.SUCCEEDED),
            stop_reason=None,
            answer=self.proof(f"ok {DEFAULT_SENTINEL}"),
            group_gone=False,
        )
        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, GROUP_SURVIVED)
        self.assertIn(ANSWER_PRESENT, outcome.failure_text or "")

    def test_cancel_records_the_answer_evidence_it_had(self) -> None:
        outcome = verify_completion(
            session_outcome=None,
            stop_reason="cancel",
            answer=self.proof("partial"),
            group_gone=True,
            last_progress_at=1.0,
            now=4.0,
            silence_threshold_seconds=10.0,
        )
        self.assertIs(outcome.status, AgentStatus.CANCELLED)
        self.assertEqual(outcome.failure_kind, ANSWER_INCOMPLETE)
        self.assertEqual(outcome.failure_text, "silence=3.0s/active")

    def test_cancel_and_timeout_preserve_a_complete_answer(self) -> None:
        body = f"usable partial result\n{DEFAULT_SENTINEL}"
        proof = self.proof(body)
        for reason, status in (
            ("cancel", AgentStatus.CANCELLED),
            ("timeout", AgentStatus.TIMED_OUT),
        ):
            with self.subTest(reason=reason):
                outcome = verify_completion(
                    session_outcome=Outcome(
                        AgentStatus.SUCCEEDED, runtime_session_id="sess-stop"
                    ),
                    stop_reason=reason,
                    answer=proof,
                    group_gone=True,
                )
                self.assertIs(outcome.status, status)
                self.assertEqual(outcome.answer_path, proof.path)
                self.assertEqual(outcome.answer_bytes, len(body.encode()))
                self.assertEqual(outcome.answer_sha256, proof.sha256)
                self.assertEqual(outcome.runtime_session_id, "sess-stop")

    def test_timeout_without_any_answer_reports_silence(self) -> None:
        outcome = verify_completion(
            session_outcome=None,
            stop_reason="timeout",
            answer=self.proof(None),
            group_gone=True,
            last_progress_at=None,
            now=90.0,
        )
        self.assertIs(outcome.status, AgentStatus.TIMED_OUT)
        self.assertEqual(outcome.failure_kind, NO_ANSWER)
        self.assertEqual(outcome.failure_text, "silence=no_progress")

    def test_timeout_with_a_silent_engine_says_silent(self) -> None:
        outcome = verify_completion(
            session_outcome=None,
            stop_reason="timeout",
            answer=self.proof("cut off"),
            group_gone=True,
            last_progress_at=10.0,
            now=200.0,
            silence_threshold_seconds=60.0,
        )
        self.assertEqual(outcome.failure_kind, ANSWER_INCOMPLETE)
        self.assertEqual(outcome.failure_text, "silence=190.0s/silent")

    def test_engine_success_without_a_complete_answer_is_a_failure(self) -> None:
        outcome = verify_completion(
            session_outcome=Outcome(AgentStatus.SUCCEEDED, exit_code=0),
            stop_reason=None,
            answer=self.proof("no marker"),
            group_gone=True,
        )
        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, ANSWER_INCOMPLETE)
        self.assertEqual(outcome.exit_code, 0)

    def test_success_with_a_complete_answer_carries_the_proof(self) -> None:
        body = f"the answer\n{DEFAULT_SENTINEL}"
        proof = self.proof(body)
        outcome = verify_completion(
            session_outcome=Outcome(
                AgentStatus.SUCCEEDED, exit_code=0, runtime_session_id="sess-1"
            ),
            stop_reason=None,
            answer=proof,
            group_gone=True,
        )
        self.assertIs(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(outcome.answer_sha256, proof.sha256)
        self.assertEqual(outcome.answer_bytes, len(body.encode("utf-8")))
        self.assertEqual(outcome.runtime_session_id, "sess-1")

    def test_a_vanished_engine_is_a_failure(self) -> None:
        outcome = verify_completion(
            session_outcome=None,
            stop_reason=None,
            answer=self.proof(None),
            group_gone=True,
        )
        self.assertIs(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, ENGINE_VANISHED)

    def test_an_engine_failure_is_passed_through(self) -> None:
        failure = Outcome(AgentStatus.FAILED, exit_code=2, failure_kind="engine_error")
        outcome = verify_completion(
            session_outcome=failure,
            stop_reason=None,
            answer=self.proof(None),
            group_gone=True,
        )
        self.assertIs(outcome, failure)

    def test_unknown_stop_reason_is_refused(self) -> None:
        with self.assertRaises(ValidationError):
            verify_completion(
                session_outcome=None,
                stop_reason="explode",
                answer=None,
                group_gone=True,
            )


if __name__ == "__main__":
    unittest.main()
