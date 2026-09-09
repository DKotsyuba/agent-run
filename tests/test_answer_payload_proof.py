"""Acceptance coverage for clean answer payloads and versioned proofs."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    LaunchPlan,
    ModelInfo,
    RuntimeHealth,
    RuntimeInfo,
)
from agent_run.adapters.home import seal_answer
from agent_run.adapters.snapshots import finalize_runtime_snapshots
from agent_run.config import Config, ProfilesConfig, RuntimeConfig
from agent_run.domain import TERMINAL, AgentStatus, Outcome, StartRequest
from agent_run.errors import PathEscapeError
from agent_run.paths import agent_dir
from agent_run.preparation import prepare_launch
from agent_run.service import AgentService
from agent_run.state.store import StateStore
from agent_run.verify import (
    ANSWER_FORMAT_LEGACY,
    ANSWER_FORMAT_PROOF,
    DEFAULT_SENTINEL,
    AnswerEncodingError,
    AnswerMissingError,
    AnswerOversizedError,
    AnswerProofError,
    AnswerTamperedError,
    answer_format_path,
    answer_proof_path,
    inspect_answer,
    read_answer_payload,
    strip_legacy_frame,
)


def _framed(payload: bytes) -> bytes:
    """Return the exact bytes the historical sentinel sealer would have written."""

    separator = b"" if payload.endswith(b"\n") else b"\n"
    return payload + separator + DEFAULT_SENTINEL.encode("utf-8") + b"\n"


class SealAndProofTests(unittest.TestCase):
    """Cover sealing, proof inspection, legacy framing, and bounded reads."""

    def setUp(self) -> None:
        """Create an isolated answer directory for each proof-path test."""

        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.path = self.root / "answer.md"

    def tearDown(self) -> None:
        """Remove the isolated answer directory."""

        self.temporary.cleanup()

    def test_seal_writes_clean_payload_and_versioned_proof(self) -> None:
        """Seal exact payload bytes and bind them to the current proof format."""

        size, digest = seal_answer(self.path, "body text")
        data = self.path.read_bytes()
        self.assertEqual(data, b"body text")
        self.assertNotIn(DEFAULT_SENTINEL.encode(), data)
        self.assertEqual((size, digest), (len(data), hashlib.sha256(data).hexdigest()))
        sidecar = self.root / "answer.md.proof.json"
        proof = json.loads(sidecar.read_bytes())
        self.assertEqual(proof["proof_version"], ANSWER_FORMAT_PROOF)
        self.assertEqual(proof["bytes"], size)
        self.assertEqual(proof["sha256"], digest)
        inspection = inspect_answer(self.path)
        self.assertTrue(inspection.complete)
        self.assertEqual(inspection.proof_version, ANSWER_FORMAT_PROOF)
        self.assertIsNone(inspection.proof_error)

    def test_sealed_payload_may_contain_sentinel_text(self) -> None:
        """Keep sentinel-looking text inside a new payload as ordinary content."""

        text = f"the marker {DEFAULT_SENTINEL} is just content here"
        size, digest = seal_answer(self.path, text)
        self.assertEqual(self.path.read_bytes(), text.encode("utf-8"))
        self.assertTrue(inspect_answer(self.path).complete)
        payload = read_answer_payload(
            self.path,
            expected_bytes=size,
            expected_sha256=digest,
            max_bytes=1 << 20,
            strip_legacy=False,
        )
        self.assertEqual(payload, text)

    def test_legacy_terminal_frame_is_stripped_once_exactly(self) -> None:
        """Strip only the historical terminal frame and at most once."""

        self.assertEqual(strip_legacy_frame(_framed(b"payload")), b"payload")
        # A payload trailing newline is not distinguishable from the joining
        # separator, so presentation drops exactly the one known frame.
        self.assertEqual(strip_legacy_frame(_framed(b"payload\n")), b"payload")
        self.assertEqual(strip_legacy_frame(b"no frame here"), b"no frame here")

    def test_legacy_embedded_sentinel_is_preserved(self) -> None:
        """Preserve sentinel text embedded before the historical terminal frame."""

        framed = _framed(f"see {DEFAULT_SENTINEL} inside".encode())
        stripped = strip_legacy_frame(framed)
        self.assertEqual(stripped, f"see {DEFAULT_SENTINEL} inside".encode())

    def test_inspect_legacy_requires_the_terminal_sentinel(self) -> None:
        """Require the terminal frame when no durable new-format evidence exists."""

        self.path.write_bytes(_framed(b"legacy body"))
        proof = inspect_answer(self.path)
        self.assertTrue(proof.complete)
        self.assertEqual(proof.proof_version, ANSWER_FORMAT_LEGACY)
        self.path.write_bytes(b"cut off mid write")
        cut = inspect_answer(self.path)
        self.assertFalse(cut.complete)
        self.assertEqual(cut.proof_version, ANSWER_FORMAT_LEGACY)

    def test_malformed_or_contradicting_proof_never_downgrades(self) -> None:
        """Keep malformed and mismatched proof artifacts in format two."""

        seal_answer(self.path, "proven body")
        sidecar = self.root / "answer.md.proof.json"
        sidecar.write_bytes(b"this is not json")
        broken = inspect_answer(self.path)
        self.assertFalse(broken.complete)
        self.assertEqual(broken.proof_version, ANSWER_FORMAT_PROOF)
        self.assertIsNotNone(broken.proof_error)
        seal_answer(self.path, "proven body")
        document = json.loads(sidecar.read_bytes())
        document["sha256"] = "0" * 64
        sidecar.write_text(json.dumps(document), encoding="utf-8")
        contradicted = inspect_answer(self.path)
        self.assertFalse(contradicted.complete)
        self.assertIsNotNone(contradicted.proof_error)

    def test_missing_proof_never_downgrades_a_sealed_payload(self) -> None:
        """Use the durable format marker when a new payload loses its proof."""

        seal_answer(self.path, f"payload\n{DEFAULT_SENTINEL}\n")
        answer_proof_path(self.path).unlink()
        inspection = inspect_answer(self.path)
        self.assertEqual(inspection.proof_version, ANSWER_FORMAT_PROOF)
        self.assertFalse(inspection.complete)
        self.assertIn("missing", inspection.proof_error or "")

    def test_directory_sync_failure_cannot_leave_false_legacy_completion(self) -> None:
        """Stop after publishing the format marker when its directory sync fails."""

        self.path.write_bytes(_framed(b"old legacy body"))

        def fail_directory_sync(descriptor: int) -> None:
            """Fail only the directory fsync that makes one replace durable."""

            if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                raise OSError("directory sync failed")

        with patch("agent_run.adapters.home.os.fsync", side_effect=fail_directory_sync):
            with self.assertRaisesRegex(OSError, "directory sync failed"):
                seal_answer(self.path, "new payload")
        inspection = inspect_answer(self.path)
        self.assertEqual(inspection.proof_version, ANSWER_FORMAT_PROOF)
        self.assertFalse(inspection.complete)
        self.assertIn("missing", inspection.proof_error or "")

    def test_metadata_reads_are_bounded_and_reject_symlinks(self) -> None:
        """Reject oversized proof metadata and proof paths that are symlinks."""

        seal_answer(self.path, "bounded")
        proof = answer_proof_path(self.path)
        proof.write_bytes(b"x" * 4097)
        self.assertIn("4096-byte bound", inspect_answer(self.path).proof_error or "")
        proof.unlink()
        target = self.root / "elsewhere.json"
        target.write_text("{}", encoding="utf-8")
        proof.symlink_to(target)
        self.assertIn("regular file", inspect_answer(self.path).proof_error or "")

    def test_metadata_symlink_swap_cannot_read_an_external_proof(self) -> None:
        """Reject a proof replaced by a symlink at the descriptor-open boundary."""

        seal_answer(self.path, "bounded")
        proof = answer_proof_path(self.path)
        with tempfile.TemporaryDirectory() as directory:
            outside = Path(directory) / "proof.json"
            outside.write_bytes(proof.read_bytes())
            real_open = os.open
            swapped = False

            def swap_then_open(file, flags, mode=0o777, *, dir_fd=None):
                """Swap the proof immediately before the production open call."""

                nonlocal swapped
                if not swapped and dir_fd is None and Path(file) == proof:
                    proof.unlink()
                    proof.symlink_to(outside)
                    swapped = True
                if dir_fd is None:
                    return real_open(file, flags, mode)
                return real_open(file, flags, mode, dir_fd=dir_fd)

            with patch("agent_run.verify.os.open", side_effect=swap_then_open):
                inspection = inspect_answer(self.path)
        self.assertTrue(swapped)
        self.assertFalse(inspection.complete)
        self.assertIn("regular file", inspection.proof_error or "")

    def test_corrupted_format_marker_fails_closed(self) -> None:
        """Reject a corrupted durable format marker even with a valid proof."""

        seal_answer(self.path, "marked")
        answer_format_path(self.path).write_bytes(b"legacy?\n")
        inspection = inspect_answer(self.path)
        self.assertEqual(inspection.proof_version, ANSWER_FORMAT_PROOF)
        self.assertFalse(inspection.complete)
        self.assertIn("format marker", inspection.proof_error or "")

    def test_read_answer_payload_raises_distinct_typed_errors(self) -> None:
        """Classify missing, oversized, tampered, linked, and invalid UTF-8 payloads."""

        size, digest = seal_answer(self.path, "typed errors")
        with self.assertRaises(AnswerMissingError):
            read_answer_payload(
                self.root / "absent.md",
                expected_bytes=size,
                expected_sha256=digest,
                max_bytes=1 << 20,
                strip_legacy=False,
            )
        with self.assertRaises(AnswerOversizedError):
            read_answer_payload(
                self.path,
                expected_bytes=size,
                expected_sha256=digest,
                max_bytes=size - 1,
                strip_legacy=False,
            )
        with self.assertRaises(AnswerTamperedError):
            read_answer_payload(
                self.path,
                expected_bytes=size,
                expected_sha256="0" * 64,
                max_bytes=1 << 20,
                strip_legacy=False,
            )
        with self.assertRaises(AnswerTamperedError):
            read_answer_payload(
                self.path,
                expected_bytes=size + 1,
                expected_sha256=digest,
                max_bytes=1 << 20,
                strip_legacy=False,
            )
        link = self.root / "linked.md"
        link.symlink_to(self.path)
        with self.assertRaises(PathEscapeError):
            read_answer_payload(
                link,
                expected_bytes=size,
                expected_sha256=digest,
                max_bytes=1 << 20,
                strip_legacy=False,
            )
        raw = self.root / "raw.md"
        raw.write_bytes(b"\xff\xfe\x01")
        bad = b"\xff\xfe\x01"
        with self.assertRaises(AnswerEncodingError):
            read_answer_payload(
                raw,
                expected_bytes=len(bad),
                expected_sha256=hashlib.sha256(bad).hexdigest(),
                max_bytes=1 << 20,
                strip_legacy=False,
            )

    def test_read_answer_payload_legacy_strips_the_frame_once(self) -> None:
        """Verify historical bytes before stripping their terminal frame."""

        framed = _framed(b"legacy body")
        self.path.write_bytes(framed)
        payload = read_answer_payload(
            self.path,
            expected_bytes=len(framed),
            expected_sha256=hashlib.sha256(framed).hexdigest(),
            max_bytes=1 << 20,
            strip_legacy=True,
        )
        self.assertEqual(payload, "legacy body")

    def test_read_answer_payload_can_validate_without_retaining_content(self) -> None:
        """Hash and UTF-8 validate a payload while discarding presentation text."""

        data = b"valid payload"
        self.path.write_bytes(data)
        self.assertIsNone(
            read_answer_payload(
                self.path,
                expected_bytes=len(data),
                expected_sha256=hashlib.sha256(data).hexdigest(),
                max_bytes=1 << 20,
                strip_legacy=False,
                return_content=False,
            )
        )
        invalid = b"\xff" + data
        self.path.write_bytes(invalid)
        with self.assertRaises(AnswerEncodingError):
            read_answer_payload(
                self.path,
                expected_bytes=len(invalid),
                expected_sha256=hashlib.sha256(invalid).hexdigest(),
                max_bytes=1 << 20,
                strip_legacy=False,
                return_content=False,
            )

class _Adapter:
    """Minimal runtime adapter satisfying the service's preparation surface."""

    def __init__(self) -> None:
        """Advertise every capability required by the service fixture."""

        self.capabilities = frozenset(Capability)

    def describe(self):
        """Describe the synthetic runtime used by service tests."""

        return RuntimeInfo("fake", ADAPTER_API_VERSION, self.capabilities)

    def validate(self, config):
        """Accept the fixture's synthetic runtime configuration."""

        return None

    def materialize(self, config, home, *, mcp_servers, skills_root):
        """Finalize empty runtime evidence and return its fixture revision."""

        finalize_runtime_snapshots(Path(home), "cfg-1")
        return "cfg-1"

    def probe(self, config, home):
        """Report the fixture runtime as available."""

        return RuntimeHealth(True, "1", True, None)

    def models(self, config, home):
        """Return the fixture's single supported model."""

        return (ModelInfo("model", "fake model", ("high",)),)

    def limits(self, config, home):
        """Fail if a service answer test unexpectedly asks for live limits."""

        raise AssertionError("service limits must use stored samples")

    def prepare(
        self,
        request,
        role,
        config,
        home,
        agent_dir,
        *,
        resume_session_id=None,
    ):
        """Build the minimal launch plan required to create a stored agent row."""

        return LaunchPlan(
            ("fake",), request.workdir, {}, request.task,
            agent_dir / "runtime.jsonl", {}, agent_dir / "answer.md",
            resume_session_id,
        )

    def launch(self, plan, sink):
        """Fail if the service bypasses its injected launch seam."""

        raise AssertionError("AgentService uses the injected launch seam")


ADAPTER = _Adapter()


class _ServiceFixture:
    """Shared real store/service fixture for answer-format service tests."""

    def setUp(self) -> None:
        """Create a real state store and service around a synthetic adapter."""

        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.workdir = self.root / "work"
        self.runtime_home = self.root / "runtime"
        self.profiles = self.root / "profiles"
        for path in (self.workdir, self.runtime_home, self.profiles):
            path.mkdir()
        (self.profiles / "profile.md").write_text(
            "+++\nwrite = true\n+++\nDo the requested work.\n", encoding="utf-8"
        )
        self.config = Config(
            schema_version=1,
            profiles=ProfilesConfig(self.profiles),
            runtimes={
                "fake": RuntimeConfig(
                    True,
                    f"{__name__}:ADAPTER",
                    Path("/bin/true"),
                    self.runtime_home,
                    ("model",),
                )
            },
        )
        self.store = StateStore.initialize(self.root / "state.db")
        def launch(agent_id, request, role) -> None:
            """Execute the detached supervisor's preparation boundary synchronously."""

            prepare_launch(self.store, self.root, self.config, agent_id, request, role)

        self.service = AgentService(
            self.config,
            self.store,
            self.root,
            launch=launch,
            now=lambda: 100.0,
        )

    def tearDown(self) -> None:
        """Close the service, store, and temporary fixture directory."""

        self.service.close()
        self.store.close()
        self.temporary.cleanup()

    def _start(self) -> str:
        """Create one accepted synthetic agent and return its identifier."""

        request = StartRequest(
            runtime="fake",
            model="model",
            profile="profile",
            task="produce an answer",
            workdir=self.workdir,
        )
        return str(self.service.start(request).agent_id)

    def _record_answer(self, agent_id: str, payload: bytes, *, seal: bool) -> tuple[Path, int, str]:
        """Record a current sealed or historical framed answer for one agent."""

        directory = agent_dir(agent_id, self.root)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "answer.md"
        if seal:
            size, digest = seal_answer(path, payload.decode("utf-8"))
        else:
            path.write_bytes(payload)
            size, digest = len(payload), hashlib.sha256(payload).hexdigest()
        self.store.transition(agent_id, AgentStatus.RUNNING, at=102)
        self.store.transition(
            agent_id,
            AgentStatus.SUCCEEDED,
            outcome=Outcome(
                AgentStatus.SUCCEEDED,
                answer_path=path,
                answer_bytes=size,
                answer_sha256=digest,
            ),
            at=103,
        )
        return path, size, digest


class AnswerServiceFormatTests(_ServiceFixture, unittest.TestCase):
    """Cover the AgentService answer descriptor for both proof formats."""

    def test_new_descriptor_carries_kind_media_type_path_and_proof_version(self) -> None:
        """Expose stable descriptor metadata for a verified current payload."""

        agent_id = self._start()
        payload = b"# Report\n\nall done\n"
        _, size, digest = self._record_answer(agent_id, payload, seal=True)
        view = self.service.answer(agent_id)
        self.assertTrue(view.available)
        self.assertEqual(view.kind, "agent_answer")
        self.assertEqual(view.media_type, "text/markdown; charset=utf-8")
        self.assertEqual(view.relative_path, "answer.md")
        self.assertEqual(view.proof_version, ANSWER_FORMAT_PROOF)
        self.assertEqual(view.size_bytes, size)
        self.assertEqual(view.sha256, digest)
        self.assertEqual(view.content, payload.decode("utf-8"))
        self.assertTrue(view.inline_complete)

    def test_legacy_descriptor_keeps_original_hash_and_strips_frame_once(self) -> None:
        """Keep historical hash evidence while stripping its frame for display."""

        agent_id = self._start()
        framed = _framed(f"legacy body with {DEFAULT_SENTINEL} inside".encode())
        _, size, digest = self._record_answer(agent_id, framed, seal=False)
        view = self.service.answer(agent_id)
        self.assertEqual(view.proof_version, ANSWER_FORMAT_LEGACY)
        self.assertEqual(view.size_bytes, size)
        self.assertEqual(view.sha256, digest)
        self.assertEqual(view.content, f"legacy body with {DEFAULT_SENTINEL} inside")

    def test_corrupted_proof_raises_typed_error_without_legacy_downgrade(self) -> None:
        """Raise a typed proof error for corrupt current metadata."""

        agent_id = self._start()
        path, _, _ = self._record_answer(agent_id, b"proven", seal=True)
        (path.parent / "answer.md.proof.json").write_bytes(b"not json at all")
        with self.assertRaises(AnswerProofError):
            self.service.answer(agent_id)

    def test_contradicting_proof_raises_typed_error(self) -> None:
        """Raise a typed proof error when metadata contradicts the payload."""

        agent_id = self._start()
        path, _, _ = self._record_answer(agent_id, b"proven", seal=True)
        sidecar = path.parent / "answer.md.proof.json"
        document = json.loads(sidecar.read_bytes())
        document["bytes"] = document["bytes"] + 1
        sidecar.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaises(AnswerProofError):
            self.service.answer(agent_id)

    def test_tampered_payload_raises_typed_error(self) -> None:
        """Reject payload changes to either recorded size or recorded hash."""

        agent_id = self._start()
        path, size, _ = self._record_answer(agent_id, b"tamper target", seal=True)
        path.write_bytes(b"tamper targetX")
        with self.assertRaises(AnswerTamperedError):
            self.service.answer(agent_id)
        path.write_bytes(b"tamper tarFet")
        with self.assertRaises(AnswerTamperedError):
            self.service.answer(agent_id)

    def test_missing_payload_raises_typed_error(self) -> None:
        """Raise the missing-artifact error when the stored payload disappears."""

        agent_id = self._start()
        path, _, _ = self._record_answer(agent_id, b"gone soon", seal=True)
        path.unlink()
        with self.assertRaises(AnswerMissingError):
            self.service.answer(agent_id)

    def test_invalid_utf8_raises_typed_error(self) -> None:
        """Reject stored answer bytes that cannot decode as UTF-8."""

        agent_id = self._start()
        self._record_answer(agent_id, b"\xff\xfe\x01", seal=False)
        with self.assertRaises(AnswerEncodingError):
            self.service.answer(agent_id)

    def test_above_inline_limit_payload_is_still_fully_verified(self) -> None:
        """Verify a full payload even when it is too large for inline display."""

        agent_id = self._start()
        payload = b"x" * (1024 * 1024 + 1)
        self._record_answer(agent_id, payload, seal=True)
        view = self.service.answer(agent_id)
        self.assertIsNone(view.content)
        self.assertFalse(view.inline_complete)
        self.assertEqual(view.size_bytes, len(payload))
        path = agent_dir(agent_id, self.root) / "answer.md"
        path.write_bytes(b"y" + payload[1:])
        with self.assertRaises(AnswerTamperedError):
            self.service.answer(agent_id)

    def test_non_inline_payload_still_rejects_invalid_utf8(self) -> None:
        """Incrementally validate UTF-8 even when content is not returned inline."""

        agent_id = self._start()
        self._record_answer(agent_id, b"x" * (1024 * 1024 + 1) + b"\xff", seal=False)
        with self.assertRaises(AnswerEncodingError):
            self.service.answer(agent_id)

    def test_answer_symlink_swap_cannot_escape_the_agent_directory(self) -> None:
        """Anchor payload opening to the agent directory across a final-path swap."""

        agent_id = self._start()
        path, _, _ = self._record_answer(agent_id, b"trusted", seal=True)
        with tempfile.TemporaryDirectory() as directory:
            outside = Path(directory) / "outside.md"
            outside.write_bytes(b"trusted")
            real_open = os.open
            swapped = False

            def swap_then_open(file, flags, mode=0o777, *, dir_fd=None):
                """Swap the owned payload immediately before its relative open."""

                nonlocal swapped
                if not swapped and dir_fd is not None and os.fspath(file) == path.name:
                    path.unlink()
                    path.symlink_to(outside)
                    swapped = True
                if dir_fd is None:
                    return real_open(file, flags, mode)
                return real_open(file, flags, mode, dir_fd=dir_fd)

            with patch("agent_run.verify.os.open", side_effect=swap_then_open):
                with self.assertRaises(PathEscapeError):
                    self.service.answer(agent_id)
        self.assertTrue(swapped)

    def test_missing_current_proof_raises_instead_of_becoming_legacy(self) -> None:
        """Require proof when the durable marker identifies a current payload."""

        agent_id = self._start()
        path, _, _ = self._record_answer(agent_id, b"proved", seal=True)
        answer_proof_path(path).unlink()
        with self.assertRaisesRegex(AnswerProofError, "missing"):
            self.service.answer(agent_id)


if __name__ == "__main__":
    unittest.main()
