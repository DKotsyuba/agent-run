"""Acceptance coverage for clean answer payloads and versioned proofs."""

from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    LaunchPlan,
    ModelInfo,
    RuntimeHealth,
    RuntimeInfo,
)
from agent_run.adapters.home import seal_answer
from agent_run.config import Config, ProfilesConfig, RuntimeConfig
from agent_run.domain import AgentStatus, Outcome, StartRequest
from agent_run.errors import PathEscapeError, ValidationError
from agent_run.paths import agent_dir
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
    inspect_answer,
    read_answer_payload,
    strip_legacy_frame,
)
from agent_run.workflow_executor import (
    AnswerJsonError,
    AnswerSchemaError,
    WorkflowStepExecutor,
    validate_output,
)


def _framed(payload: bytes) -> bytes:
    """Return the exact bytes the historical sentinel sealer would have written."""

    separator = b"" if payload.endswith(b"\n") else b"\n"
    return payload + separator + DEFAULT_SENTINEL.encode("utf-8") + b"\n"


class SealAndProofTests(unittest.TestCase):
    """Cover sealing, proof inspection, legacy framing, and bounded reads."""

    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.path = self.root / "answer.md"

    def tearDown(self) -> None:
        self.temporary.cleanup()

    def test_seal_writes_clean_payload_and_versioned_proof(self) -> None:
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
        self.assertEqual(strip_legacy_frame(_framed(b"payload")), b"payload")
        # A payload trailing newline is not distinguishable from the joining
        # separator, so presentation drops exactly the one known frame.
        self.assertEqual(strip_legacy_frame(_framed(b"payload\n")), b"payload")
        self.assertEqual(strip_legacy_frame(b"no frame here"), b"no frame here")

    def test_legacy_embedded_sentinel_is_preserved(self) -> None:
        framed = _framed(f"see {DEFAULT_SENTINEL} inside".encode())
        stripped = strip_legacy_frame(framed)
        self.assertEqual(stripped, f"see {DEFAULT_SENTINEL} inside".encode())

    def test_inspect_legacy_requires_the_terminal_sentinel(self) -> None:
        self.path.write_bytes(_framed(b"legacy body"))
        proof = inspect_answer(self.path)
        self.assertTrue(proof.complete)
        self.assertEqual(proof.proof_version, ANSWER_FORMAT_LEGACY)
        self.path.write_bytes(b"cut off mid write")
        cut = inspect_answer(self.path)
        self.assertFalse(cut.complete)
        self.assertEqual(cut.proof_version, ANSWER_FORMAT_LEGACY)

    def test_malformed_or_contradicting_proof_never_downgrades(self) -> None:
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

    def test_read_answer_payload_raises_distinct_typed_errors(self) -> None:
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


class ValidateOutputSchemaTests(unittest.TestCase):
    """Cover jsonschema-backed output validation with an empty registry."""

    def test_invalid_json_is_a_typed_error(self) -> None:
        with self.assertRaises(AnswerJsonError):
            validate_output("{not json", {"type": "object"})
        with self.assertRaises(AnswerJsonError):
            validate_output("", {"type": "object"})

    def test_schema_features_beyond_the_old_subset(self) -> None:
        schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["name", "count"],
            "properties": {
                "name": {"type": "string", "pattern": "^[a-z]+$"},
                "count": {"type": "integer", "minimum": 1},
                "mode": {"enum": ["fast", "slow"]},
            },
        }
        validate_output('{"name": "abc", "count": 2, "mode": "fast"}', schema)
        for bad in (
            '{"name": "ABC", "count": 2}',
            '{"name": "abc", "count": 0}',
            '{"name": "abc", "count": 2, "mode": "wild"}',
            '{"name": "abc", "count": 2, "extra": true}',
            '{"count": 2}',
        ):
            with self.assertRaises(ValidationError, msg=bad):
                validate_output(bad, schema)

    def test_local_ref_resolves_without_io(self) -> None:
        schema = {
            "definitions": {"positive": {"type": "integer", "minimum": 1}},
            "type": "object",
            "properties": {"count": {"$ref": "#/definitions/positive"}},
        }
        validate_output('{"count": 3}', schema)
        with self.assertRaises(ValidationError):
            validate_output('{"count": -3}', schema)

    def test_local_ref_resolves_under_an_http_id_without_io(self) -> None:
        schema = {
            "$id": "https://schemas.example.invalid/answer.json",
            "definitions": {"word": {"type": "string"}},
            "type": "object",
            "properties": {"word": {"$ref": "#/definitions/word"}},
        }
        validate_output('{"word": "fine"}', schema)
        with self.assertRaises(ValidationError):
            validate_output('{"word": 5}', schema)

    def test_remote_and_file_refs_fail_without_io(self) -> None:
        with self.assertRaises(AnswerSchemaError):
            validate_output("{}", {"$ref": "https://schemas.example.invalid/x.json"})
        with self.assertRaises(AnswerSchemaError):
            validate_output("{}", {"$ref": "file:///etc/passwd"})

    def test_unknown_explicit_dialect_is_rejected_deterministically(self) -> None:
        schema = {"$schema": "https://example.invalid/dialect", "type": "object"}
        with self.assertRaises(AnswerSchemaError):
            validate_output("{}", schema)

    def test_explicit_supported_dialect_is_honored(self) -> None:
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "string",
        }
        validate_output('"text"', schema)
        with self.assertRaises(ValidationError):
            validate_output("5", schema)

    def test_malformed_schema_is_a_typed_error(self) -> None:
        with self.assertRaises(AnswerSchemaError):
            validate_output("{}", {"type": "not-a-type"})
        with self.assertRaises(AnswerSchemaError):
            validate_output("{}", "not a schema")


class _Adapter:
    """Minimal runtime adapter satisfying the service's preparation surface."""

    def __init__(self) -> None:
        self.capabilities = frozenset(Capability)

    def describe(self):
        return RuntimeInfo("fake", ADAPTER_API_VERSION, self.capabilities)

    def validate(self, config):
        return None

    def materialize(self, config, home, *, mcp_servers, skills_root):
        return "cfg-1"

    def probe(self, config, home):
        return RuntimeHealth(True, "1", True, None)

    def models(self, config, home):
        return (ModelInfo("model", "fake model", ("high",)),)

    def limits(self, config, home):
        raise AssertionError("service limits must use stored samples")

    def prepare(self, request, profile, config, home, agent_dir, *, mcp_servers):
        return LaunchPlan(
            ("fake",), request.workdir, {}, request.task,
            agent_dir / "runtime.jsonl", {}, agent_dir / "answer.md",
        )

    def launch(self, plan, sink):
        raise AssertionError("AgentService uses the injected launch seam")


ADAPTER = _Adapter()


class _ServiceFixture:
    """Shared real store/service fixture for answer-format service tests."""

    def setUp(self) -> None:
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
        self.service = AgentService(
            self.config,
            self.store,
            self.root,
            launch=lambda *args: None,
            now=lambda: 100.0,
        )

    def tearDown(self) -> None:
        self.service.close()
        self.store.close()
        self.temporary.cleanup()

    def _start(self) -> str:
        request = StartRequest(
            runtime="fake",
            model="model",
            profile="profile",
            task="produce an answer",
            workdir=self.workdir,
        )
        return str(self.service.start(request).agent_id)

    def _record_answer(self, agent_id: str, payload: bytes, *, seal: bool) -> tuple[Path, int, str]:
        directory = agent_dir(agent_id, self.root)
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
        agent_id = self._start()
        framed = _framed(f"legacy body with {DEFAULT_SENTINEL} inside".encode())
        _, size, digest = self._record_answer(agent_id, framed, seal=False)
        view = self.service.answer(agent_id)
        self.assertEqual(view.proof_version, ANSWER_FORMAT_LEGACY)
        self.assertEqual(view.size_bytes, size)
        self.assertEqual(view.sha256, digest)
        self.assertEqual(view.content, f"legacy body with {DEFAULT_SENTINEL} inside")

    def test_corrupted_proof_raises_typed_error_without_legacy_downgrade(self) -> None:
        agent_id = self._start()
        path, _, _ = self._record_answer(agent_id, b"proven", seal=True)
        (path.parent / "answer.md.proof.json").write_bytes(b"not json at all")
        with self.assertRaises(AnswerProofError):
            self.service.answer(agent_id)

    def test_contradicting_proof_raises_typed_error(self) -> None:
        agent_id = self._start()
        path, _, _ = self._record_answer(agent_id, b"proven", seal=True)
        sidecar = path.parent / "answer.md.proof.json"
        document = json.loads(sidecar.read_bytes())
        document["bytes"] = document["bytes"] + 1
        sidecar.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaises(AnswerProofError):
            self.service.answer(agent_id)

    def test_tampered_payload_raises_typed_error(self) -> None:
        agent_id = self._start()
        path, size, _ = self._record_answer(agent_id, b"tamper target", seal=True)
        path.write_bytes(b"tamper targetX")
        with self.assertRaises(AnswerTamperedError):
            self.service.answer(agent_id)
        path.write_bytes(b"tamper tarFet")
        with self.assertRaises(AnswerTamperedError):
            self.service.answer(agent_id)

    def test_missing_payload_raises_typed_error(self) -> None:
        agent_id = self._start()
        path, _, _ = self._record_answer(agent_id, b"gone soon", seal=True)
        path.unlink()
        with self.assertRaises(AnswerMissingError):
            self.service.answer(agent_id)

    def test_invalid_utf8_raises_typed_error(self) -> None:
        agent_id = self._start()
        self._record_answer(agent_id, b"\xff\xfe\x01", seal=False)
        with self.assertRaises(AnswerEncodingError):
            self.service.answer(agent_id)


class _ReplayService:
    """Service double replaying one real terminal agent through a real service."""

    def __init__(self, real: AgentService, agent_id: str) -> None:
        self._real = real
        self._agent_id = agent_id

    def start(self, request):
        return SimpleNamespace(agent_id=self._agent_id)

    def get(self, agent_id):
        return self._real.get(agent_id)

    def answer(self, agent_id):
        return self._real.answer(agent_id)

    def cancel(self, agent_id):
        return None


class ExecutorFullPayloadTests(_ServiceFixture, unittest.TestCase):
    """Run seal -> store -> service -> executor with real artifacts."""

    def setUp(self) -> None:
        super().setUp()
        self.run_id = self.store.create_workflow_run("workflow", "digest")
        self.store.claim_workflow_run(self.run_id, "1 test")

    def _executor(self, service) -> WorkflowStepExecutor:
        return WorkflowStepExecutor(
            self.root,
            self.store,
            self.run_id,
            service=service,
            sleep=lambda _: None,
            poll_seconds=0,
        )

    def test_above_inline_limit_json_still_validates(self) -> None:
        agent_id = self._start()
        payload = json.dumps(
            {"ok": True, "items": ["entry-%04d" % index for index in range(64)]}
        ).encode("utf-8")
        self._record_answer(agent_id, payload, seal=True)
        bounded = AgentService(
            self.config,
            self.store,
            self.root,
            launch=lambda *args: None,
            now=lambda: 100.0,
            max_inline_answer_bytes=16,
        )
        view = bounded.answer(agent_id)
        self.assertIsNone(view.content)
        self.assertFalse(view.inline_complete)
        schema = {
            "type": "object",
            "required": ["ok", "items"],
            "properties": {
                "ok": {"type": "boolean"},
                "items": {"type": "array", "items": {"type": "string"}},
            },
        }
        spec = {
            "runtime": "fake",
            "model": "model",
            "profile": "profile",
            "task": "produce an answer",
            "workdir": str(self.workdir),
            "output_schema": schema,
        }
        result = self._executor(_ReplayService(bounded, agent_id))("s1", spec)
        self.assertEqual(result["status"], "succeeded")
        self.assertNotIn("answer", result)

    def test_invalid_json_fails_the_step_with_typed_message(self) -> None:
        agent_id = self._start()
        self._record_answer(agent_id, b"{not json", seal=True)
        schema = {"type": "object"}
        spec = {
            "runtime": "fake",
            "model": "model",
            "profile": "profile",
            "task": "produce an answer",
            "workdir": str(self.workdir),
            "output_schema": schema,
        }
        result = self._executor(_ReplayService(self.service, agent_id))("s1", spec)
        self.assertEqual(result["failure_kind"], "step_output_invalid")
        self.assertIn("not valid JSON", result["failure_params"]["message"])

    def test_tampered_payload_fails_closed(self) -> None:
        agent_id = self._start()
        path, _, _ = self._record_answer(agent_id, b'{"ok": true}', seal=True)
        path.write_bytes(b'{"ok": false}')
        schema = {"type": "object"}
        spec = {
            "runtime": "fake",
            "model": "model",
            "profile": "profile",
            "task": "produce an answer",
            "workdir": str(self.workdir),
            "output_schema": schema,
        }
        result = self._executor(_ReplayService(self.service, agent_id))("s1", spec)
        self.assertEqual(result["failure_kind"], "step_output_invalid")


if __name__ == "__main__":
    unittest.main()
