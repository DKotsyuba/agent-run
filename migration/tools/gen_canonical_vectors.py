#!/usr/bin/env python3
"""Generate Python-authoritative canonical-JSON test vectors.

Dev-only tool for migration plan backlog M08 / ADR A12 (Python-compatible
canonical serialization for the Rust port). It calls the *actual* Python
functions that hash and persist documents (role plan config revisions,
skill-tree revisions, request replay JSON, context-receipt keys, capacity
route payloads, answer proofs) and dumps their exact bytes so
`rust/tests/canonical.rs` can assert the Rust port reproduces them
byte-for-byte.

Run with the pinned interpreter from the worktree root:

    PYTHONPATH=src /Users/pluto/projects/agent-run/.venv-py314/bin/python \\
        migration/tools/gen_canonical_vectors.py \\
        > rust/tests/fixtures/canonical/vectors.json

Not shipped; not imported by the package or its tests.
"""

from __future__ import annotations

import base64
import hashlib
import json
import struct
import sys
import tempfile
from pathlib import Path

if "src" not in sys.path[0:1]:
    sys.path.insert(0, "src")

from agent_run.adapters.snapshot_tree import _read_tree, tree_revision  # noqa: E402
from agent_run.domain import OrchestratorRef, StartRequest  # noqa: E402
from agent_run.effective_policy import Constraint  # noqa: E402
from agent_run.role_plan import (  # noqa: E402
    ResolvedMcp,
    ResolvedRolePlan,
    ResolvedSkill,
    _canonical_payload,
)
from agent_run.state.capacity import _route_payload  # noqa: E402
from agent_run.state.db import (  # noqa: E402
    _CONTEXT_RECEIPT_VERSION,
    encode_context_components,
    request_json,
)
from agent_run.verify import (  # noqa: E402
    ANSWER_FORMAT_PROOF,
    ANSWER_KIND,
    ANSWER_MEDIA_TYPE,
    answer_proof_document,
)


def _dump(value: object, *, ensure_ascii: bool) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=ensure_ascii
    ).encode("utf-8")


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def scalar(name: str, *, ensure_ascii: bool, kind: str, **fields: object) -> dict:
    return {"name": name, "ensure_ascii": ensure_ascii, "kind": kind, **fields}


def float_case(name: str, value: float, *, ensure_ascii: bool = False) -> dict:
    data = _dump(value, ensure_ascii=ensure_ascii)
    return scalar(
        name,
        ensure_ascii=ensure_ascii,
        kind="float",
        bits_hex=f"{struct.unpack('<Q', struct.pack('<d', value))[0]:016x}",
        python_repr=repr(value),
        expected_base64=_b64(data),
        expected_sha256=_sha(data),
    )


def int_case(name: str, value: int, *, ensure_ascii: bool = False) -> dict:
    data = _dump(value, ensure_ascii=ensure_ascii)
    return scalar(
        name,
        ensure_ascii=ensure_ascii,
        kind="int",
        decimal=str(value),
        expected_base64=_b64(data),
        expected_sha256=_sha(data),
    )


def string_case(name: str, value: str, *, ensure_ascii: bool) -> dict:
    data = _dump(value, ensure_ascii=ensure_ascii)
    return scalar(
        name,
        ensure_ascii=ensure_ascii,
        kind="string",
        value=value,
        expected_base64=_b64(data),
        expected_sha256=_sha(data),
    )


def json_case(name: str, value: object, *, ensure_ascii: bool) -> dict:
    """For null/bool/nested structures with only string/int/bool/null leaves."""
    data = _dump(value, ensure_ascii=ensure_ascii)
    return scalar(
        name,
        ensure_ascii=ensure_ascii,
        kind="json",
        value=value,
        expected_base64=_b64(data),
        expected_sha256=_sha(data),
    )


def build_primitives() -> list[dict]:
    cases: list[dict] = []
    for ensure_ascii in (True, False):
        suffix = "ascii" if ensure_ascii else "raw"
        cases.append(json_case(f"null_{suffix}", None, ensure_ascii=ensure_ascii))
        cases.append(json_case(f"bool_true_{suffix}", True, ensure_ascii=ensure_ascii))
        cases.append(json_case(f"bool_false_{suffix}", False, ensure_ascii=ensure_ascii))
        cases.append(string_case(f"string_empty_{suffix}", "", ensure_ascii=ensure_ascii))
        cases.append(
            string_case(f"string_ascii_{suffix}", "hello world", ensure_ascii=ensure_ascii)
        )
        cases.append(
            string_case(
                f"string_quote_backslash_{suffix}",
                'He said "hi"\\bye',
                ensure_ascii=ensure_ascii,
            )
        )
        cases.append(
            string_case(
                f"string_all_control_chars_{suffix}",
                "".join(chr(c) for c in range(0x20)),
                ensure_ascii=ensure_ascii,
            )
        )
        cases.append(string_case(f"string_del_{suffix}", "a\x7fb", ensure_ascii=ensure_ascii))
        cases.append(
            string_case(f"string_unicode_bmp_{suffix}", "héllo日本語Спасибо", ensure_ascii=ensure_ascii)
        )
        cases.append(
            string_case(
                f"string_astral_emoji_{suffix}", "before\U0001F600after", ensure_ascii=ensure_ascii
            )
        )
        cases.append(
            string_case(
                f"string_astral_multi_{suffix}",
                "\U0001F600\U0001F4A9\U0002F800",
                ensure_ascii=ensure_ascii,
            )
        )
        cases.append(int_case(f"int_zero_{suffix}", 0, ensure_ascii=ensure_ascii))
        cases.append(int_case(f"int_negative_one_{suffix}", -1, ensure_ascii=ensure_ascii))
        cases.append(
            int_case(f"int_i64_max_{suffix}", 2**63 - 1, ensure_ascii=ensure_ascii)
        )
        cases.append(int_case(f"int_i64_min_{suffix}", -(2**63), ensure_ascii=ensure_ascii))
        cases.append(
            int_case(f"int_u64_max_{suffix}", 2**64 - 1, ensure_ascii=ensure_ascii)
        )
        for label, value in [
            ("zero", 0.0),
            ("neg_zero", -0.0),
            ("one", 1.0),
            ("neg_one_half", -1.5),
            ("pi_ish", -3.14159),
            ("hundred", 100.0),
            ("one_e15", 1e15),
            ("one_e16", 1e16),
            ("boundary_16_digits", 1234567890123456.0),
            ("boundary_17_digits", 12345678901234567.0),
            ("one_e-4", 1e-4),
            ("one_e-5", 1e-5),
            ("small_frac", 0.1),
            ("large_sci", 1.1e300),
            ("smallest_subnormal", 5e-324),
        ]:
            cases.append(float_case(f"float_{label}_{suffix}", value, ensure_ascii=ensure_ascii))
        cases.append(
            json_case(
                f"nested_ordering_{suffix}",
                {
                    "z": 1,
                    "a": [3, 2, 1],
                    "denis": {"b": True, "a": None, "1": "one", "10": "ten"},
                    "Юля": "cyrillic key",
                    "apple": -42,
                },
                ensure_ascii=ensure_ascii,
            )
        )
    return cases


def build_documents() -> list[dict]:
    docs: list[dict] = []

    # 1. role_plan.py config_revision -- ResolvedRolePlan._canonical_payload,
    #    hashed with content_hash(json.dumps(..., sort_keys=True, separators=(",", ":"))).
    #    Deliberately includes unicode in prompt/role fields to exercise
    #    ensure_ascii=True escaping inside a real persisted document.
    plan = ResolvedRolePlan(
        role_name="explorer-role",
        role_revision="rev-1",
        prompt="Explore the code—carefully. Emoji: \U0001F600. Ends.",
        write=True,
        network=False,
        allow_external_read_roots=True,
        read_roots=(Path("/tmp/one"), Path("/tmp/two")),
        skills=(
            ResolvedSkill(id="agent-ide", revision="a" * 64),
            ResolvedSkill(id="python-runtime", revision="b" * 64),
        ),
        mcp=(
            ResolvedMcp(
                id="codegraph",
                transport="stdio",
                command="codegraph-mcp",
                args=("--home", "/tmp"),
                env_from=("HOME",),
                approval_mode="auto",
            ),
        ),
        required_constraints=frozenset(
            {Constraint.WEB_TOOLS_DISABLED, Constraint.FILESYSTEM_WRITE_ISOLATION}
        ),
        auth_mode="global",
        auth_reference=None,
        config_revision="",
    )
    payload = _canonical_payload(plan)
    document = _dump(payload, ensure_ascii=True)
    sha = hashlib.sha256(document).hexdigest()
    docs.append(
        {
            "name": "role_plan_config_revision",
            "source": "agent_run.role_plan._canonical_payload + content_hash(json.dumps(..., sort_keys=True, separators=(',', ':')))",
            "ensure_ascii": True,
            "payload": payload,
            "trailing_newline": False,
            "document_base64": _b64(document),
            "sha256": sha,
        }
    )

    # 2. verify.py answer_proof_document -- literal persisted proof sidecar bytes
    #    (the function appends a trailing b"\n" after the canonical JSON).
    answer_name, answer_bytes_count, answer_sha = "answer.txt", 1234, "d" * 64
    proof_bytes = answer_proof_document(answer_name, answer_bytes_count, answer_sha)
    proof_payload = {
        "kind": ANSWER_KIND,
        "media_type": ANSWER_MEDIA_TYPE,
        "proof_version": ANSWER_FORMAT_PROOF,
        "answer": answer_name,
        "bytes": answer_bytes_count,
        "sha256": answer_sha,
    }
    assert _dump(proof_payload, ensure_ascii=True) + b"\n" == proof_bytes, (
        "reconstructed answer-proof payload does not match answer_proof_document's own bytes"
    )
    docs.append(
        {
            "name": "answer_proof_document",
            "source": "agent_run.verify.answer_proof_document",
            "ensure_ascii": True,
            "payload": proof_payload,
            "trailing_newline": True,
            "document_base64": _b64(proof_bytes),
            "sha256": hashlib.sha256(proof_bytes).hexdigest(),
        }
    )

    # 3. state/db.py encode_context_components -- ensure_ascii=False, unicode value.
    components = {"skill:python-runtime": "rev-abc123", "unicode-src": "héllo-Спасибо"}
    context_text = encode_context_components(components)
    context_bytes = context_text.encode("utf-8")
    context_payload = {"v": _CONTEXT_RECEIPT_VERSION, "components": components}
    assert _dump(context_payload, ensure_ascii=False) == context_bytes, (
        "reconstructed context-receipt payload does not match encode_context_components's own bytes"
    )
    docs.append(
        {
            "name": "encode_context_components",
            "source": "agent_run.state.db.encode_context_components",
            "ensure_ascii": False,
            "payload": context_payload,
            "trailing_newline": False,
            "document_base64": _b64(context_bytes),
            "sha256": hashlib.sha256(context_bytes).hexdigest(),
        }
    )

    # 4. state/db.py request_json -- the byte-for-byte replay comparison payload.
    workdir = Path.cwd()
    request = StartRequest(
        runtime="claude",
        model="sonnet-5",
        profile="implement",
        task="Fix the édge case \U0001F600",
        workdir=workdir,
        write=True,
        effort="high",
        timeout_seconds=1800.5,
        read_roots=(),
        output_schema=None,
        orchestrator=OrchestratorRef(
            transport="claude-code", external_session_id="sess-1", external_turn_id="turn-2"
        ),
        request_id="req-unicode-é",
        fast=True,
        account="personal2",
        required_constraints=frozenset({Constraint.EXTERNAL_NETWORK_ISOLATION}),
    )
    request_text = request_json(request)
    request_bytes = request_text.encode("utf-8")
    docs.append(
        {
            "name": "request_json_replay",
            "source": "agent_run.state.db.request_json",
            "ensure_ascii": False,
            # `request_json`'s dict literal is reconstructed via a
            # dumps->loads round trip rather than duplicated by hand; JSON
            # round-trips int/float/str/bool/null exactly, so this is still
            # exactly what was hashed, just recovered as plain data.
            "payload": json.loads(request_text),
            "trailing_newline": False,
            "document_base64": _b64(request_bytes),
            "sha256": hashlib.sha256(request_bytes).hexdigest(),
        }
    )

    # 5. state/capacity.py _route_payload -- ensure_ascii=False, floats present.
    route_value = {
        "lane": "codex",
        "percent_remaining": 42.5,
        "targets": ["personal1", "personal2"],
    }
    route_text = _route_payload(route_value)
    route_bytes = route_text.encode("utf-8")
    docs.append(
        {
            "name": "capacity_route_payload",
            "source": "agent_run.state.capacity._route_payload",
            "ensure_ascii": False,
            "payload": route_value,
            "trailing_newline": False,
            "document_base64": _b64(route_bytes),
            "sha256": hashlib.sha256(route_bytes).hexdigest(),
        }
    )

    # 6. adapters/snapshot_tree.py tree_revision -- content_hash(json.dumps(
    #    content_entries, separators=(",", ":"))); no sort_keys since the
    #    payload is a list, not an object.
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        (root / "sub").mkdir()
        (root / "sub" / "a.txt").write_bytes(b"alpha content\n")
        (root / "b.txt").write_bytes(b"beta \xc3\xa9 content\n")
        entries, _files = _read_tree(root)
        content_entries = [
            [entry["path"], entry["type"]]
            if entry["type"] == "directory"
            else [entry["path"], entry["type"], entry["bytes"], entry["sha256"]]
            for entry in entries
        ]
        tree_text = json.dumps(content_entries, separators=(",", ":"))
        tree_bytes = tree_text.encode("utf-8")
        expected_revision = hashlib.sha256(tree_bytes).hexdigest()
        actual_revision = tree_revision(root)
        assert actual_revision == expected_revision, "tree_revision drifted from its own algorithm"
        docs.append(
            {
                "name": "skill_tree_revision",
                "source": "agent_run.adapters.snapshot_tree.tree_revision",
                "ensure_ascii": True,
                "payload": content_entries,
                "trailing_newline": False,
                "document_base64": _b64(tree_bytes),
                "sha256": expected_revision,
            }
        )

    return docs


def main() -> None:
    vectors = {"primitives": build_primitives(), "documents": build_documents()}
    json.dump(vectors, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
