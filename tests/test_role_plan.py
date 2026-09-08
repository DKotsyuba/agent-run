"""Checks for runtime-neutral canonical role resolution."""

from __future__ import annotations

import json
import copy
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.config import McpConfig
from agent_run.errors import ValidationError
from agent_run.profiles import load_profile
from agent_run.role_plan import ResolvedRolePlan, resolve_role_plan


class ResolvedRolePlanTests(unittest.TestCase):
    """Verify one role resolves identically before adapter translation."""

    def test_resolves_serializable_revisioned_role(self) -> None:
        """Bind prompt, grants, skill revisions, MCP, auth reference, and hash."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            profiles = root / "profiles"
            profiles.mkdir()
            skills = root / "skills"
            skill = skills / "code-reading"
            skill.mkdir(parents=True)
            (skill / "SKILL.md").write_text("Read code.\n", encoding="utf-8")
            (profiles / "review.md").write_text(
                """+++
revision = "3"
write = false
network = false
allow_external_read_roots = true
skills = ["code-reading"]
mcp = ["codegraph"]
required_constraints = []
+++
Review the assigned change.
""",
                encoding="utf-8",
            )
            profile = load_profile(profiles, "review", read_roots=(root,))
            plan = resolve_role_plan(
                profile,
                skills_root=skills,
                mcp_catalog={
                    "codegraph": McpConfig(
                        "stdio", Path("/bin/echo"), ("serve",), ("PATH",)
                    )
                },
                auth_mode="account",
                auth_reference="personal2",
            )

            payload = plan.to_payload()
            json.dumps(payload)
            self.assertEqual(payload["role_revision"], "3")
            self.assertEqual(payload["auth"], {"mode": "account", "reference": "personal2"})
            self.assertEqual(payload["skills"][0]["id"], "code-reading")
            self.assertEqual(
                payload["skills"][0]["revision"],
                "29ec0f76a446221ad66bb45110a822e8b2803b1d953ed99dea296614339a8746",
            )
            self.assertEqual(len(plan.config_revision), 64)
            self.assertEqual(type(plan).from_payload(payload), plan)
            self.assertEqual(
                plan.config_revision,
                resolve_role_plan(
                    profile,
                    skills_root=skills,
                    mcp_catalog={
                        "codegraph": McpConfig(
                            "stdio", Path("/bin/echo"), ("serve",), ("PATH",)
                        )
                    },
                    auth_mode="account",
                    auth_reference="personal2",
                ).config_revision,
            )

    def test_from_payload_rejects_malformed_or_tampered_documents(self) -> None:
        """Reject wrong shapes, grants, paths, assets, auth, and revision."""

        payload = json.loads(
            (Path(__file__).parent / "fixtures" / "role_plan_7bbd43b.json").read_text(
                encoding="utf-8"
            )
        )
        self.assertEqual(
            ResolvedRolePlan.from_payload(payload).to_payload(), payload
        )
        cases = []
        extra = copy.deepcopy(payload)
        extra["extra"] = True
        cases.append(extra)
        missing = copy.deepcopy(payload)
        del missing["prompt"]
        cases.append(missing)
        nested_extra = copy.deepcopy(payload)
        nested_extra["grants"]["extra"] = False
        cases.append(nested_extra)
        wrong_bool = copy.deepcopy(payload)
        wrong_bool["grants"]["write"] = 1
        cases.append(wrong_bool)
        bad_root = copy.deepcopy(payload)
        bad_root["grants"]["read_roots"] = ["relative"]
        cases.append(bad_root)
        bad_skill = copy.deepcopy(payload)
        bad_skill["skills"][0]["revision"] = "bad"
        cases.append(bad_skill)
        duplicate_skill = copy.deepcopy(payload)
        duplicate_skill["skills"].append(copy.deepcopy(duplicate_skill["skills"][0]))
        cases.append(duplicate_skill)
        bad_mcp = copy.deepcopy(payload)
        bad_mcp["mcp"] = [
            {"id": "tool", "transport": "http", "command": "relative", "args": [], "env_from": []}
        ]
        cases.append(bad_mcp)
        bad_env = copy.deepcopy(payload)
        bad_env["mcp"] = [
            {"id": "tool", "transport": "stdio", "command": "/bin/echo", "args": [], "env_from": ["bad-name"]}
        ]
        cases.append(bad_env)
        bad_constraint = copy.deepcopy(payload)
        bad_constraint["required_constraints"] = ["unknown"]
        cases.append(bad_constraint)
        bad_auth = copy.deepcopy(payload)
        bad_auth["auth"] = {"mode": "global", "reference": "account"}
        cases.append(bad_auth)
        bad_revision = copy.deepcopy(payload)
        bad_revision["config_revision"] = "0" * 64
        cases.append(bad_revision)
        for malformed in cases:
            with self.subTest(malformed=malformed), self.assertRaises(ValidationError):
                ResolvedRolePlan.from_payload(malformed)

    def test_missing_skill_or_mcp_fails_closed(self) -> None:
        """Reject incomplete catalogs before any adapter sees the role."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            profiles = root / "profiles"
            profiles.mkdir()
            skills = root / "skills"
            skills.mkdir()
            (profiles / "review.md").write_text(
                """+++
revision = "1"
write = false
network = false
allow_external_read_roots = false
skills = ["missing"]
mcp = ["missing"]
required_constraints = []
+++
Review.
""",
                encoding="utf-8",
            )
            profile = load_profile(profiles, "review")
            with self.assertRaisesRegex(ValidationError, "skill is not available"):
                resolve_role_plan(profile, skills_root=skills, mcp_catalog={})


if __name__ == "__main__":
    unittest.main()
