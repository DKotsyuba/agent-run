"""Checks for runtime-neutral canonical role resolution."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.config import McpConfig
from agent_run.errors import ValidationError
from agent_run.profiles import load_profile
from agent_run.role_plan import resolve_role_plan


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
            self.assertEqual(len(plan.config_revision), 64)
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
