import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.errors import PathEscapeError, ValidationError
from agent_run.profiles import (
    AgentProfile,
    assign_role,
    load_profile,
    normalize_read_roots,
    profile_path,
)


class ProfileTests(unittest.TestCase):
    def test_named_profile_body_and_write_can_only_narrow(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "review.md").write_text(
                "+++\nwrite = false\n+++\nReview carefully.\n", encoding="utf-8"
            )
            profile = load_profile(root, "review", requested_write=True, read_roots=(root,))
            self.assertEqual(profile.body, "Review carefully.")
            self.assertFalse(profile.write)
            self.assertFalse(profile.network)
            self.assertEqual(profile.read_roots, (root,))

            (root / "implement.md").write_text(
                "+++\nwrite = true\n+++\nImplement.\n", encoding="utf-8"
            )
            self.assertFalse(
                load_profile(root, "implement", requested_write=False).write
            )
            self.assertTrue(load_profile(root, "implement", requested_write=True).write)

            (root / "research.md").write_text(
                "+++\nnetwork = true\n+++\nResearch.\n", encoding="utf-8"
            )
            self.assertTrue(load_profile(root, "research").network)

    def test_profile_names_and_symlink_escape_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            root = Path(directory).resolve()
            outside_path = Path(outside) / "role.md"
            outside_path.write_text("Outside", encoding="utf-8")
            (root / "linked.md").symlink_to(outside_path)
            with self.assertRaises(ValidationError):
                profile_path(root, "../role")
            with self.assertRaises(PathEscapeError):
                profile_path(root, "linked")

    def test_read_roots_are_resolved_deduplicated_and_minimal(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            child = root / "child"
            child.mkdir()
            alias = root / "alias"
            alias.symlink_to(child, target_is_directory=True)
            self.assertEqual(normalize_read_roots((child, alias, root)), (root,))
            with self.assertRaises(ValidationError):
                normalize_read_roots((Path("relative"),))
            with self.assertRaises(ValidationError):
                normalize_read_roots((root / "missing",))


class RoleAssignmentTests(unittest.TestCase):
    """The profile is the runtime adapter that assigns a ``role-*`` contract."""

    def test_a_shipped_role_is_assigned_and_the_body_is_kept(self) -> None:
        profile = AgentProfile("review", "Review carefully.", False, ())
        assigned = assign_role(profile, "claude", ("code-reading", "role-review"))
        self.assertTrue(assigned.body.startswith("Review carefully."))
        self.assertIn("Your assigned role is role-review.", assigned.body)
        self.assertIn("Load the role-review skill now", assigned.body)
        # Permissions are the profile's business; assignment never touches them.
        self.assertEqual((assigned.name, assigned.write), (profile.name, profile.write))

    def test_no_matching_skill_leaves_the_profile_untouched(self) -> None:
        profile = AgentProfile("review", "Review carefully.", False, ())
        # An unshipped role must never be named: the contract is explicit-only,
        # and pointing at a skill the child cannot load is a broken instruction.
        self.assertIs(assign_role(profile, "claude", ("code-reading",)), profile)
        self.assertIs(assign_role(profile, "opencode", ()), profile)

    def test_codex_assigns_the_research_role_when_shipped(self) -> None:
        profile = AgentProfile("research", "Research.", False, ())
        self.assertIn(
            "role-research", assign_role(profile, "codex", ("role-research",)).body
        )


if __name__ == "__main__":
    unittest.main()
