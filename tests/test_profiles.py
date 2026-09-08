import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.errors import PathEscapeError, ValidationError
from agent_run.profiles import (
    AgentProfile,
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


    def test_canonical_role_owns_every_grant_and_asset_selection(self) -> None:
        """Load a complete revisioned role without runtime-specific assignment."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "implement.md").write_text(
                """+++
revision = "1"
write = true
network = false
allow_external_read_roots = true
skills = ["lsp-first", "document-code"]
mcp = ["agent-lsp"]
required_constraints = ["plugin_immutability"]
+++
Implement and verify the requested change.
""",
                encoding="utf-8",
            )
            profile = load_profile(
                root, "implement", requested_write=False, read_roots=(root,)
            )

        self.assertTrue(profile.canonical)
        self.assertTrue(profile.write)
        self.assertEqual(profile.revision, "1")
        self.assertEqual(profile.skills, ("lsp-first", "document-code"))
        self.assertEqual(profile.mcp, ("agent-lsp",))
        self.assertEqual(
            {item.value for item in profile.required_constraints},
            {"plugin_immutability"},
        )

    def test_incomplete_or_unrevisioned_canonical_role_is_rejected(self) -> None:
        """Fail closed instead of mixing legacy and canonical declarations."""

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            (root / "mixed.md").write_text(
                "+++\nwrite = false\nskills = [\"code-reading\"]\n+++\nReview.\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValidationError, "require a revision"):
                load_profile(root, "mixed")

            (root / "partial.md").write_text(
                "+++\nrevision = \"1\"\nwrite = false\n+++\nReview.\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValidationError, "incomplete"):
                load_profile(root, "partial")


if __name__ == "__main__":
    unittest.main()
