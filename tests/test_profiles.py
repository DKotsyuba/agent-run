import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.errors import PathEscapeError, ValidationError
from agent_run.profiles import load_profile, normalize_read_roots, profile_path


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
            self.assertEqual(profile.read_roots, (root,))

            (root / "implement.md").write_text(
                "+++\nwrite = true\n+++\nImplement.\n", encoding="utf-8"
            )
            self.assertFalse(
                load_profile(root, "implement", requested_write=False).write
            )
            self.assertTrue(load_profile(root, "implement", requested_write=True).write)

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


if __name__ == "__main__":
    unittest.main()
