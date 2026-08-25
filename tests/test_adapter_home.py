import hashlib
import stat
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.home import (
    content_hash,
    create_symlink_bridge,
    write_managed_file,
)
from agent_run.errors import PathEscapeError, ValidationError


class AdapterHomeTests(unittest.TestCase):
    def test_managed_files_are_private_atomic_and_content_hashed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory) / "generated"
            digest = write_managed_file(home, "settings/config.toml", "first")
            target = home / "settings/config.toml"
            self.assertEqual(digest, hashlib.sha256(b"first").hexdigest())
            self.assertEqual(content_hash("first"), digest)
            self.assertEqual(target.read_text(encoding="utf-8"), "first")
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
            write_managed_file(home, "settings/config.toml", b"second")
            self.assertEqual(target.read_bytes(), b"second")
            self.assertEqual(list(target.parent.glob("*.tmp")), [])

    def test_managed_paths_refuse_traversal_and_symlink_escape(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            home = Path(directory).resolve()
            (home / "linked").symlink_to(outside, target_is_directory=True)
            with self.assertRaises(PathEscapeError):
                write_managed_file(home, "../outside", "no")
            with self.assertRaises(PathEscapeError):
                write_managed_file(home, "linked/outside", "no")

    def test_symlink_bridges_are_explicit_and_validated(self) -> None:
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as source_dir:
            home = Path(directory) / "generated"
            source = Path(source_dir) / "auth.json"
            source.write_text("{}", encoding="utf-8")
            bridge = create_symlink_bridge(home, "auth/auth.json", source)
            self.assertTrue(bridge.is_symlink())
            self.assertEqual(bridge.resolve(strict=True), source.resolve(strict=True))
            with self.assertRaises(PathEscapeError):
                create_symlink_bridge(home, "../auth.json", source)
            with self.assertRaises(ValidationError):
                create_symlink_bridge(home, "auth/other.json", "relative")


if __name__ == "__main__":
    unittest.main()
