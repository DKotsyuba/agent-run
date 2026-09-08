import errno
import hashlib
import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.home import (
    content_hash,
    create_symlink_bridge,
    write_managed_file,
)
from agent_run.errors import PathEscapeError, ValidationError


class AdapterHomeTests(unittest.TestCase):
    def test_new_parent_is_synced_before_file_publication(self) -> None:
        """Persist a newly created managed directory before publishing beneath it."""

        with tempfile.TemporaryDirectory() as directory:
            events = []
            real_replace = os.replace

            def record_fsync(descriptor: int) -> None:
                """Record file-versus-directory synchronization order."""

                events.append("dir" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file")

            def record_replace(source, destination, **kwargs) -> None:
                """Record and perform the atomic replacement."""

                events.append("replace")
                real_replace(source, destination, **kwargs)

            with patch("agent_run.adapters.home.os.fsync", side_effect=record_fsync), patch(
                "agent_run.adapters.home.os.replace", side_effect=record_replace
            ):
                write_managed_file(Path(directory).resolve(), "nested/answer.md", "done")
            self.assertEqual(events, ["dir", "file", "replace", "dir"])

    def test_managed_replace_fsyncs_file_then_parent_directory(self) -> None:
        """Publish a replacement only after its bytes and directory entry are synced."""

        with tempfile.TemporaryDirectory() as directory:
            events = []
            real_replace = os.replace

            def record_fsync(descriptor: int) -> None:
                """Record whether the synchronized descriptor is a file or directory."""

                events.append("dir" if stat.S_ISDIR(os.fstat(descriptor).st_mode) else "file")

            def record_replace(source, destination, **kwargs) -> None:
                """Record and perform the atomic replacement."""

                events.append("replace")
                real_replace(source, destination, **kwargs)

            with patch("agent_run.adapters.home.os.fsync", side_effect=record_fsync), patch(
                "agent_run.adapters.home.os.replace", side_effect=record_replace
            ):
                write_managed_file(Path(directory).resolve(), "answer.md", "done")
            self.assertEqual(events, ["file", "replace", "dir"])

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

    def test_failed_atomic_replace_preserves_existing_content(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()
            target = home / "settings/config.toml"
            write_managed_file(home, "settings/config.toml", "original")
            with patch("agent_run.adapters.home.os.replace", side_effect=OSError("failed")):
                with self.assertRaises(OSError):
                    write_managed_file(home, "settings/config.toml", "replacement")
            self.assertEqual(target.read_text(encoding="utf-8"), "original")
            self.assertEqual(list(target.parent.glob(".*.tmp")), [])

    def test_parent_swap_cannot_redirect_managed_replace(self) -> None:
        """Keep publication on its retained parent descriptor during a path swap."""

        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            home = Path(directory).resolve()
            parent = home / "settings"
            parent.mkdir()
            retained = home / "retained-settings"
            real_replace = os.replace

            def swap_then_replace(source, destination, **kwargs) -> None:
                """Swap the lexical parent immediately before descriptor-relative replace."""

                parent.rename(retained)
                parent.symlink_to(outside, target_is_directory=True)
                real_replace(source, destination, **kwargs)

            with patch(
                "agent_run.adapters.home.os.replace", side_effect=swap_then_replace
            ):
                write_managed_file(home, "settings/config.toml", "retained")
            self.assertEqual(
                (retained / "config.toml").read_text(encoding="utf-8"), "retained"
            )
            self.assertFalse((Path(outside) / "config.toml").exists())

    def test_failed_file_sync_publishes_nothing_and_cleans_its_temp(self) -> None:
        """Leave no target or owned temporary when payload synchronization fails."""

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()

            def fail_file_sync(descriptor: int) -> None:
                """Fail only synchronization of the temporary regular file."""

                if stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise OSError("file sync failed")

            with patch("agent_run.adapters.home.os.fsync", side_effect=fail_file_sync):
                with self.assertRaisesRegex(OSError, "file sync failed"):
                    write_managed_file(home, "answer.md", "replacement")
            self.assertFalse((home / "answer.md").exists())
            self.assertEqual(list(home.glob(".agent-run-tmp-*.tmp")), [])

    def test_unsupported_directory_sync_does_not_reject_publication(self) -> None:
        """Accept a filesystem that explicitly reports directory fsync unsupported."""

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory).resolve()

            def reject_directory_sync(descriptor: int) -> None:
                """Report EINVAL only for directory descriptors."""

                if stat.S_ISDIR(os.fstat(descriptor).st_mode):
                    raise OSError(errno.EINVAL, "directory fsync unsupported")

            with patch("agent_run.adapters.home.os.fsync", side_effect=reject_directory_sync):
                write_managed_file(home, "answer.md", "replacement")
            self.assertEqual((home / "answer.md").read_text(encoding="utf-8"), "replacement")

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
