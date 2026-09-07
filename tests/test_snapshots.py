"""Contract checks for immutable managed-tree snapshots and recovery state."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from agent_run.adapters.snapshots import (
    SNAPSHOT_MANIFEST,
    inspect_managed_snapshot,
    snapshot_managed_tree,
)
from agent_run.errors import ValidationError


class ManagedSnapshotTests(unittest.TestCase):
    """Exercise deterministic publication, source safety, and recovery labels."""

    def setUp(self) -> None:
        """Create isolated source and generated-home directories."""

        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.source = self.root / "source"
        (self.source / "scripts").mkdir(parents=True)
        (self.source / "empty").mkdir()
        (self.source / "SKILL.md").write_text("first", encoding="utf-8")
        (self.source / "scripts" / "run.sh").write_bytes(b"#!/bin/sh\n")
        self.home = self.root / "home"

    def test_full_tree_is_copied_and_content_changes_revision(self) -> None:
        """Copy every file and empty directory with deterministic content evidence."""

        first = snapshot_managed_tree(self.home, "skills/demo", self.source)
        self.assertTrue(inspect_managed_snapshot(self.home, "skills/demo").verified)
        self.assertTrue((self.home / "skills/demo/empty").is_dir())
        self.assertEqual((self.home / "skills/demo/scripts/run.sh").read_bytes(), b"#!/bin/sh\n")
        (self.source / "scripts" / "run.sh").write_bytes(b"#!/bin/sh\necho changed\n")
        second = snapshot_managed_tree(self.home, "skills/demo", self.source)
        self.assertNotEqual(first.sha256, second.sha256)
        self.assertEqual(
            (self.home / "skills/demo/scripts/run.sh").read_bytes(),
            b"#!/bin/sh\necho changed\n",
        )

    def test_sources_reject_symlinks_and_special_files(self) -> None:
        """Never follow a skill source link or accept a non-regular entry."""

        (self.source / "linked").symlink_to(self.source / "SKILL.md")
        with self.assertRaisesRegex(ValidationError, "regular"):
            snapshot_managed_tree(self.home, "skills/demo", self.source)

    def test_interrupted_metadata_and_recovery_states_never_verify(self) -> None:
        """Keep missing metadata, owned temps, orphans, and missing references distinct."""

        from agent_run.adapters import snapshots

        real_write = snapshots.write_managed_file

        def fail_manifest(home, relative, content):
            """Fail only the metadata publication after copying content."""

            if Path(relative).name == SNAPSHOT_MANIFEST:
                raise OSError("metadata failed")
            return real_write(home, relative, content)

        with patch("agent_run.adapters.snapshots.write_managed_file", side_effect=fail_manifest):
            with self.assertRaisesRegex(OSError, "metadata failed"):
                snapshot_managed_tree(self.home, "skills/demo", self.source)
        incomplete = inspect_managed_snapshot(self.home, "skills/demo")
        self.assertFalse(incomplete.verified)
        self.assertIn("SKILL.md", incomplete.orphans)

        clean_home = self.root / "clean-home"
        snapshot_managed_tree(clean_home, "skills/demo", self.source)
        destination = clean_home / "skills/demo"
        (destination / ".agent-run-tmp-answer.dead.tmp").write_bytes(b"partial")
        (destination / "orphan.txt").write_bytes(b"orphan")
        (destination / "SKILL.md").unlink()
        recovery = inspect_managed_snapshot(clean_home, "skills/demo")
        self.assertFalse(recovery.verified)
        self.assertIn(".agent-run-tmp-answer.dead.tmp", recovery.owned_temps)
        self.assertIn("orphan.txt", recovery.orphans)
        self.assertIn("SKILL.md", recovery.referenced_missing)
        self.assertTrue((destination / "orphan.txt").exists())


if __name__ == "__main__":
    unittest.main()
