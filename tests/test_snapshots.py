"""Contract checks for immutable managed-tree snapshots and recovery state."""

from __future__ import annotations

import json
import subprocess
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from types import MappingProxyType
from unittest.mock import patch

from agent_run.adapters.snapshots import (
    CONFIG_SNAPSHOT_FILENAME,
    RUNTIME_SNAPSHOT_INDEX,
    SNAPSHOT_MANIFEST,
    build_config_snapshot,
    finalize_runtime_snapshots,
    inspect_config_snapshot,
    inspect_managed_snapshot,
    inspect_runtime_snapshots,
    runtime_snapshot_index_sha256,
    snapshot_managed_tree,
    snapshot_selected_assets,
)
from agent_run.config import EnvironmentConfig, RuntimeConfig
from agent_run.errors import ValidationError
from agent_run.profiles import AgentProfile


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
        (self.source / "scripts" / "run.sh").chmod(0o755)
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

    def test_selected_assets_preserve_layout_without_copying_other_files(self) -> None:
        """Copy only explicitly named plugin files and selected directory contents."""

        (self.source / "unselected.txt").write_text("do not copy", encoding="utf-8")
        snapshot_selected_assets(
            self.home,
            "declared-plugins/demo",
            self.source,
            ("SKILL.md", "scripts"),
        )
        copied = self.home / "declared-plugins/demo"
        self.assertTrue((copied / "SKILL.md").is_file())
        self.assertTrue((copied / "scripts/run.sh").is_file())
        self.assertFalse((copied / "unselected.txt").exists())
        script = copied / "scripts/run.sh"
        self.assertEqual(script.stat().st_mode & 0o777, 0o700)
        self.assertEqual(subprocess.run((str(script),), check=False).returncode, 0)

    def test_interrupted_metadata_and_recovery_states_never_verify(self) -> None:
        """Keep missing metadata, owned temps, orphans, and missing references distinct."""

        from agent_run.adapters import snapshot_tree

        real_write = snapshot_tree.write_managed_file

        def fail_manifest(home, relative, content, **kwargs):
            """Fail only the metadata publication after copying content."""

            if Path(relative).name == SNAPSHOT_MANIFEST:
                raise OSError("metadata failed")
            return real_write(home, relative, content, **kwargs)

        with patch(
            "agent_run.adapters.snapshot_tree.write_managed_file",
            side_effect=fail_manifest,
        ):
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

    def test_config_snapshot_changes_with_content_without_storing_secret_values(self) -> None:
        """Bind runtime/profile content while retaining only hashes of configured values."""

        secret = "credential-like-value"
        config = RuntimeConfig(
            True,
            "agent_run.adapters.claude.adapter:ADAPTER",
            Path("/bin/echo"),
            self.home,
            ("sonnet",),
            environment=EnvironmentConfig(
                variables=MappingProxyType({"TOKEN_LIKE": secret})
            ),
        )
        profile = AgentProfile("review", "Review exactly.", False, (self.root,), False)
        index_sha256 = finalize_runtime_snapshots(self.home, "files-1")
        first = build_config_snapshot(
            runtime="claude",
            adapter_api_version=1,
            schema_version=1,
            materialize_revision="files-1",
            snapshot_index_sha256=index_sha256,
            config=config,
            profile=profile,
        )
        same = build_config_snapshot(
            runtime="claude",
            adapter_api_version=1,
            schema_version=1,
            materialize_revision="files-1",
            snapshot_index_sha256=index_sha256,
            config=config,
            profile=profile,
        )
        changed = build_config_snapshot(
            runtime="claude",
            adapter_api_version=1,
            schema_version=1,
            materialize_revision="files-1",
            snapshot_index_sha256=index_sha256,
            config=config,
            profile=AgentProfile("review", "Changed body.", False, (self.root,), False),
        )
        declared_assets = build_config_snapshot(
            runtime="claude",
            adapter_api_version=1,
            schema_version=1,
            materialize_revision="files-1",
            snapshot_index_sha256=index_sha256,
            config=replace(
                config,
                plugin_snapshot_assets=MappingProxyType(
                    {"compressor": ("hooks/hooks.json",)}
                ),
            ),
            profile=profile,
        )
        observed_version = build_config_snapshot(
            runtime="claude",
            adapter_api_version=1,
            schema_version=1,
            materialize_revision="files-1",
            snapshot_index_sha256=index_sha256,
            config=config,
            profile=profile,
            runtime_version="2.1.0 (Claude Code)",
        )
        self.assertEqual(first, same)
        self.assertIsNone(first.runtime_version)
        self.assertEqual(observed_version.runtime_version, "2.1.0 (Claude Code)")
        self.assertNotEqual(first.sha256, changed.sha256)
        self.assertNotEqual(first.sha256, declared_assets.sha256)
        self.assertNotEqual(first.sha256, observed_version.sha256)
        self.assertNotIn(secret.encode(), first.document)
        runtime_document = json.loads(first.document)["runtime_config"]
        self.assertEqual(runtime_document["models"], ["sonnet"])
        self.assertIn("TOKEN_LIKE", runtime_document["environment"]["variable_sha256"])
        candidate = self.root / "candidate"
        candidate.mkdir()
        (candidate / CONFIG_SNAPSHOT_FILENAME).write_bytes(observed_version.document)
        self.assertEqual(
            inspect_config_snapshot(candidate, observed_version.sha256),
            observed_version,
        )
        (candidate / CONFIG_SNAPSHOT_FILENAME).write_bytes(
            observed_version.document + b" "
        )
        with self.assertRaisesRegex(ValidationError, "hash"):
            inspect_config_snapshot(candidate, observed_version.sha256)

    def test_runtime_index_detects_an_entire_missing_snapshot_root(self) -> None:
        """Keep expected roots discoverable after their whole directory disappears."""

        snapshot_managed_tree(self.home, "skills/demo", self.source)
        settings = self.home / "settings.json"
        settings.write_text("{}", encoding="utf-8")
        index_sha256 = finalize_runtime_snapshots(
            self.home, "files-1", ("settings.json",)
        )
        self.assertTrue(
            inspect_runtime_snapshots(
                self.home, "files-1", expected_sha256=index_sha256
            ).verified
        )
        settings.write_text('{"changed":true}', encoding="utf-8")
        changed = inspect_runtime_snapshots(
            self.home, "files-1", expected_sha256=index_sha256
        )
        self.assertIn("settings.json", changed.mismatched)
        self.assertIn("settings.json", changed.hash_mismatches)
        settings.write_text("{}", encoding="utf-8")
        settings.unlink()
        settings.symlink_to(self.source / "SKILL.md")
        wrong_type = inspect_runtime_snapshots(
            self.home, "files-1", expected_sha256=index_sha256
        )
        self.assertIn("settings.json", wrong_type.type_mismatches)
        settings.unlink()
        settings.write_text("{}", encoding="utf-8")
        (self.home / "skills/demo").rename(self.home / "skills/demo.gone")
        inspection = inspect_runtime_snapshots(
            self.home, "files-1", expected_sha256=index_sha256
        )
        self.assertFalse(inspection.verified)
        self.assertIn("skills/demo/.agent-run-snapshot.json", inspection.missing)
        (self.home / RUNTIME_SNAPSHOT_INDEX).write_text(
            '{"files":[],"materialize_revision":"files-1",'
            '"roots":[],"snapshot_index_version":1}\n',
            encoding="utf-8",
        )
        with self.assertRaisesRegex(ValidationError, "malformed"):
            runtime_snapshot_index_sha256(self.home, "files-1")

    def test_runtime_index_binds_each_root_manifest_revision(self) -> None:
        """Reject an internally consistent tree replaced after index finalization."""

        snapshot_managed_tree(self.home, "skills/demo", self.source)
        index_sha256 = finalize_runtime_snapshots(self.home, "files-1")
        finalized = (self.home / RUNTIME_SNAPSHOT_INDEX).read_bytes()
        (self.source / "SKILL.md").write_text("replacement", encoding="utf-8")
        snapshot_managed_tree(self.home, "skills/demo", self.source)
        (self.home / RUNTIME_SNAPSHOT_INDEX).write_bytes(finalized)
        inspection = inspect_runtime_snapshots(
            self.home, "files-1", expected_sha256=index_sha256
        )
        self.assertFalse(inspection.verified)
        self.assertIn(
            "skills/demo/.agent-run-snapshot.json", inspection.hash_mismatches
        )

    def test_runtime_hash_reader_rejects_incomplete_index_shape(self) -> None:
        """Require canonical complete producer evidence before returning its hash."""

        self.home.mkdir()
        (self.home / RUNTIME_SNAPSHOT_INDEX).write_text(
            '{"materialize_revision":"files-1"}\n', encoding="utf-8"
        )
        with self.assertRaisesRegex(ValidationError, "malformed"):
            runtime_snapshot_index_sha256(self.home, "files-1")


if __name__ == "__main__":
    unittest.main()
