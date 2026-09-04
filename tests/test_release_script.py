"""Maintainer release safety tests with mocked external commands and temporary stores."""

from __future__ import annotations

import base64
import argparse
from contextlib import ExitStack
import hashlib
import importlib
from pathlib import Path
import plistlib
import runpy
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import release
import release_local as local


class PublicationTests(unittest.TestCase):
    """Check immutable remote publication and artifact evidence without network writes."""

    def test_missing_or_wrong_head_workflow_never_passes(self):
        """Empty/wrong-SHA run lists must wait rather than treating absence as success."""
        for runs in ([], [{"headSha": "other", "event": "push"}]):
            runner = Mock()
            runner.json.return_value = runs
            runner.pause.side_effect = release.ReleaseError("timeout")
            with self.assertRaisesRegex(release.ReleaseError, "timeout"):
                release.wait_workflow(runner, "ci.yml", "wanted", "push")
            runner.pause.assert_called_once()

    def test_cli_uses_shared_exception_identity_and_restores_every_job(self):
        """Real __main__ loading catches local failures and attempts all three restarts."""
        calls = []

        def arguments(_argv):
            """Configure the temporary runpy module at parsing, returning Namespace options."""
            script = sys.modules["release"]
            vars(script)["publish"] = Mock(return_value="head")
            vars(script)["verify_assets"] = Mock(return_value=Path("wheel.whl"))
            deployment = importlib.import_module("release_local")
            self.assertIs(deployment.ReleaseError, script.ReleaseError)

            def deploy(runner, *_args):
                """Inject canonical runner failures and invoke real recovery of string jobs."""
                runner.run = Mock(side_effect=script.ReleaseError("bootstrap failed"))
                try:
                    with patch.object(deployment, "loaded", return_value=False):
                        deployment.restart(runner, {"api": "api.plist", "capacity": "capacity.plist", "delivery": "delivery.plist"})
                finally:
                    calls.extend(runner.run.call_args_list)

            vars(deployment)["deploy"] = deploy
            return argparse.Namespace(version="1.2.3", publish_only=False, home=Path("/unused"),
                                      python="python3.11", launchd_prefix="com.test", timeout=10, poll=1)

        with patch.dict(sys.modules), patch.object(argparse.ArgumentParser, "parse_args", side_effect=arguments):
            sys.modules.pop("release_local", None)
            with self.assertRaises(SystemExit) as exit_status:
                runpy.run_path(str(release.ROOT / "scripts/release.py"), run_name="__main__")
            self.assertEqual(exit_status.exception.code, 1)
        self.assertEqual(len(calls), 3)

    def test_failed_and_empty_job_workflows_fail_closed(self):
        """Failed run conclusions and missing job evidence cannot pass a CI gate."""
        runner = Mock()
        runner.json.return_value = [{"headSha": "wanted", "event": "push", "status": "completed", "conclusion": "failure"}]
        with self.assertRaisesRegex(release.ReleaseError, "failure"):
            release.wait_workflow(runner, "ci.yml", "wanted", "push")
        runner.json.side_effect = [[{"databaseId": 1, "headSha": "wanted", "event": "push", "status": "completed", "conclusion": "success"}],
                                   {"headSha": "wanted", "jobs": []}]
        runner.pause.side_effect = release.ReleaseError("missing jobs")
        with self.assertRaisesRegex(release.ReleaseError, "missing jobs"):
            release.wait_workflow(runner, "ci.yml", "wanted", "push")

    def test_required_pending_and_missing_checks_wait(self):
        """GitHub pending exit 8 and an empty required-check set are not success."""
        for output in ("[]", '[{"bucket":"pending"}]'):
            runner = Mock()
            runner.json.return_value = {"headRefOid": "head"}
            runner.run.return_value = subprocess.CompletedProcess([], 8, output, "")
            runner.pause.side_effect = release.ReleaseError("waited")
            with self.assertRaisesRegex(release.ReleaseError, "waited"):
                release.wait_required(runner, 3, "head")

    def test_required_failure_and_head_change(self):
        """Required failures and head drift fail before merge can be requested."""
        runner = Mock()
        runner.json.return_value = {"headRefOid": "changed"}
        with self.assertRaisesRegex(release.ReleaseError, "head changed"):
            release.wait_required(runner, 3, "head")
        runner.json.return_value = {"headRefOid": "head"}
        runner.run.return_value = subprocess.CompletedProcess([], 1, '[{"bucket":"fail"}]', "")
        with self.assertRaisesRegex(release.ReleaseError, "failed"):
            release.wait_required(runner, 3, "head")

    def test_tag_must_be_annotated_and_match_package(self):
        """An existing lightweight or version-mismatched tag is never accepted."""
        runner = Mock()
        with patch.object(release, "optional_api", return_value={"object": {"type": "commit"}}):
            with self.assertRaisesRegex(release.ReleaseError, "annotated"):
                release.tag_commit(runner, "v1.2.3")
        with patch.object(release, "optional_api", return_value={"object": {"type": "tag", "sha": "tag"}}):
            runner.json.side_effect = [{"object": {"type": "commit", "sha": "head"}},
                {"content": base64.b64encode(b'[project]\nversion="1.2.4"').decode()}]
            with self.assertRaisesRegex(release.ReleaseError, "does not match"):
                release.tag_commit(runner, "v1.2.3")

    def test_public_resume_does_not_inspect_dirty_checkout(self):
        """Existing publication proceeds directly to artifacts without source mutation."""
        runner = Mock()
        with patch.object(release, "optional_api", return_value={"draft": False, "tag_name": "v1.2.3"}), \
             patch.object(release, "tag_commit", return_value="head"), patch.object(release, "prepare_pr") as prepare:
            self.assertEqual(release.publish(runner, "1.2.3"), "head")
            prepare.assert_not_called()
            runner.run.assert_not_called()

    def test_dirty_new_source_refused_before_fetch(self):
        """New releases fail before git writes when source changes are uncommitted."""
        runner = Mock()
        runner.run.return_value.stdout = " M source.py\n"
        with self.assertRaisesRegex(release.ReleaseError, "clean committed"):
            release.prepare_pr(runner, "1.2.3")
        self.assertEqual(runner.run.call_count, 1)

    def test_new_publication_gates_exact_heads_before_annotated_tag(self):
        """PR/main CI gates precede immutable tagging and the existing release workflow."""
        runner = Mock()
        runner.json.return_value = {"state": "MERGED", "mergeCommit": {"oid": "merge"}}
        runner.run.return_value = subprocess.CompletedProcess([], 1, "", "")
        with patch.object(release, "optional_api", side_effect=[None, {"draft": False}]), \
             patch.object(release, "tag_commit", side_effect=[None, None, "merge"]), \
             patch.object(release, "prepare_pr", return_value={"number": 9, "state": "OPEN", "headRefOid": "prhead"}), \
             patch.object(release, "wait_workflow") as workflow, patch.object(release, "wait_required") as required:
            self.assertEqual(release.publish(runner, "1.2.3"), "merge")
        self.assertEqual([call.args[1:] for call in workflow.call_args_list],
                         [("ci.yml", "prhead", "pull_request"), ("ci.yml", "merge", "push"), ("release.yml", "merge", "push")])
        required.assert_called_once_with(runner, 9, "prhead")
        runner.run.assert_any_call("git", "tag", "-a", "v1.2.3", "merge", "-m", "agent-run 1.2.3")
        runner.run.assert_any_call("gh", "pr", "merge", "9", "--repo", release.REPOSITORY,
                                  "--squash", "--match-head-commit", "prhead")

    def test_assets_require_hash_and_signed_commit_workflow_subject(self):
        """Hash corruption and signed identity drift are rejected despite verifier exit 0."""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            names = ["agent_run-1.2.3-py3-none-any.whl", "agent_run-1.2.3.tar.gz"]
            digest = hashlib.sha256(b"artifact").hexdigest()
            for name in names:
                (root / name).write_bytes(b"artifact")
            (root / "SHA256SUMS").write_text("".join(f"{digest}  {name}\n" for name in names))
            evidence = [{"verificationResult": {"signature": {"certificate": {
                "sourceRepositoryDigest": "head", "sourceRepositoryRef": "refs/tags/v1.2.3",
                "buildConfigURI": f"https://github.com/{release.REPOSITORY}/.github/workflows/release.yml@refs/tags/v1.2.3"}},
                "statement": {"subject": [{"name": name, "digest": {"sha256": digest}} for name in names]}}}]
            runner = Mock()
            runner.json.return_value = evidence
            self.assertEqual(release.verify_assets(runner, "1.2.3", "head", root), root / names[0])
            with self.assertRaisesRegex(release.ReleaseError, "Provenance"):
                release.verify_assets(runner, "1.2.3", "wrong", root)
            (root / names[0]).write_bytes(b"corrupted")
            with self.assertRaisesRegex(release.ReleaseError, "Checksum"):
                release.verify_assets(runner, "1.2.3", "head", root)


class LocalTests(unittest.TestCase):
    """Exercise deploy ordering and crash recovery against temporary SQLite state."""

    def setUp(self):
        """Create a disposable sealed layout, active-state tables, and launchd fixtures."""
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.user = Path(self.temporary.name).resolve()
        self.home = self.user / ".agent-run"
        self.old = self.home / "standalone/releases/old"
        self.new = self.home / "standalone/releases/new"
        self.old.mkdir(parents=True)
        self.current = self.home / "standalone/current"
        self.current.symlink_to(self.old)
        self.home.joinpath("config.toml").write_text("schema_version=1\n")
        with sqlite3.connect(self.home / "state.db") as connection:
            connection.executescript("CREATE TABLE agents(status TEXT); CREATE TABLE workflow_runs(status TEXT); PRAGMA user_version=10;")
        self.plists = self.user / "Library/LaunchAgents"
        self.plists.mkdir(parents=True)
        for suffix in ("api", "capacity", "delivery"):
            label = f"com.test.agent-run.{suffix}"
            command = [str(self.current / "venv/bin/agent-run")]
            if suffix != "capacity":
                command += ["--home", str(self.home)]
            command += [suffix, "serve" if suffix == "api" else "collect"]
            (self.plists / f"{label}.plist").write_bytes(plistlib.dumps({"Label": label, "ProgramArguments": command}))
        self.runner = Mock(spec=release.Runner)
        self.runner.poll = 0.01
        self.runner.run.return_value = subprocess.CompletedProcess([], 0, "", "")

    def patches(self) -> ExitStack:
        """Return entered patch stack replacing external effects, retaining real SQLite/backups."""
        stack = ExitStack()
        stack.enter_context(patch.object(local.sys, "platform", "darwin"))
        stack.enter_context(patch.object(local.Path, "home", return_value=self.user))
        stack.enter_context(patch.object(local, "verify_complete"))
        stack.enter_context(patch.object(local, "identity", side_effect=lambda r, p: ("1.2.3", 11) if p == self.new else ("1.2.2", 10)))
        stack.enter_context(patch.object(local, "prepare", return_value=11))
        stack.enter_context(patch.object(local, "loaded", return_value=True))
        stack.enter_context(patch.object(local, "validate_live"))
        return stack

    def deploy(self):
        """Call local deployment with fixture home and a non-installed dummy wheel path."""
        local.deploy(self.runner, self.home, self.user / "wheel.whl", "1.2.3", "new", "python3.11", "com.test.agent-run")

    def migrate(self, runner, target, home):
        """Fake installed migration, asserting backup-before-migration and all shutdowns."""
        backups = list((home / "standalone/backups").glob("*/state.db"))
        self.assertEqual(len(backups), 1)
        with sqlite3.connect(backups[0]) as connection:
            self.assertEqual(connection.execute("PRAGMA user_version").fetchone()[0], 10)
        self.assertEqual(len([c for c in self.runner.run.call_args_list if c.args[:2] == ("launchctl", "bootout")]), 3)
        with sqlite3.connect(home / "state.db") as connection:
            connection.execute("PRAGMA user_version=11")

    def test_backup_before_migration_default_home_plist_and_idempotence(self):
        """Default-home capacity plist works; rerun validates without a second migration."""
        with self.patches(), patch.object(local, "migrate", side_effect=self.migrate) as migrate, patch.object(local, "restart") as restart:
            self.deploy()
            self.assertEqual(self.current.resolve(), self.new)
            self.assertEqual(local.schema_version(self.home), 11)
            self.deploy()
            self.assertEqual(migrate.call_count, 1)
            restart.assert_called_once()
            self.assertFalse((self.home / "standalone/deploy.json").exists())

    def test_wrong_default_home_plist_fails_before_stopping(self):
        """AGENT_RUN_HOME pointing elsewhere cannot stop an unrelated periodic job."""
        path = self.plists / "com.test.agent-run.capacity.plist"
        contents = plistlib.loads(path.read_bytes())
        contents["EnvironmentVariables"] = {"AGENT_RUN_HOME": str(self.user / "other")}
        path.write_bytes(plistlib.dumps(contents))
        with self.patches(), self.assertRaisesRegex(release.ReleaseError, "another home"):
            self.deploy()
        self.runner.run.assert_not_called()

    def test_active_work_waits_and_shutdown_race_refuses_migration(self):
        """Count agents/workflows and catch work admitted just before the reservation."""
        with sqlite3.connect(self.home / "state.db") as connection:
            connection.executescript("INSERT INTO agents VALUES('running'); INSERT INTO workflow_runs VALUES('created');")
        with local.database(self.home) as connection:
            self.assertEqual(local.active(connection), 2)
        self.runner.pause.side_effect = release.ReleaseError("still active")
        with self.patches(), self.assertRaisesRegex(release.ReleaseError, "still active"):
            self.deploy()
        self.runner.run.assert_not_called()
        with self.patches(), patch.object(local, "active", side_effect=[0, 1]), patch.object(local, "restart") as restart, \
             patch.object(local, "migrate") as migrate, self.assertRaisesRegex(release.ReleaseError, "Work arrived"):
            self.deploy()
        migrate.assert_not_called()
        restart.assert_called_once()

    def test_failed_migration_restores_old_only_when_schema_is_unchanged(self):
        """A pre-migration exception restarts old compatible services and retains journal."""
        with self.patches(), patch.object(local, "migrate", side_effect=RuntimeError("migration failed")), \
             patch.object(local, "restart") as restart, self.assertRaisesRegex(release.ReleaseError, "compatible services restored"):
            self.deploy()
        self.assertEqual(self.current.resolve(), self.old)
        self.assertEqual(local.schema_version(self.home), 10)
        restart.assert_called_once()
        self.assertTrue((self.home / "standalone/deploy.json").exists())

    def test_post_shutdown_recheck_prevents_backup_and_migration(self):
        """Work admitted after releasing the shutdown reservation blocks migration."""
        with self.patches(), patch.object(local, "active", side_effect=[0, 0, 0, 1]), \
             patch.object(local, "migrate") as migrate, patch.object(local, "restart") as restart, \
             self.assertRaisesRegex(release.ReleaseError, "after service shutdown"):
            self.deploy()
        migrate.assert_not_called()
        restart.assert_called_once()
        self.assertFalse(list((self.home / "standalone/backups").glob("*/state.db")))

    def test_swap_failure_after_migration_recovers_new_binary(self):
        """A failed first swap never restores the schema-10 binary onto schema 11."""
        original_switch = local.switch
        calls = []

        def swap(current, target):
            """Fail first Path pointer swap, then perform the schema-safe recovery swap."""
            calls.append(target)
            if len(calls) == 1:
                raise OSError("swap failed")
            original_switch(current, target)

        with self.patches(), patch.object(local, "migrate", side_effect=self.migrate), \
             patch.object(local, "switch", side_effect=swap), patch.object(local, "restart") as restart, \
             self.assertRaisesRegex(release.ReleaseError, "compatible services restored"):
            self.deploy()
        self.assertEqual(self.current.resolve(), self.new)
        self.assertEqual(calls, [self.new, self.new])
        restart.assert_called_once()

    def test_restart_failure_retries_compatible_services(self):
        """A first restart failure triggers another restore attempt on the new schema."""
        with self.patches(), patch.object(local, "migrate", side_effect=self.migrate), \
             patch.object(local, "restart", side_effect=[release.ReleaseError("restart failed"), None]) as restart, \
             self.assertRaisesRegex(release.ReleaseError, "compatible services restored"):
            self.deploy()
        self.assertEqual(restart.call_count, 2)
        self.assertEqual(self.current.resolve(), self.new)

    def test_recovery_failure_preserves_new_pointer_and_reports_stopped_jobs(self):
        """Repeated service restart failure reports recoverable state without downgrading."""
        with self.patches(), patch.object(local, "migrate", side_effect=self.migrate), \
             patch.object(local, "restart", side_effect=release.ReleaseError("restart failed")), \
             self.assertRaisesRegex(release.ReleaseError, "Services may be stopped"):
            self.deploy()
        self.assertEqual(self.current.resolve(), self.new)
        self.assertEqual(local.schema_version(self.home), 11)
        self.assertTrue((self.home / "standalone/deploy.json").exists())
        self.assertEqual(len(list((self.home / "standalone/backups").glob("*/state.db"))), 1)

    def test_complete_and_manifest_gate(self):
        """No pointer switch accepts missing COMPLETE or a corrupted sealed file."""
        target = self.new
        (target / "venv/bin").mkdir(parents=True)
        for name in ("python", "agent-run"):
            (target / "venv/bin" / name).write_text("binary")
        digest = hashlib.sha256(b"binary").hexdigest()
        package = "venv/lib/python3.11/site-packages/agent_run/__init__.py"
        (target / package).parent.mkdir(parents=True)
        (target / package).write_text("binary")
        (target / "SHA256SUMS").write_text(f"{digest}  venv/bin/python\n{digest}  venv/bin/agent-run\n{digest}  {package}\n")
        with self.assertRaises(FileNotFoundError):
            local.switch(self.current, target)
        self.assertEqual(self.current.resolve(), self.old)
        (target / "COMPLETE").write_text("complete\n")
        local.verify_complete(target)
        (target / "venv/bin/agent-run").write_text("corrupt")
        with self.assertRaisesRegex(release.ReleaseError, "Corrupt"):
            local.switch(self.current, target)
        self.assertEqual(self.current.resolve(), self.old)

    def test_legacy_seal_normalizes_paths_and_allows_external_python_symlink(self):
        """Legacy ./regular-file seals remain reusable but normalized duplicates fail."""
        target = self.new
        (target / "venv/bin").mkdir(parents=True)
        (target / "venv/bin/python").symlink_to(sys.executable)
        (target / "venv/bin/agent-run").write_text("binary")
        package = "venv/lib/python3.11/site-packages/agent_run/__init__.py"
        (target / package).parent.mkdir(parents=True)
        (target / package).write_text("binary")
        digest = hashlib.sha256(b"binary").hexdigest()
        manifest = f"{digest}  ./venv/bin/agent-run\n{digest}  ./{package}\n"
        (target / "SHA256SUMS").write_text(manifest)
        (target / "COMPLETE").write_text("complete\n")
        local.verify_complete(target)
        (target / "SHA256SUMS").write_text(manifest + f"{digest}  venv/bin/agent-run\n")
        with self.assertRaisesRegex(release.ReleaseError, "duplicate"):
            local.verify_complete(target)

    def test_live_validation_waits_for_api_then_retries_transient_database_open(self):
        """API startup precedes metadata reads; a transient open failure cannot abort success."""
        events = []
        original_database = local.database

        def rpc(home, method):
            """Return fixture API results after one simulated unavailable socket."""
            events.append(method)
            if len(events) == 1:
                raise ConnectionRefusedError("starting")
            return {"ok": True} if method == "ping" else [{"name": "ping"}]

        def database(home):
            """Fail first fixture DB open during sidecar recreation, then use real SQLite."""
            events.append("database")
            if events.count("database") == 1:
                raise sqlite3.OperationalError("unable to open database file")
            return original_database(home)

        with patch.object(local, "rpc", side_effect=rpc), patch.object(local, "database", side_effect=database), \
             patch.object(local, "identity", return_value=("1.2.2", 10)):
            local.validate_live(self.runner, self.current, self.home, "1.2.2", 10)
        self.assertEqual(events, ["ping", "ping", "tools", "database", "database"])
        self.assertEqual(self.runner.pause.call_count, 2)
        self.runner.run.assert_called_once()

    def test_schema_probe_retries_locks_but_refuses_missing_corrupt_or_mismatched_db(self):
        """Only concrete transient SQLite errors retry; real state defects remain errors."""
        original_database = local.database
        with patch.object(local, "database", side_effect=[sqlite3.OperationalError("database is locked"), original_database(self.home)]):
            self.assertEqual(local.schema_version(self.home, self.runner, integrity=True), 10)
        self.runner.pause.assert_called_once_with("SQLite readiness")
        self.runner.pause.reset_mock()
        with patch.object(local, "database", side_effect=sqlite3.OperationalError("no such table: agents")), \
             self.assertRaises(sqlite3.OperationalError):
            local.schema_version(self.home, self.runner, integrity=True)
        self.runner.pause.assert_not_called()
        with self.assertRaisesRegex(release.ReleaseError, "missing"):
            local.schema_version(self.user / "absent", self.runner, integrity=True)
        with patch.object(local, "rpc", side_effect=[{"ok": True}, ["tools"]]), \
             patch.object(local, "identity", return_value=("1.2.3", 11)), \
             self.assertRaisesRegex(release.ReleaseError, "version/schema mismatch"):
            local.validate_live(self.runner, self.current, self.home, "1.2.3", 11)
        with patch.object(local, "database", side_effect=sqlite3.DatabaseError("database disk image is malformed")), \
             self.assertRaises(sqlite3.DatabaseError):
            local.schema_version(self.home, self.runner, integrity=True)

    def test_recovery_retries_database_open_and_never_selects_old_binary(self):
        """Transient reads during recovery wait for actual schema 11 before choosing target."""
        journal = {"previous": str(self.old), "target": str(self.new), "previous_schema": 10,
                   "target_schema": 11, "jobs": {"api": "api.plist"}}
        with sqlite3.connect(self.home / "state.db") as connection:
            connection.execute("PRAGMA user_version=11")
        original_database = local.database
        with patch.object(local, "database", side_effect=[sqlite3.OperationalError("unable to open database file"), original_database(self.home)]), \
             patch.object(local, "verify_complete"), patch.object(local, "migrate") as migrate, \
             patch.object(local, "restart") as restart:
            local.recover(self.runner, self.home, journal)
        self.assertEqual(self.current.resolve(), self.new)
        migrate.assert_not_called()
        restart.assert_called_once()

    def test_database_readiness_timeout_preserves_journal_without_guessing_schema(self):
        """Exhausted recovery waits never change a pointer or restart an unverified binary."""
        journal = {"previous": str(self.old), "target": str(self.new), "previous_schema": 10,
                   "target_schema": 11, "jobs": {"api": "api.plist"}}
        path = self.home / "standalone/deploy.json"
        local.save_journal(path, journal)
        self.runner.pause.side_effect = release.ReleaseError("deadline exceeded")
        with patch.object(local, "database", side_effect=sqlite3.OperationalError("unable to open database file")), \
             patch.object(local, "switch") as switch, patch.object(local, "restart") as restart, \
             self.assertRaisesRegex(release.ReleaseError, "deadline exceeded"):
            local.recover(self.runner, self.home, journal)
        switch.assert_not_called()
        restart.assert_not_called()
        self.assertTrue(path.exists())


if __name__ == "__main__":
    unittest.main()
