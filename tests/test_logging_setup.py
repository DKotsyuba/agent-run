from __future__ import annotations

import logging
import logging.handlers
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run import logging_setup
from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    LaunchPlan,
    ModelInfo,
    RuntimeHealth,
    RuntimeInfo,
)
from agent_run.config import Config, ProfilesConfig, RuntimeConfig
from agent_run.domain import StartRequest
from agent_run.service import AgentService
from agent_run.state.store import StateStore


class _FakeAdapter:
    """The minimal adapter surface :meth:`AgentService.start` drives end to end."""

    def __init__(self) -> None:
        self.capabilities = frozenset(Capability)

    def describe(self):
        return RuntimeInfo("fake", ADAPTER_API_VERSION, self.capabilities)

    def validate(self, config):
        pass

    def materialize(self, config, home, *, mcp_servers, skills_root):
        return "cfg-1"

    def models(self, config, home):
        return (ModelInfo("model", "fake model", ("high",)),)

    def probe(self, config, home):
        return RuntimeHealth(True, "1", True, None)

    def limits(self, config, home):
        return ()

    def prepare(self, request, profile, config, home, agent_dir, *, mcp_servers):
        return LaunchPlan(
            ("fake",), request.workdir, {}, request.task, agent_dir / "runtime.jsonl", {},
            agent_dir / "answer.md",
        )

    def launch(self, plan, sink):
        raise AssertionError("AgentService uses the injected launch seam")


_ADAPTER = _FakeAdapter()


class ConfigureLoggingTests(unittest.TestCase):
    """Unit coverage for :func:`agent_run.logging_setup.configure_logging`."""

    def setUp(self) -> None:
        logging_setup._reset_for_tests()

    def tearDown(self) -> None:
        logging_setup._reset_for_tests()

    def test_configure_logging_creates_the_log_directory_and_file(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            logging_setup.configure_logging(home, "mcp")
            self.assertTrue((home / "logs").is_dir())
            self.assertTrue((home / "logs" / "mcp.log").exists())

    def test_repeated_configure_is_idempotent_and_keeps_the_first_component(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            root = logging_setup.configure_logging(home, "cli")
            handlers_after_first = list(root.handlers)
            second_home = home / "other"
            root_again = logging_setup.configure_logging(second_home, "mcp")
            self.assertIs(root, root_again)
            self.assertEqual(list(root.handlers), handlers_after_first)
            self.assertFalse((second_home / "logs").exists())

    def test_env_level_is_honored(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {"AGENT_RUN_LOG_LEVEL": "WARNING"}):
                root = logging_setup.configure_logging(Path(directory), "cli")
            self.assertEqual(root.level, logging.WARNING)

    def test_default_level_is_debug_for_dense_logging(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            environ = dict(os.environ)
            environ.pop("AGENT_RUN_LOG_LEVEL", None)
            with patch.dict(os.environ, environ, clear=True):
                root = logging_setup.configure_logging(Path(directory), "cli")
            self.assertEqual(root.level, logging.DEBUG)

    def test_uncreatable_log_dir_falls_back_to_stderr_silently(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory) / "not-a-directory"
            home.write_text("x", encoding="utf-8")  # a file, so home/logs cannot be created
            root = logging_setup.configure_logging(home, "cli")
            self.assertEqual(len(root.handlers), 1)
            handler = root.handlers[0]
            self.assertIsInstance(handler, logging.StreamHandler)
            self.assertNotIsInstance(handler, logging.handlers.RotatingFileHandler)


class ServiceLoggingTests(unittest.TestCase):
    """Proves service.start emits a reconstructable lifecycle without leaking secrets."""

    def setUp(self) -> None:
        logging_setup._reset_for_tests()
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name).resolve()
        self.workdir = self.root / "work"
        self.runtime_home = self.root / "runtime"
        self.profiles = self.root / "profiles"
        for path in (self.workdir, self.runtime_home, self.profiles):
            path.mkdir()
        (self.profiles / "profile.md").write_text(
            "+++\nwrite = true\n+++\nDo the requested work.\n", encoding="utf-8"
        )
        self.config = Config(
            schema_version=1,
            profiles=ProfilesConfig(self.profiles),
            runtimes={
                "fake": RuntimeConfig(
                    True,
                    f"{__name__}:_ADAPTER",
                    Path("/bin/true"),
                    self.runtime_home,
                    ("model",),
                )
            },
        )
        self.store = StateStore.initialize(self.root / "state.db")
        self.service = AgentService(
            self.config,
            self.store,
            self.root,
            launch=lambda *args: None,
            now=lambda: 100.0,
        )

    def tearDown(self) -> None:
        self.service.close()
        self.temporary.cleanup()
        logging_setup._reset_for_tests()

    def request(self, *, request_id: str, task: str = "do work") -> StartRequest:
        """Build one valid fake-runtime request for logging assertions."""

        return StartRequest(
            "fake", "model", "profile", task, self.workdir, request_id=request_id
        )

    def test_service_start_logs_a_reconstructable_lifecycle(self) -> None:
        """Capture logs until the asynchronous start worker has fully returned."""

        finished = threading.Event()
        original = self.service._continue_start

        def tracked_start(*args, **kwargs):
            """Signal only after the real worker emitted its terminal lifecycle log."""

            try:
                return original(*args, **kwargs)
            finally:
                finished.set()

        with (
            self.assertLogs("agent_run.service", level="DEBUG") as captured,
            patch.object(self.service, "_continue_start", side_effect=tracked_start),
        ):
            self.service.start(self.request(request_id="log-test"))
            self.assertTrue(finished.wait(2), "start worker did not finish")
        joined = "\n".join(captured.output)
        self.assertIn("start runtime=fake model=model", joined)
        self.assertIn("gate=capabilities ok", joined)
        self.assertIn("materialized runtime=fake revision=cfg-1", joined)
        self.assertIn("created=True", joined)
        self.assertIn("done", joined)

    def test_service_start_never_logs_env_secret_values(self) -> None:
        secret = "sk-super-secret-token-value"
        with patch.dict(os.environ, {"FAKE_RUNTIME_TOKEN": secret}):
            with self.assertLogs("agent_run.service", level="DEBUG") as captured:
                self.service.start(self.request(request_id="secret-test"))
        joined = "\n".join(captured.output)
        self.assertNotIn(secret, joined)

    def test_service_start_never_logs_the_full_task_text(self) -> None:
        long_task = "do the work " * 40
        with self.assertLogs("agent_run.service", level="DEBUG") as captured:
            self.service.start(self.request(request_id="task-test", task=long_task))
        joined = "\n".join(captured.output)
        self.assertNotIn(long_task.strip(), joined)


if __name__ == "__main__":
    unittest.main()
