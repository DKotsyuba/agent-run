"""Regression coverage for bounded capacity collection outcomes."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any, cast
from unittest import mock

from agent_run.capacity import collect, sources
from agent_run.capacity.collect import (
    STATUS_FAILED,
    STATUS_NO_DATA,
    STATUS_UNSUPPORTED,
    _collect_runtime,
)
from agent_run.capacity.sources import HOME_MISSING, PROBE_FAILED
from agent_run.capacity.topology import CapacityCollectionSlice, CapacityTopology
from agent_run.config import CapacityConfig
from agent_run.state.store import StateStore
from agent_run.adapters.codex import rate_limits

from test_capacity_collect import _runtime_config


import io
import json
from dataclasses import replace
from types import SimpleNamespace

from agent_run import cli
from agent_run.adapters.base import LimitSample


class CapacityOutcomeRegressionTests(unittest.TestCase):
    """Exercise isolation, classification, and persistence outcome contracts."""

    def test_invalid_slice_does_not_abort_later_valid_slice(self) -> None:
        """Continue after an invalid collection item and count committed samples."""
        sample = mock.Mock()
        sample.lane = "requests"
        first = CapacityCollectionSlice("codex", "first", (sample,), CapacityTopology((), ()), 1, 2)
        second = CapacityCollectionSlice("codex", "second", (sample,), CapacityTopology((), ()), 1, 2)
        collected = sources.CodexAppserverCollection(
            cast(tuple[CapacityCollectionSlice, ...], (first, object(), second))
        )
        with tempfile.TemporaryDirectory() as temporary:
            store = StateStore.initialize(Path(temporary) / "state.db")
            try:
                with mock.patch.object(collect.sources, "collect_codex_appserver", return_value=collected), \
                    mock.patch.object(collect, "persist_slice") as persist:
                    result = _collect_runtime(
                        store, "codex", _runtime_config(limits_source="codex_appserver"),
                        CapacityConfig(), cast(Any, lambda *_: None), 1, None,
                    )
                self.assertEqual(result.status, "partial")
                self.assertEqual(result.sample_count, 2)
                self.assertEqual(persist.call_count, 2)
            finally:
                store.close()

    def test_missing_home_and_missing_binary_are_distinct(self) -> None:
        """Classify a missing executable as probe failure when the home exists."""
        with tempfile.TemporaryDirectory() as temporary:
            existing = Path(temporary)
            runtime = _runtime_config(limits_source="codex_appserver", home=existing)
            with mock.patch.object(
                rate_limits, "read_rate_limits", side_effect=FileNotFoundError("secret-binary")
            ):
                result = sources.collect_codex_appserver("codex", runtime, 1)
            self.assertEqual(result.issues, (PROBE_FAILED,))

            runtime = _runtime_config(
                limits_source="codex_appserver", home=existing / "absent-home"
            )
            with mock.patch.object(
                rate_limits, "read_rate_limits", side_effect=FileNotFoundError("secret-home")
            ):
                result = sources.collect_codex_appserver("codex", runtime, 1)
            self.assertEqual(result.issues, (HOME_MISSING,))

    def test_failed_empty_and_unsupported_outcomes_are_reported(self) -> None:
        """Preserve failed, no-data, and unsupported statuses with zero samples."""
        with tempfile.TemporaryDirectory() as temporary:
            store = StateStore.initialize(Path(temporary) / "state.db")
            try:
                configs = {
                    "failed": _runtime_config(),
                    "empty": _runtime_config(),
                    "unsupported": _runtime_config(),
                }
                config_by_name = configs

                def collect_samples(name, *_args):
                    """Return the fixture outcome associated with one runtime name."""
                    if name == "failed":
                        raise RuntimeError("provider-secret")
                    return () if name == "empty" else None

                with mock.patch.object(collect.sources, "collect_samples", side_effect=collect_samples):
                    results = [
                        _collect_runtime(
                            store, name, config_by_name[name], CapacityConfig(),
                            cast(Any, lambda *_: None), 1, None,
                        )
                        for name in configs
                    ]
                self.assertEqual([result.status for result in results], [
                    STATUS_FAILED, STATUS_NO_DATA, STATUS_UNSUPPORTED
                ])
                self.assertEqual([result.sample_count for result in results], [0, 0, 0])
                self.assertEqual(results[0].error, "RuntimeError")
            finally:
                store.close()

    def test_capacity_collect_cli_preserves_status_counts_and_degraded_exit(self) -> None:
        """Execute the CLI table and retain each report status and count in JSON."""
        cases = (
            (STATUS_FAILED, 0, 2),
            ("partial", 2, 2),
            ("no_data", 0, 2),
            ("collected", 3, 0),
            ("unsupported", 0, 0),
        )
        with tempfile.TemporaryDirectory() as temporary:
            for status, count, expected_code in cases:
                with self.subTest(status=status):
                    report = collect.CollectionReport(
                        1,
                        1,
                        (collect.CollectResult("codex", status, count),),
                    )
                    stdout = io.StringIO()
                    with self.assertLogs("agent_run.cli", level="INFO") as logs:
                        code = cli.main(
                            ["--home", temporary, "capacity", "collect", "--once"],
                            service=SimpleNamespace(capacity_collect=lambda: report),
                            stdout=stdout,
                            stderr=io.StringIO(),
                        )
                    payload = json.loads(stdout.getvalue())
                    self.assertEqual(code, expected_code)
                    self.assertEqual(payload["results"][0]["status"], status)
                    self.assertEqual(payload["results"][0]["sample_count"], count)
                    rendered = "\n".join(logs.output)
                    if expected_code:
                        self.assertIn("outcome=degraded", rendered)
                        self.assertNotIn("RuntimeError", rendered)
                        self.assertNotIn("provider-secret", rendered)

    def test_middle_persistence_failure_keeps_neighboring_scopes_durable(self) -> None:
        """Persist healthy scopes despite a middle write failure without leaking its body."""
        def sample(percent: float) -> LimitSample:
            """Build one valid Codex app-server sample for an isolated scope."""
            return LimitSample(
                "requests", "5h", percent, None, None, "codex_appserver"
            )

        with tempfile.TemporaryDirectory() as temporary:
            store = StateStore.initialize(Path(temporary) / "state.db")
            try:
                runtime = _runtime_config(limits_source="codex_appserver")
                first = replace(collect._slice_from_samples("codex", runtime, (sample(10),), 1), scope_id="first")
                middle = replace(collect._slice_from_samples("codex", runtime, (sample(20),), 1), scope_id="middle")
                last = replace(collect._slice_from_samples("codex", runtime, (sample(30),), 1), scope_id="last")
                collect.persist_slice(store, middle)
                collected = sources.CodexAppserverCollection((first, middle, last))
                real_persist = collect.persist_slice

                def persist(store_arg, slice_):
                    """Raise only for the new middle scope while delegating real writes."""
                    if slice_.scope_id == "middle":
                        raise RuntimeError("provider-secret")
                    return real_persist(store_arg, slice_)

                with mock.patch.object(collect.sources, "collect_codex_appserver", return_value=collected), \
                    mock.patch.object(collect, "persist_slice", side_effect=persist), \
                    self.assertLogs("agent_run.capacity", level="WARNING") as logs:
                    result = _collect_runtime(
                        store, "codex", runtime, CapacityConfig(),
                        cast(Any, lambda *_: None), 1, None,
                    )
                rows = store.connection.execute(
                    "SELECT remaining_percent FROM capacity_samples ORDER BY remaining_percent"
                ).fetchall()
                self.assertEqual(result.status, "partial")
                self.assertEqual(result.sample_count, 2)
                self.assertEqual([row["remaining_percent"] for row in rows], [10.0, 20.0, 30.0])
                rendered = "\n".join(logs.output)
                self.assertIn("persist_failed", rendered)
                self.assertNotIn("provider-secret", rendered)
            finally:
                store.close()

    def test_collect_once_reports_all_failed_codex_and_commits_later_runtime(self) -> None:
        """Keep an all-failed Codex result degraded while a later runtime commits."""
        def collected(name, runtime, *_args):
            """Return a failed Codex batch or one healthy batch for the round."""
            if name == "codex":
                return sources.CodexAppserverCollection((), (HOME_MISSING,))
            slice_ = collect._slice_from_samples(
                name, runtime, (LimitSample("requests", "5h", 50, None, None, "codex_appserver"),), 1
            )
            return sources.CodexAppserverCollection((slice_,))

        with tempfile.TemporaryDirectory() as temporary:
            store = StateStore.initialize(Path(temporary) / "state.db")
            try:
                runtime = _runtime_config(limits_source="codex_appserver")
                config = cast(Any, SimpleNamespace(
                    runtimes={"codex": runtime, "healthy": runtime}, capacity=CapacityConfig()
                ))
                with mock.patch.object(collect.sources, "collect_codex_appserver", side_effect=collected):
                    report = collect.collect_once(store, config, at=1)
                self.assertFalse(report.ok)
                self.assertEqual([(item.runtime, item.status, item.sample_count) for item in report.results], [
                    ("codex", STATUS_FAILED, 0), ("healthy", "collected", 1),
                ])
                self.assertEqual(
                    store.connection.execute("SELECT COUNT(*) FROM capacity_samples").fetchone()[0], 1
                )
            finally:
                store.close()

    def test_duplicate_backend_account_id_is_skipped_without_issue(self) -> None:
        """Treat two account labels for one backend identity as one clean scope."""
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary) / "codex"
            for account_home in (home, home.with_name("codex@one"), home.with_name("codex@two")):
                account_home.mkdir()
            runtime = _runtime_config(
                limits_source="codex_appserver", home=home, accounts=("one", "two")
            )
            sample = LimitSample("requests", "5h", 50, None, None, "codex_appserver")
            topology = collect._slice_from_samples("codex", runtime, (sample,), 1).topology
            with mock.patch.object(rate_limits, "read_rate_limits", return_value=object()), \
                mock.patch("agent_run.capacity.codex_appserver.normalize_rate_limits", return_value=((sample,), topology, "backend-id")):
                result = sources.collect_codex_appserver("codex", runtime, 1)
        self.assertEqual(len(result.slices), 1)
        self.assertEqual(result.issues, ())


if __name__ == "__main__":
    unittest.main()
