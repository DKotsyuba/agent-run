"""End-to-end CLI coverage for the read-only capacity-order command."""

import io
import json
import tempfile
import time
import unittest
from pathlib import Path

from agent_run.cli import main
from agent_run.config import Config, RuntimeConfig
from agent_run.service import AgentService
from agent_run.state import StateStore


def _runtime(name: str) -> RuntimeConfig:
    """Build one opaque enabled runtime for a capacity-only service test."""

    return RuntimeConfig(
        enabled=True,
        adapter="agent_run.adapters.codex.adapter:ADAPTER",
        binary=Path(f"/tmp/{name}"),
        home=Path(f"/tmp/{name}-home"),
        models=("model",),
    )


def _unused_launch(*args: object, **kwargs: object) -> None:
    """Fail if the read-only capacity command unexpectedly launches work."""

    del args, kwargs
    raise AssertionError("capacity order must not launch an agent")


def _append_scope(
    store: StateStore,
    runtime: str,
    *,
    remaining: float,
    observed_at: float,
    reset_at: float = 2_000.0,
    valid_until: float = 1_100.0,
) -> None:
    """Append one sample and refresh its explicit topology at ``observed_at``.

    ``reset_at`` and ``valid_until`` default to the fixed epochs the injected
    ``now`` fixtures use; tests that run the real clock pass fresh ones.
    """

    lane = f"lane-{runtime}"
    pool_id = f"pool-{runtime}"
    store.append_capacity_samples(
        [
            {
                "lane": lane,
                "window": "window",
                "source": "source",
                "target": None,
                "remaining_percent": remaining,
                "reset_at": reset_at,
                "observed_at": observed_at,
                "valid_until": valid_until,
                "payload": None,
            }
        ],
        runtime=runtime,
        scope_id="scope",
        observed_at=observed_at,
        valid_until=valid_until,
        payload={
            "pools": [
                {
                    "pool_id": pool_id,
                    "keys": [
                        {
                            "runtime": runtime,
                            "lane": lane,
                            "window": "window",
                            "target": None,
                            "source": "source",
                        }
                    ],
                }
            ],
            "routes": [
                {
                    "route_id": f"route-{runtime}",
                    "runtime": runtime,
                    "account": None,
                    "quota_lane": lane,
                    "pool_ids": [pool_id],
                }
            ],
        },
    )


def _config_text(home: Path) -> str:
    """Render a temporary config declaring the capacity-order fixture runtimes.

    ``home`` is the temporary agent-run home, so no binary or runtime home ever
    points at real user state. ``disabled`` proves the default facade drops
    disabled runtimes exactly like the injected-service path does.
    """

    runtimes = ""
    for name, enabled in (
        ("alpha", True),
        ("beta", True),
        ("empty", True),
        ("missing", True),
        ("disabled", False),
    ):
        runtimes += (
            f"\n[runtimes.{name}]\n"
            f"enabled = {str(enabled).lower()}\n"
            'adapter = "agent_run.adapters.codex.adapter:ADAPTER"\n'
            f'binary = "{home / name}"\n'
            f'home = "{home / name}-home"\n'
            'models = ["model"]\n'
        )
    return f"schema_version = 1\n{runtimes}"


def _durable_row_counts(store: StateStore) -> dict[str, int]:
    """Count the rows a read-only capacity order must leave untouched."""

    tables = ("capacity_samples", "capacity_route_snapshots", "agents", "deliveries")
    return {
        table: int(
            store.connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        )
        for table in tables
    }


class _CapacityService:
    """Record capacity-order calls and return a JSON-safe service payload."""

    def __init__(self, payload: dict[str, object]) -> None:
        """Initialize with the exact payload the CLI must emit unchanged."""

        self.payload = payload
        self.calls = 0

    def capacity_order(self) -> dict[str, object]:
        """Return the captured order and record one no-argument invocation."""

        self.calls += 1
        return self.payload


class CapacityOrderCliTests(unittest.TestCase):
    """Verify parser rejection and service-backed capacity-order output."""

    def test_order_emits_the_service_shape_once(self) -> None:
        """Emit concrete aliases and every public order field without reshaping it."""

        payload = {
            "observed_at": 100.0,
            "routes": [{"aliases": [{"runtime": "alpha", "account": "a", "model": "m"}]}],
            "deferred": [],
            "omitted": [{"runtime": "beta", "reason": "exhausted"}],
            "unavailable_runtimes": ["gamma"],
            "insufficient_diversity": True,
        }
        service = _CapacityService(payload)
        stdout = io.StringIO()

        self.assertEqual(main(["capacity", "order"], service=service, stdout=stdout), 0)
        self.assertEqual(json.loads(stdout.getvalue()), payload)
        self.assertEqual(service.calls, 1)

    def test_order_rejects_unexpected_arguments(self) -> None:
        """Reject ranking, role, model, and other arguments at the parser boundary."""

        service = _CapacityService({})
        self.assertEqual(main(["capacity", "order", "--model", "m"], service=service), 2)
        self.assertEqual(service.calls, 0)

    def test_real_state_reorders_and_explains_nonworking_runtimes(self) -> None:
        """New samples reorder routes while exhausted and missing stay explained."""

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            store = StateStore.initialize(home / "state.db")
            try:
                _append_scope(store, "alpha", remaining=80.0, observed_at=900.0)
                _append_scope(store, "beta", remaining=60.0, observed_at=900.0)
                _append_scope(store, "empty", remaining=0.0, observed_at=900.0)
                service = AgentService(
                    Config(
                        schema_version=1,
                        runtimes={
                            "alpha": _runtime("alpha"),
                            "beta": _runtime("beta"),
                            "empty": _runtime("empty"),
                            "missing": _runtime("missing"),
                        },
                    ),
                    store,
                    home,
                    launch=_unused_launch,
                    now=lambda: 1_000.0,
                )
                first = service.capacity_order()
                self.assertEqual(
                    [route.runtime for route in first.routes], ["alpha", "beta"]
                )
                self.assertEqual(first.omitted[0].runtime, "empty")
                self.assertEqual(first.unavailable_runtimes, ("missing",))

                _append_scope(store, "alpha", remaining=20.0, observed_at=950.0)
                _append_scope(store, "beta", remaining=90.0, observed_at=950.0)
                second = service.capacity_order()
                self.assertEqual(
                    [route.runtime for route in second.routes], ["beta", "alpha"]
                )

                stdout = io.StringIO()
                self.assertEqual(
                    main(["capacity", "order"], service=service, stdout=stdout), 0
                )
                payload = json.loads(stdout.getvalue())
                self.assertEqual(
                    [route["runtime"] for route in payload["routes"]],
                    ["beta", "alpha"],
                )
                self.assertEqual(payload["omitted"][0]["runtime"], "empty")
                self.assertEqual(payload["unavailable_runtimes"], ["missing"])
            finally:
                store.close()

    def test_default_facade_serves_capacity_order_without_injection(self) -> None:
        """Run the real non-injected command end to end against a temp home.

        Every other test here injects ``service``, so none reaches the
        ``_Runtime`` facade ``main`` builds by default. This one drives
        ``cli.main`` with ``--home`` only, a real config file, and a real
        ``StateStore``, then checks the order, the fresh clock epoch, and that
        no durable row was touched.
        """

        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            (home / "config.toml").write_text(_config_text(home), encoding="utf-8")
            store = StateStore.initialize(home / "state.db")
            try:
                now = time.time()
                fresh = {
                    "reset_at": now + 3_600.0,
                    "valid_until": now + 900.0,
                }
                _append_scope(store, "alpha", remaining=80.0, observed_at=now, **fresh)
                _append_scope(store, "beta", remaining=20.0, observed_at=now, **fresh)
                _append_scope(store, "empty", remaining=0.0, observed_at=now, **fresh)
                durable = _durable_row_counts(store)
            finally:
                store.close()

            started = time.time()
            stdout = io.StringIO()
            code = main(["--home", str(home), "capacity", "order"], stdout=stdout)
            finished = time.time()
            payload = json.loads(stdout.getvalue())

            self.assertEqual(code, 0)
            self.assertEqual(
                [route["runtime"] for route in payload["routes"]], ["alpha", "beta"]
            )
            self.assertEqual(payload["omitted"][0]["runtime"], "empty")
            self.assertEqual(payload["unavailable_runtimes"], ["missing"])
            self.assertNotIn("disabled", stdout.getvalue())
            self.assertTrue(started <= payload["observed_at"] <= finished)
            store = StateStore.open(home / "state.db")
            try:
                self.assertEqual(_durable_row_counts(store), durable)
            finally:
                store.close()
            self.assertFalse((home / "agents").exists())
