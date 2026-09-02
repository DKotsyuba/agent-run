"""Service and dispatch contracts for capacity-aware route ordering."""

from __future__ import annotations

import sys
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path
from typing import cast

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.config import Config, RuntimeConfig
from agent_run.dispatch import Session, _jsonable, call_tool
from agent_run.service import AgentService
from agent_run.state import StateStore


#: Single deterministic service timestamp used by persisted test evidence.
_NOW = 1_000.0


def _runtime(
    name: str, *, multiplier: float = 1.0, enabled: bool = True
) -> RuntimeConfig:
    """Build an opaque runtime configuration for capacity-only service calls."""

    return RuntimeConfig(
        enabled=enabled,
        adapter="agent_run.adapters.codex.adapter:ADAPTER",
        binary=Path(f"/tmp/{name}"),
        home=Path(f"/tmp/{name}-home"),
        models=("model",),
        priority_multiplier=multiplier,
    )


def _unused_launch(*_args: object, **_kwargs: object) -> None:
    """Fail if a read-only capacity service test unexpectedly launches work."""

    del _args, _kwargs
    raise AssertionError("capacity_order must not launch an agent")


class CapacityServiceTests(unittest.TestCase):
    """Verify configuration filtering and transport-neutral JSON output."""

    def setUp(self) -> None:
        """Create one isolated store and home for each service contract test."""

        self._directory = tempfile.TemporaryDirectory()
        self.home = Path(self._directory.name)
        self.store = StateStore.initialize(self.home / "state.db")

    def tearDown(self) -> None:
        """Close the thread-affine store and remove its temporary directory."""

        self.store.close()
        self._directory.cleanup()

    def _service(
        self, runtimes: dict[str, RuntimeConfig], now: Callable[[], float]
    ) -> AgentService:
        """Create an AgentService whose only exercised surface is capacity reads."""

        return AgentService(
            Config(schema_version=1, runtimes=runtimes),
            self.store,
            self.home,
            launch=_unused_launch,
            now=now,
        )

    def _append_scope(
        self,
        runtime: str,
        scope: str,
        *,
        remaining: float,
        pool_id: str,
        route_ids: tuple[str, ...] = ("route",),
        accounts: tuple[str | None, ...] = (None,),
        valid_until: float = 1_100.0,
    ) -> None:
        """Persist one sample and explicit topology with optional route aliases."""

        lane = f"lane-{scope}"
        key = {
            "runtime": runtime,
            "lane": lane,
            "window": "window",
            "target": None,
            "source": "source",
        }
        self.store.append_capacity_samples(
            [
                {
                    "lane": lane,
                    "window": "window",
                    "source": "source",
                    "target": None,
                    "remaining_percent": remaining,
                    "reset_at": 2_000.0,
                    "observed_at": 900.0,
                    "valid_until": valid_until,
                    "payload": None,
                }
            ],
            runtime=runtime,
            scope_id=scope,
            observed_at=900.0,
            valid_until=valid_until,
            payload={
                "pools": [{"pool_id": pool_id, "keys": [key]}],
                "routes": [
                    {
                        "route_id": route_id,
                        "runtime": runtime,
                        "account": account,
                        "quota_lane": f"quota-{index}",
                        "pool_ids": [pool_id],
                    }
                    for index, (route_id, account) in enumerate(
                        zip(route_ids, accounts, strict=True)
                    )
                ],
            },
        )

    def test_service_reads_clock_once_and_filters_runtime_availability(self) -> None:
        """Disabled evidence is absent and one healthy sibling keeps runtime usable."""

        self._append_scope(
            "enabled", "fresh", remaining=60.0, pool_id="enabled-fresh"
        )
        self._append_scope(
            "enabled",
            "expired",
            remaining=60.0,
            pool_id="enabled-expired",
            valid_until=950.0,
        )
        self._append_scope(
            "disabled", "fresh", remaining=90.0, pool_id="disabled-fresh"
        )
        calls: list[float] = []

        def now() -> float:
            """Record and return the one service ranking timestamp."""

            calls.append(_NOW)
            return _NOW

        service = self._service(
            {
                "enabled": _runtime("enabled"),
                "empty": _runtime("empty"),
                "disabled": _runtime("disabled", enabled=False),
            },
            now,
        )
        order = service.capacity_order()
        self.assertEqual(calls, [_NOW])
        self.assertEqual([item.runtime for item in order.routes], ["enabled"])
        self.assertEqual(order.unavailable_runtimes, ("empty",))
        self.assertNotIn("disabled", repr(order))

    def test_dispatch_serializes_multiplier_aliases_omissions_and_limits_shape(self) -> None:
        """Shared transports retain concrete aliases and the legacy limits schema."""

        self._append_scope(
            "boosted",
            "shared",
            remaining=20.0,
            pool_id="shared-pool",
            route_ids=("route-a", "route-b"),
            accounts=("account-a", "account-b"),
        )
        self._append_scope(
            "ordinary", "fresh", remaining=80.0, pool_id="ordinary-pool"
        )
        self._append_scope(
            "exhausted", "fresh", remaining=0.0, pool_id="exhausted-pool"
        )
        service = self._service(
            {
                "boosted": _runtime("boosted", multiplier=5.0),
                "ordinary": _runtime("ordinary"),
                "exhausted": _runtime("exhausted", multiplier=100.0),
            },
            lambda: _NOW,
        )
        session = Session()
        result = cast(
            dict[str, object],
            _jsonable(call_tool(service, "capacity_order", {}, session)),
        )
        routes = cast(list[dict[str, object]], result["routes"])
        aliases = cast(list[dict[str, object]], routes[0]["aliases"])
        omitted = cast(list[dict[str, object]], result["omitted"])
        self.assertEqual(routes[0]["runtime"], "boosted")
        self.assertEqual(routes[0]["multiplier"], 5.0)
        self.assertEqual(
            [alias["account"] for alias in aliases],
            ["account-a", "account-b"],
        )
        self.assertNotIn(
            "exhausted", [item["runtime"] for item in routes]
        )
        self.assertEqual(omitted[0]["runtime"], "exhausted")
        self.assertFalse(result["insufficient_diversity"])

        limits = cast(
            dict[str, object], _jsonable(call_tool(service, "limits", {}, session))
        )
        items = cast(list[dict[str, object]], limits["items"])
        self.assertEqual(
            set(items[0]),
            {
                "key",
                "known",
                "remaining_percent",
                "reset_at",
                "warmup",
                "risk",
                "recommendations",
            },
        )


if __name__ == "__main__":
    unittest.main()
