"""Regression tests for configuration-to-capacity multiplier precedence."""

from types import SimpleNamespace
from typing import Any, cast
from unittest import TestCase
from unittest.mock import patch

from agent_run.capacity.order import build_capacity_order
from agent_run.capacity.ranking import CapacityOrder
from agent_run.capacity.snapshot import CapacityRouteSnapshot


class CapacityOrderTests(TestCase):
    """Ensure the shared builder scopes route overrides by runtime."""

    def test_account_precedes_lane_and_runtime_for_shared_route_ids(self) -> None:
        """The builder emits independent pair keys and account wins precedence."""

        route = SimpleNamespace(
            descriptor=SimpleNamespace(
                runtime="provider-a", route_id="shared", account="new-account", quota_lane="new-lane"
            )
        )
        runtime = SimpleNamespace(
            enabled=True,
            priority_multiplier=1.0,
            priority_account_multipliers={"new-account": 3.0},
            priority_lane_multipliers={"new-lane": 2.0},
        )
        config = SimpleNamespace(
            runtimes={"provider-a": runtime},
            capacity=SimpleNamespace(sample_retention=10),
        )
        ranked = CapacityOrder(0.0, (), (), (), (), True)
        with patch("agent_run.capacity.order.build_capacity_routes", return_value=CapacityRouteSnapshot((cast(Any, route),), (), ())), patch(
            "agent_run.capacity.order.rank_capacity_routes", return_value=ranked
        ) as rank:
            build_capacity_order(cast(Any, SimpleNamespace()), cast(Any, config), now=0.0)
        self.assertEqual(rank.call_args.kwargs["route_multipliers"], {("provider-a", "shared"): 3.0})

    def test_new_accounts_fallbacks_and_runtime_filtering(self) -> None:
        """Scope equal route ids, apply all fallbacks, and retain unknown runtimes."""

        descriptors = (
            ("provider-a", "shared", "first", "lane"),
            ("provider-a", "second", "second", "lane"),
            ("provider-a", "third", "third", "other"),
            ("provider-b", "shared", None, "lane"),
            ("disabled", "shared", None, "lane"),
        )
        routes = tuple(SimpleNamespace(descriptor=SimpleNamespace(
            runtime=name, route_id=route_id, account=account, quota_lane=lane,
        )) for name, route_id, account, lane in descriptors)
        runtimes = {name: SimpleNamespace(
            enabled=name != "disabled", priority_multiplier=factor,
            priority_account_multipliers={"first": 3.0} if name == "provider-a" else {},
            priority_lane_multipliers={"lane": 2.0} if name == "provider-a" else {},
        ) for name, factor in (("provider-a", 0.5), ("provider-b", 4.0),
                               ("disabled", 9.0), ("unknown", 1.0))}
        config = SimpleNamespace(runtimes=runtimes,
                                 capacity=SimpleNamespace(sample_retention=10))
        with patch("agent_run.capacity.order.build_capacity_routes",
                   return_value=CapacityRouteSnapshot(cast(Any, routes), (), ())), patch(
            "agent_run.capacity.order.rank_capacity_routes",
            return_value=CapacityOrder(0.0, (), (), (), (), True),
        ) as rank:
            result = build_capacity_order(cast(Any, None), cast(Any, config), now=0.0)
        self.assertEqual(rank.call_args.kwargs["route_multipliers"], {
            ("provider-a", "shared"): 3.0, ("provider-a", "second"): 2.0,
            ("provider-a", "third"): 0.5, ("provider-b", "shared"): 4.0,
        })
        self.assertEqual(len(rank.call_args.args[0].routes), 4)
        self.assertNotIn("disabled", rank.call_args.args[1])
        self.assertEqual(result.unavailable_runtimes, ("unknown",))
