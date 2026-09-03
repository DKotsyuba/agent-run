"""Regression checks for nullable account identities and incomplete quotas."""

from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from agent_run.adapters.base import LimitSample
from agent_run.capacity import sources
from agent_run.capacity.codex_appserver import normalize_rate_limits
from agent_run.capacity.collect import persist_slice
from agent_run.capacity.snapshot import build_capacity_routes
from agent_run.capacity.topology import account_token
from agent_run.config import RuntimeConfig
from agent_run.state import StateStore


def _response(account_id: str = "fictitious") -> dict:
    """Return one fresh quota bucket with a fictitious backend identity."""

    return {"accountId": account_id, "rateLimitsByLimitId": {
        "bucket": {"primary": {"usedPercent": 10, "windowDurationMins": 300,
                                "resetsAt": 2000}},
    }}


def _config() -> RuntimeConfig:
    """Return a minimal configuration with a legal sentinel-looking label."""

    return RuntimeConfig(True, "fake:ADAPTER", Path("/fake"), Path("/fake-home"),
                         ("model",), accounts=("base",))


class CapacityIdentityTests(unittest.TestCase):
    """Ensure opaque labels never erase base capacity or unknown windows."""

    def test_base_label_persists_beside_the_absent_account(self) -> None:
        """Both scopes and pool ids survive one real atomic persistence round."""

        with patch("agent_run.adapters.codex.rate_limits.read_rate_limits",
                   side_effect=[_response("a"), _response("b")]):
            slices = sources.collect_codex_appserver_slices("runtime", _config(), 1000)
        self.assertEqual(len(slices), 2)
        self.assertEqual(len({item.scope_id for item in slices}), 2)
        self.assertEqual(len({pool.pool_id for item in slices for pool in item.topology.pools}), 2)
        with tempfile.TemporaryDirectory() as directory:
            store = StateStore.initialize(Path(directory) / "state.db")
            try:
                for item in slices:
                    persist_slice(store, item)
                self.assertEqual(len(store.capacity_route_snapshots()), 2)
                snapshot = build_capacity_routes(store, retention=10, now=1001)
                self.assertEqual(len(snapshot.routes), 2)
                self.assertFalse(snapshot.deferred)
            finally:
                store.close()

    def test_codexbar_default_and_shared_labels_remain_independent(self) -> None:
        """Neither route nor pool sentinel may collapse a literal account label."""

        config = replace(_config(), limits_source="codexbar", accounts=("default", "shared"))
        samples = tuple(LimitSample(
            lane="primary", window="five_hour", remaining_percent=50,
            reset_at=datetime.fromtimestamp(2000, timezone.utc),
            observed_at=datetime.fromtimestamp(1000, timezone.utc),
            source="codexbar", target=target, valid_for_seconds=900,
        ) for target in (None, "default", "shared"))
        topology = sources.sample_topology("runtime", config, samples)
        self.assertEqual(len(topology.routes), 3)
        self.assertEqual(len(topology.pools), 3)
        self.assertTrue(all(len(pool.keys) == 1 for pool in topology.pools))
        self.assertEqual(topology, sources.sample_topology("runtime", config, tuple(reversed(samples))))

    def test_account_tokens_are_injective_for_separator_like_labels(self) -> None:
        """Nullable labels and escaped separators produce stable distinct tokens."""

        labels = (None, "base", "default", "shared", "@base", "a:b", "a%3Ab", "α")
        tokens = [account_token(label, absent_token="base") for label in labels]
        self.assertEqual(len(set(tokens)), len(labels))
        self.assertEqual(tokens[0], "base")
        self.assertTrue(all(":" not in token for token in tokens))

    def test_malformed_present_window_never_becomes_absent_capacity(self) -> None:
        """A valid primary cannot make a bucket with unknown secondary routable."""

        for malformed in ("bad", {}, {"usedPercent": "oops", "windowDurationMins": 10080}):
            with self.subTest(window=malformed):
                payload = _response()
                payload["rateLimitsByLimitId"]["bucket"]["secondary"] = malformed
                samples, topology, _ = normalize_rate_limits("runtime", None, payload, 1000)
                self.assertEqual(len(samples), 1)
                self.assertFalse(topology.routes)
        payload = _response()
        payload["rateLimitsByLimitId"]["bucket"]["secondary"] = None
        self.assertEqual(len(normalize_rate_limits("runtime", None, payload, 1000)[1].routes), 1)

    def test_valid_bucket_survives_a_malformed_sibling(self) -> None:
        """Independent valid buckets remain routable when a sibling is incomplete."""

        payload = _response()
        payload["rateLimitsByLimitId"]["bad"] = {
            "primary": {"usedPercent": 20, "windowDurationMins": 300},
            "secondary": {"usedPercent": "oops", "windowDurationMins": 10080},
        }
        samples, topology, _ = normalize_rate_limits("runtime", None, payload, 1000)
        self.assertEqual(len(samples), 2)
        self.assertEqual([route.quota_lane for route in topology.routes], ["bucket"])
