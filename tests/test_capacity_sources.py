import base64
import json
import sys
import tempfile
import unittest
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.accounts import account_email, account_store_dir
from agent_run.adapters import omniroute
from agent_run.adapters.base import (
    ADAPTER_API_VERSION,
    Capability,
    LimitSample,
    RuntimeInfo,
)
from agent_run.capacity import sources
from agent_run.config import CapacityConfig, RuntimeAuthConfig, RuntimeConfig


def _runtime_config(**overrides) -> RuntimeConfig:
    defaults = dict(
        enabled=True,
        adapter="fake:ADAPTER",
        binary=Path("/usr/bin/fake"),
        home=Path("/tmp/fake-home"),
        models=("model-a",),
    )
    defaults.update(overrides)
    return RuntimeConfig(**defaults)


class FakeAdapter:
    def __init__(self, capabilities, samples=()):
        self._capabilities = capabilities
        self._samples = samples

    def describe(self) -> RuntimeInfo:
        return RuntimeInfo(
            name="fake", adapter_api_version=ADAPTER_API_VERSION, capabilities=self._capabilities
        )

    def limits(self, config, home):
        return self._samples


class Completed:
    def __init__(self, stdout, returncode=0, stderr=""):
        self.stdout = stdout
        self.returncode = returncode
        self.stderr = stderr


_CODEXBAR_PAYLOAD = [
    {
        "provider": "codex",
        "source": "oauth",
        "usage": {
            "accountEmail": "owner@example.com",
            "dataConfidence": "exact",
            "updatedAt": "2026-08-29T12:12:53Z",
            "primary": None,
            "secondary": {
                "usedPercent": 46,
                "windowMinutes": 10080,
                "resetsAt": "2026-09-03T16:26:47Z",
                "resetDescription": "weekly",
            },
            "tertiary": {"usedPercent": 5.5, "windowMinutes": 300, "resetsAt": None},
            "extraRateWindows": [],
        },
        "pace": "ignored",
        "credits": {"balance": 0},
        "openaiDashboard": "ignored",
    }
]


def _jwt(email: str | None) -> str:
    claims = {} if email is None else {"email": email}
    payload = base64.urlsafe_b64encode(json.dumps(claims).encode()).rstrip(b"=").decode()
    return f"header.{payload}.signature"


def _auth_file(path: Path, email: str | None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"tokens": {"id_token": _jwt(email)}}))


class AccountEmailTests(unittest.TestCase):
    def test_decodes_email_and_collapses_failures(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "auth.json"
            _auth_file(path, "a @b")
            self.assertEqual(account_email(path), "a @b")
            self.assertIsNone(account_email(path.with_name("missing.json")))
            path.write_text("garbage")
            self.assertIsNone(account_email(path))
            _auth_file(path, None)
            self.assertIsNone(account_email(path))


class DispatchTests(unittest.TestCase):
    def test_none_source_collects_zero_samples(self) -> None:
        def load(_name, _config):
            raise AssertionError("adapter must not be loaded")

        result = sources.collect_samples(
            "codex", _runtime_config(limits_source="none"), CapacityConfig(), load
        )
        self.assertEqual(result, ())

    def test_native_source_gates_on_live_limits_capability(self) -> None:
        sample = LimitSample(
            lane="requests",
            window="5h",
            remaining_percent=50.0,
            reset_at=None,
            observed_at=None,
            source="provider",
        )
        adapters = {
            "with": FakeAdapter(frozenset({Capability.LIVE_LIMITS}), samples=(sample,)),
            "without": FakeAdapter(frozenset()),
        }

        with_live = sources.collect_samples(
            "with", _runtime_config(), CapacityConfig(), lambda name, config: adapters[name]
        )
        self.assertEqual(with_live, (sample,))
        without_live = sources.collect_samples(
            "without", _runtime_config(), CapacityConfig(), lambda name, config: adapters[name]
        )
        self.assertIsNone(without_live)

    def test_omniroute_source_uses_the_pool_without_any_adapter(self) -> None:
        sample = LimitSample(
            lane="pool",
            window="session_5h",
            remaining_percent=95.0,
            reset_at=None,
            observed_at=None,
            source="omniroute_quota_pool",
        )

        def load(_name, _config):
            raise AssertionError("adapter must not be loaded")

        with mock.patch.object(omniroute, "pool_samples", return_value=(sample,)):
            result = sources.collect_samples(
                "opencode", _runtime_config(limits_source="omniroute"), CapacityConfig(), load
            )
        self.assertEqual(result, (sample,))

    def test_codexbar_source_rejects_undocumented_providers(self) -> None:
        with self.assertLogs("agent_run.capacity", level="WARNING"):
            result = sources.collect_samples(
                "opencode", _runtime_config(limits_source="codexbar"), CapacityConfig(), None
            )
        self.assertEqual(result, ())


class NativeClaudeMappingTests(unittest.TestCase):
    _PAYLOAD = {
        "limits": [
            {
                "kind": "session",
                "percent": 25,
                "severity": "warning",
                "resets_at": "2026-09-01T15:00:00-04:00",
                "scope": None,
                "is_active": True,
            },
            {
                "kind": "weekly_all",
                "percent": 40,
                "severity": "warning",
                "resets_at": "2026-09-07T12:00:00Z",
                "scope": None,
                "is_active": True,
            },
            {
                "kind": "weekly_scoped",
                "percent": 60,
                "severity": "warning",
                "resets_at": "2026-09-07T12:00:00Z",
                "scope": {"model": {"display_name": "Fable"}},
                "is_active": True,
            },
            {
                "kind": "mystery_limit",
                "percent": 10,
                "severity": "warning",
                "resets_at": None,
                "scope": None,
                "is_active": True,
            },
            {"kind": "weekly_all", "resets_at": None},
        ]
    }

    class Response:
        def __init__(self, payload):
            self._payload = payload

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def read(self):
            return json.dumps(self._payload).encode()

    def collect(self, payload=None, *, urlopen=None):
        payload = self._PAYLOAD if payload is None else payload
        opener = urlopen or (lambda request, timeout, **kwargs: self.Response(payload))
        with mock.patch.object(sources, "keychain_token", return_value="access-token"), mock.patch.object(
            sources.urllib.request, "urlopen", side_effect=opener
        ), mock.patch.object(sources.time, "time", return_value=1788278400.0):
            return sources.collect_samples(
                "claude", _runtime_config(limits_source="native"), CapacityConfig(), None
            )

    def test_live_payload_maps_lanes_targets_and_remaining(self) -> None:
        samples = self.collect()
        self.assertEqual(
            [(sample.lane, sample.window, sample.target, sample.remaining_percent) for sample in samples],
            [
                ("primary", "five_hour", None, 75.0),
                ("secondary", "seven_day", None, 60.0),
                ("secondary", "seven_day", "fable", 40.0),
                ("secondary", "seven_day", "mystery_limit", 90.0),
            ],
        )
        self.assertEqual(samples[0].source, "native")
        self.assertEqual(samples[0].valid_for_seconds, 900)
        self.assertEqual(
            samples[0].reset_at,
            datetime(2026, 9, 1, 19, 0, tzinfo=timezone.utc),
        )

    def test_request_uses_oauth_headers_and_timeout(self) -> None:
        captured = {}

        def opener(request, timeout, context=None):
            captured.update(
                url=request.full_url,
                headers=dict(request.headers),
                timeout=timeout,
                context=context,
            )
            return self.Response({"limits": []})

        self.assertEqual(self.collect(urlopen=opener), ())
        self.assertEqual(captured["url"], "https://api.anthropic.com/api/oauth/usage")
        self.assertEqual(captured["headers"]["Authorization"], "Bearer access-token")
        self.assertEqual(captured["headers"]["Anthropic-beta"], "oauth-2025-04-20")
        self.assertEqual(captured["timeout"], 20)

    def test_missing_token_http_error_and_timeout_are_no_samples(self) -> None:
        with mock.patch.object(sources, "keychain_token", return_value=None), self.assertLogs(
            "agent_run.capacity", level="WARNING"
        ):
            self.assertEqual(
                sources.collect_samples(
                    "claude", _runtime_config(limits_source="native"), CapacityConfig(), None
                ),
                (),
            )

        for error in (urllib.error.HTTPError("https://example.com", 401, "unauthorized", {}, None), TimeoutError()):
            with self.subTest(error=type(error).__name__), self.assertLogs(
                "agent_run.capacity", level="WARNING"
            ):
                self.assertEqual(self.collect(urlopen=mock.Mock(side_effect=error)), ())


class CodexbarMappingTests(unittest.TestCase):
    def test_glm_maps_to_the_zai_provider(self) -> None:
        self.assertEqual(sources._CODEXBAR_PROVIDERS["glm"], "zai")

    def collect(self, payload):
        def run(argv):
            return Completed(
                json.dumps(payload)
                if not isinstance(payload, str)
                else payload
            )

        with mock.patch.object(sources, "_run_codexbar", side_effect=run):
            return sources.collect_samples(
                "codex", _runtime_config(limits_source="codexbar"), CapacityConfig(), None
            )

    def test_real_shape_maps_lanes_and_shelves_honestly(self) -> None:
        samples = self.collect(_CODEXBAR_PAYLOAD)
        self.assertEqual(len(samples), 2)
        secondary, tertiary = samples
        self.assertEqual(
            (secondary.lane, secondary.window, secondary.remaining_percent),
            ("secondary", "seven_day", 54.0),
        )
        self.assertEqual(secondary.source, "codexbar")
        self.assertIsNone(secondary.target)
        self.assertEqual(secondary.valid_for_seconds, 900)
        self.assertEqual(
            secondary.reset_at,
            datetime(2026, 9, 3, 16, 26, 47, tzinfo=timezone.utc),
        )
        self.assertEqual(
            secondary.observed_at,
            datetime(2026, 8, 29, 12, 12, 53, tzinfo=timezone.utc),
        )
        self.assertEqual(
            (tertiary.lane, tertiary.window), ("tertiary", "five_hour")
        )

    def test_first_account_only_and_ignored_sections(self) -> None:
        payload = [
            {"usage": {"updatedAt": None, "primary": {"usedPercent": 35}}},
            {"usage": {"updatedAt": None, "primary": {"usedPercent": 99}}},
        ]
        (sample,) = self.collect(payload)
        self.assertEqual(sample.remaining_percent, 65.0)
        self.assertIsNone(sample.observed_at)

    def test_declared_accounts_map_targets_and_add_all_accounts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            home = Path(directory)
            default_auth = home / "default-auth.json"
            _auth_file(default_auth, "default@example.com")
            _auth_file(
                account_store_dir(home, "codex", "personal1") / "auth.json",
                "personal1@example.com",
            )
            _auth_file(
                account_store_dir(home, "codex", "personal2") / "auth.json",
                "personal2@example.com",
            )
            payload = [
                {"usage": {"accountEmail": "default@example.com", "primary": {"usedPercent": 3, "windowMinutes": 300}}},
                {"usage": {"identity": {"accountEmail": "personal2@example.com"}, "primary": {"usedPercent": 4, "windowMinutes": 300}}},
                {"usage": {"identity": {"accountEmail": "stranger@example.com"}, "primary": {"usedPercent": 5, "windowMinutes": 300}}},
            ]
            captured = []

            def run(argv):
                captured.append(argv)
                return Completed(json.dumps(payload))

            runtime = _runtime_config(
                limits_source="codexbar",
                accounts=("personal1", "personal2"),
                auth=RuntimeAuthConfig("file_link", source=default_auth, target="auth.json"),
            )
            with mock.patch.object(sources, "_run_codexbar", side_effect=run):
                samples = sources.collect_samples(
                    "codex", runtime, CapacityConfig(), None, home
                )

        self.assertEqual([sample.target for sample in samples], [None, "personal2", "stranger@example.com"])
        self.assertIn("--all-accounts", captured[0])

    def test_unknown_window_minutes_names_itself(self) -> None:
        payload = [{"usage": {"primary": {"usedPercent": 80.0, "windowMinutes": 30}}}]
        (sample,) = self.collect(payload)
        self.assertEqual(sample.window, "min30")

    def test_bool_and_nonfinite_and_missing_usage_are_no_samples(self) -> None:
        for window in (
            {"usedPercent": True, "windowMinutes": 300},
            {"usedPercent": float("nan"), "windowMinutes": 300},
            {},
        ):
            with self.subTest(window=window):
                result = self.collect([{"usage": {"primary": window}}])
                self.assertEqual(result, ())

    def test_command_failure_and_garbage_are_no_samples(self) -> None:
        def raising_run(argv):
            raise OSError("codexbar missing")

        with mock.patch.object(sources, "_run_codexbar", side_effect=raising_run):
            self.assertEqual(
                sources.collect_samples(
                    "codex", _runtime_config(limits_source="codexbar"), CapacityConfig(), None
                ),
                (),
            )

        for stdout in ("garbage", '{"usage":{}}', "{}", "[]"):
            with self.subTest(stdout=stdout), self.assertLogs(
                "agent_run.capacity", level="WARNING"
            ):
                result = self.collect(stdout)
                self.assertEqual(result, ())

    def test_single_object_payload_still_maps_one_account_without_accounts(self) -> None:
        payload = {"usage": {"primary": {"usedPercent": 20, "windowMinutes": 300}}}
        (sample,) = self.collect(payload)
        self.assertEqual(sample.target, None)
        self.assertEqual(sample.remaining_percent, 80.0)

    def test_codexbar_timeout_allows_two_minutes(self) -> None:
        # 60s timed the claude provider out on every tick for hours; 120s
        # clears it while staying far under the 900s validity window.
        self.assertEqual(sources._CODEXBAR_TIMEOUT_SECONDS, 120)

    def test_bounded_error_tail_collapses_to_one_bounded_line(self) -> None:
        noisy = "first line\nsecond line " + "x" * 400
        tail = sources._bounded_error_tail(noisy)
        self.assertEqual(tail, " ".join(noisy.split())[:200])
        self.assertEqual(len(tail), 200)
        self.assertNotIn("\n", tail)
        self.assertEqual(sources._bounded_error_tail(None), "")
        self.assertEqual(sources._bounded_error_tail(b"raw bytes\n"), "raw bytes")

    def test_failure_warning_carries_the_bounded_stderr_tail(self) -> None:
        noisy = "provider exploded\n" + "x" * 400
        failed = Completed("[]", returncode=1, stderr=noisy)
        with mock.patch.object(
            sources, "_run_codexbar", return_value=failed
        ), self.assertLogs("agent_run.capacity", level="WARNING") as logs:
            result = sources.collect_samples(
                "claude", _runtime_config(limits_source="codexbar"), CapacityConfig(), None
            )
        self.assertEqual(result, ())
        self.assertEqual(len(logs.output), 1)
        message = logs.output[0]
        self.assertIn("runtime=claude", message)
        self.assertIn("rc=1", message)
        self.assertIn("stderr=provider exploded", message)
        self.assertNotIn("\n", message)


if __name__ == "__main__":
    unittest.main()
