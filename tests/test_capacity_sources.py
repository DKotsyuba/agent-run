import base64
import json
import os
import subprocess
import sys
import tempfile
import unittest
import urllib.error
from datetime import datetime, timedelta, timezone
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
from agent_run.errors import CapacitySourceError


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
    def test_none_source_is_unsupported(self) -> None:
        def load(_name, _config):
            raise AssertionError("adapter must not be loaded")

        # "none" declares that the limits concept does not apply: the source
        # is unsupported, not a collected zero-sample round.
        self.assertIsNone(
            sources.collect_samples(
                "codex", _runtime_config(limits_source="none"), CapacityConfig(), load
            )
        )

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
        # A runtime codexbar cannot serve is unsupported, not a failure.
        with self.assertLogs("agent_run.capacity", level="WARNING"):
            self.assertIsNone(
                sources.collect_samples(
                    "opencode",
                    _runtime_config(limits_source="codexbar"),
                    CapacityConfig(),
                    None,
                )
            )


class TimestampTests(unittest.TestCase):
    def test_rejects_naive_and_garbage_stamps(self) -> None:
        # A naive stamp has an unknown timezone: honoring it as UTC would
        # invent freshness, so it is unknown exactly like an unparsable value.
        for value in ("2026-08-29T12:12:53", "", 12345, None, "not-a-stamp"):
            with self.subTest(value=value):
                self.assertIsNone(sources._timestamp(value))
        aware = sources._timestamp("2026-08-29T12:12:53Z")
        self.assertEqual(aware.utcoffset(), timedelta(0))
        offset = sources._timestamp("2026-09-01T15:00:00-04:00")
        self.assertEqual(offset.utcoffset(), timedelta(hours=-4))


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

    def collect(self, payload=None, *, urlopen=None, runtime=None, keychain_token_value="access-token"):
        payload = self._PAYLOAD if payload is None else payload
        opener = urlopen or (lambda request, timeout, **kwargs: self.Response(payload))
        with mock.patch.object(sources, "keychain_token", return_value=keychain_token_value), mock.patch.object(
            sources.urllib.request, "urlopen", side_effect=opener
        ), mock.patch.object(sources.time, "time", return_value=1788278400.0):
            return sources.collect_samples(
                "claude",
                runtime if runtime is not None else _runtime_config(limits_source="native"),
                CapacityConfig(),
                None,
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

    def test_missing_token_http_error_and_timeout_are_source_failures(self) -> None:
        # Missing token, HTTP failure, and timeout are failures, never
        # collected-empty evidence.
        with mock.patch.object(sources, "keychain_token", return_value=None):
            with self.assertRaises(CapacitySourceError) as raised:
                sources.collect_samples(
                    "claude", _runtime_config(limits_source="native"), CapacityConfig(), None
                )
        self.assertEqual(raised.exception.reason, "claude_token_missing")

        for error in (urllib.error.HTTPError("https://example.com", 401, "unauthorized", {}, None), TimeoutError()):
            with self.subTest(error=type(error).__name__):
                with self.assertRaises(CapacitySourceError) as raised:
                    self.collect(urlopen=mock.Mock(side_effect=error))
                self.assertEqual(raised.exception.reason, "claude_usage_unreachable")

    def test_malformed_present_entries_and_responses_are_source_failures(self) -> None:
        # A present limits entry with an unusable percent must not silently
        # disappear (the model would look unconstrained); a response without
        # a limits array or with an unparsable body is malformed.
        for payload in (
            {"limits": "nope"},
            {"other": []},
            {"limits": [{"kind": "session", "percent": True}]},
            {"limits": [{"kind": "session", "percent": float("nan")}]},
            {"limits": [{"kind": "session", "percent": "50"}]},
            {"limits": ["not-a-mapping"]},
        ):
            with self.subTest(payload=payload):
                with self.assertRaises(CapacitySourceError) as raised:
                    self.collect(payload)
                self.assertEqual(raised.exception.reason, "claude_malformed_response")

        class RawResponse(self.Response):
            def read(self):
                return b"not-json"

        with mock.patch.object(sources, "keychain_token", return_value="access-token"):
            with mock.patch.object(
                sources.urllib.request, "urlopen", return_value=RawResponse({})
            ):
                with self.assertRaises(CapacitySourceError) as raised:
                    sources.collect_samples(
                        "claude",
                        _runtime_config(limits_source="native"),
                        CapacityConfig(),
                        None,
                    )
        self.assertEqual(raised.exception.reason, "claude_malformed_response")

    def test_declared_oauth_env_takes_precedence_over_keychain(self) -> None:
        captured = {}

        def opener(request, timeout, context=None):
            captured["auth"] = request.headers["Authorization"]
            return self.Response({"limits": []})

        runtime = _runtime_config(
            limits_source="native",
            auth=RuntimeAuthConfig("env", names=("CLAUDE_CODE_OAUTH_TOKEN",)),
        )
        environment = {
            "CLAUDE_CODE_OAUTH_TOKEN": "env-oauth-value",
            "ANTHROPIC_API_KEY": "api-key-value",
        }
        with mock.patch.dict(os.environ, environment), mock.patch.object(
            sources, "keychain_token", return_value="keychain-value"
        ) as keychain:
            self.assertEqual(self.collect(urlopen=opener, runtime=runtime), ())
        self.assertEqual(captured["auth"], "Bearer env-oauth-value")
        keychain.assert_not_called()

    def test_undeclared_or_api_key_env_never_becomes_the_oauth_token(self) -> None:
        captured = {}

        def opener(request, timeout, context=None):
            captured["auth"] = request.headers["Authorization"]
            return self.Response({"limits": []})

        environment = {
            "CLAUDE_CODE_OAUTH_TOKEN": "env-oauth-value",
            "ANTHROPIC_API_KEY": "api-key-value",
        }
        # No auth declaration: an exported variable must not silently widen
        # the auth bridge, and an API key is never an OAuth token.
        with mock.patch.dict(os.environ, environment):
            self.assertEqual(self.collect(urlopen=opener, keychain_token_value="keychain-value"), ())
        self.assertEqual(captured["auth"], "Bearer keychain-value")


class CodexbarMappingTests(unittest.TestCase):
    def test_glm_maps_to_the_zai_provider(self) -> None:
        self.assertEqual(sources._CODEXBAR_PROVIDERS["glm"], "zai")

    def collect(self, payload, *, runtime=None):
        def run(argv):
            return Completed(
                json.dumps(payload)
                if not isinstance(payload, str)
                else payload
            )

        with mock.patch.object(sources, "_run_codexbar", side_effect=run):
            return sources.collect_samples(
                "codex",
                runtime if runtime is not None else _runtime_config(limits_source="codexbar"),
                CapacityConfig(),
                None,
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
            {"usage": {"updatedAt": "2026-08-29T12:12:53Z", "primary": {"usedPercent": 35, "windowMinutes": 300}}},
            {"usage": {"updatedAt": "2026-08-29T12:12:53Z", "primary": {"usedPercent": 99, "windowMinutes": 300}}},
        ]
        (sample,) = self.collect(payload)
        self.assertEqual(sample.remaining_percent, 65.0)
        self.assertEqual(
            sample.observed_at,
            datetime(2026, 8, 29, 12, 12, 53, tzinfo=timezone.utc),
        )

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
                {"usage": {"updatedAt": "2026-08-29T12:12:53Z", "accountEmail": "default@example.com", "primary": {"usedPercent": 3, "windowMinutes": 300}}},
                {"usage": {"updatedAt": "2026-08-29T12:12:53Z", "identity": {"accountEmail": "personal2@example.com"}, "primary": {"usedPercent": 4, "windowMinutes": 300}}},
                {"usage": {"updatedAt": "2026-08-29T12:12:53Z", "identity": {"accountEmail": "stranger@example.com"}, "primary": {"usedPercent": 5, "windowMinutes": 300}}},
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
        payload = [{"usage": {"updatedAt": "2026-08-29T12:12:53Z", "primary": {"usedPercent": 80.0, "windowMinutes": 30}}}]
        (sample,) = self.collect(payload)
        self.assertEqual(sample.window, "min30")

    def test_absent_lanes_are_absent_but_present_windows_must_be_valid(self) -> None:
        # A null or missing lane is real absence; a present window mapping
        # with unusable numbers fails the round instead of silently
        # dropping a governing constraint.
        (sample,) = self.collect(
            [{"usage": {"updatedAt": "2026-08-29T12:12:53Z", "primary": None, "secondary": {"usedPercent": 10, "windowMinutes": 300}}}]
        )
        self.assertEqual(sample.lane, "secondary")

        for window in (
            {"usedPercent": True, "windowMinutes": 300},
            {"usedPercent": float("nan"), "windowMinutes": 300},
            {"usedPercent": 50},
            {"usedPercent": 50, "windowMinutes": True},
            {"usedPercent": 50, "windowMinutes": 0},
            {"usedPercent": 50, "windowMinutes": -5},
            {},
        ):
            with self.subTest(window=window):
                with self.assertRaises(CapacitySourceError) as raised:
                    self.collect(
                        [{"usage": {"updatedAt": "2026-08-29T12:12:53Z", "primary": window}}]
                    )
                self.assertEqual(raised.exception.reason, "codexbar_invalid_window")

    def test_missing_or_naive_updated_at_cannot_revive_old_evidence(self) -> None:
        # Neither a missing nor a timezone-less observation stamp may fall
        # back to collector-now: that would make stale evidence fresh.
        for updated in (None, "2026-08-29T12:12:53", 1_785_000_000):
            with self.subTest(updated=updated):
                payload = [
                    {
                        "usage": {
                            "updatedAt": updated,
                            "primary": {"usedPercent": 3, "windowMinutes": 300},
                        }
                    }
                ]
                with self.assertRaises(CapacitySourceError) as raised:
                    self.collect(payload)
                self.assertEqual(
                    raised.exception.reason, "codexbar_invalid_observed_at"
                )

    def test_spawn_failure_timeout_nonzero_exit_and_garbage_fail_the_round(self) -> None:
        def raising_run(argv):
            raise OSError("codexbar missing")

        with mock.patch.object(sources, "_run_codexbar", side_effect=raising_run):
            with self.assertRaises(CapacitySourceError) as raised:
                sources.collect_samples(
                    "codex", _runtime_config(limits_source="codexbar"), CapacityConfig(), None
                )
        self.assertEqual(raised.exception.reason, "codexbar_spawn_failed")

        timed_out = subprocess.TimeoutExpired(cmd="codexbar", timeout=120)
        with mock.patch.object(sources, "_run_codexbar", side_effect=timed_out):
            with self.assertRaises(CapacitySourceError) as raised:
                sources.collect_samples(
                    "codex", _runtime_config(limits_source="codexbar"), CapacityConfig(), None
                )
        self.assertEqual(raised.exception.reason, "codexbar_timeout")

        garbage_cases = (
            ("garbage", "codexbar_malformed_response"),
            ("{}", "codexbar_malformed_response"),
            ("[]", "codexbar_missing_data"),
        )
        for stdout, reason in garbage_cases:
            with self.subTest(stdout=stdout):
                with self.assertRaises(CapacitySourceError) as raised:
                    self.collect(stdout)
                self.assertEqual(raised.exception.reason, reason)

    def test_empty_usage_object_is_an_invalid_observation(self) -> None:
        # A usage object without a timezone-aware updatedAt cannot be
        # aged honestly, so the round fails instead of reviving evidence.
        with self.assertRaises(CapacitySourceError) as raised:
            self.collect('{"usage":{}}')
        self.assertEqual(raised.exception.reason, "codexbar_invalid_observed_at")

    def test_single_object_payload_still_maps_one_account_without_accounts(self) -> None:
        payload = {"usage": {"updatedAt": "2026-08-29T12:12:53Z", "primary": {"usedPercent": 20, "windowMinutes": 300}}}
        (sample,) = self.collect(payload)
        self.assertEqual(sample.target, None)
        self.assertEqual(sample.remaining_percent, 80.0)

    def test_codexbar_timeout_allows_two_minutes(self) -> None:
        # 60s timed the claude provider out on every tick for hours; 120s
        # clears it while staying far under the 900s validity window.
        self.assertEqual(sources._CODEXBAR_TIMEOUT_SECONDS, 120)

    def test_failure_logs_and_exception_are_secret_safe(self) -> None:
        secret = "provider-secret-token"
        noisy = f"provider exploded {secret}\n"
        failed = Completed("[]", returncode=1, stderr=noisy)
        with mock.patch.object(
            sources, "_run_codexbar", return_value=failed
        ), self.assertLogs("agent_run.capacity", level="WARNING") as logs:
            with self.assertRaises(CapacitySourceError) as raised:
                sources.collect_samples(
                    "claude", _runtime_config(limits_source="codexbar"), CapacityConfig(), None
                )
        self.assertEqual(len(logs.output), 1)
        message = logs.output[0]
        self.assertIn("runtime=claude", message)
        self.assertIn("rc=1", message)
        self.assertIn("codexbar_nonzero_exit", message)
        self.assertNotIn(secret, message)
        self.assertNotIn(secret, str(raised.exception))


if __name__ == "__main__":
    unittest.main()
