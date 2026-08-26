import base64
import io
import json
import os
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.opencode.service import PASSWORD_ENV
from agent_run.adapters.opencode.http import (
    HEALTH_PATH,
    MAX_POLL_INTERVAL_SECONDS,
    MODEL_PATH,
    NO_CONTENT,
    SESSION_STATUS_PATH,
    HttpError,
    OpenCodeHttpClient,
    PollTimeout,
    RetryPolicy,
    TransientHttpError,
)
from agent_run.errors import ValidationError


class FakeReply:
    def __init__(self, payload, status=200):
        body = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")
        self._buffer = io.BytesIO(body)
        self.status = status
        self.closed = False
        self.reads = 0

    def read(self, size=-1):
        self.reads += 1
        return self._buffer.read(size)

    def close(self):
        self.closed = True


class FakeOpener:
    def __init__(self, *replies):
        self.replies = list(replies)
        self.calls = []
        self.authorizations = []

    def __call__(self, request, timeout=None):
        self.calls.append((request.get_method(), request.full_url, request.data))
        self.authorizations.append(request.get_header("Authorization"))
        reply = self.replies.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        return reply


def http_error(code):
    return urllib.error.HTTPError("http://127.0.0.1:41777/x", code, "boom", {}, None)


class ClientCase(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.directory = Path(self._temp.name).resolve()
        self.slept = []

    def client(self, *replies, **kwargs):
        self.opener = FakeOpener(*replies)
        kwargs.setdefault("retry", RetryPolicy(attempts=3, base_seconds=0.01, cap_seconds=0.04))
        return OpenCodeHttpClient(
            "http://127.0.0.1:41777",
            self.directory,
            opener=self.opener,
            sleep=self.slept.append,
            **kwargs,
        )

    def captures(self):
        return sorted(path.name for path in self.directory.iterdir())


class AuthenticationTests(ClientCase):
    def test_explicit_password_authenticates_every_endpoint(self):
        password = "fixture-password"
        client = self.client(
            FakeReply({"healthy": True, "pid": 1, "version": "2.1.0"}),
            FakeReply({"data": [], "location": {}}),
            FakeReply({"data": {}}),
            FakeReply({"data": {"id": "ses_1"}}),
            FakeReply({"data": {}}),
            FakeReply({}),
            FakeReply([]),
            FakeReply([]),
            FakeReply(b"", status=NO_CONTENT),
            password=password,
        )
        client.health()
        client.models()
        client.session_status()
        client.create_session({"agent": "agent-run"})
        client.prompt_async("ses_1", {"parts": []})
        client.abort("ses_1")
        client.messages("ses_1").release()
        client.permissions("ses_1").release()
        client.answer_permission("ses_1", "perm_1", {"response": "reject"})

        encoded = base64.b64encode(f"opencode:{password}".encode("utf-8")).decode("ascii")
        self.assertEqual(self.opener.authorizations, [f"Basic {encoded}"] * 9)

    def test_live_client_refuses_missing_or_blank_password(self):
        for environment in ({}, {PASSWORD_ENV: "   "}):
            with self.subTest(environment=environment):
                with mock.patch.dict(os.environ, environment, clear=True):
                    with self.assertRaises(ValidationError) as caught:
                        OpenCodeHttpClient("http://127.0.0.1:41777", self.directory)
                self.assertEqual(
                    str(caught.exception),
                    f"{PASSWORD_ENV} must be set to a nonblank value",
                )

    def test_injected_opener_may_remain_unauthenticated(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            client = self.client(FakeReply({"ok": True}))
            self.assertEqual(client.health(), {"ok": True})
        self.assertEqual(self.opener.authorizations, [None])

    def test_authentication_error_never_exposes_credentials_or_retries(self):
        password = "fixture-password"
        encoded = base64.b64encode(f"opencode:{password}".encode("utf-8")).decode("ascii")
        header = f"Basic {encoded}"
        error = urllib.error.HTTPError(
            "http://127.0.0.1:41777/global/health", 401, header, None, None
        )
        client = self.client(error, password=password)
        with self.assertRaises(HttpError) as caught:
            client.health()
        message = str(caught.exception)
        self.assertIn("authentication failed", message)
        self.assertNotIn(password, message)
        self.assertNotIn(header, message)
        self.assertEqual(client.attempts, 1)


class CaptureTests(ClientCase):
    def test_reply_is_captured_to_a_private_file_before_decoding(self):
        client = self.client(FakeReply({"state": "completed"}))
        response = client.get("/session/s1")
        self.assertTrue(response.body_path.is_file())
        self.assertEqual(response.body_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(response.body_path.parent, self.directory)
        self.assertEqual(response.mapping()["state"], "completed")
        self.assertEqual(response.raw_ref, str(response.body_path))

    def test_release_deletes_the_capture_and_is_idempotent(self):
        client = self.client(FakeReply({"state": "completed"}))
        response = client.get("/session/s1")
        response.release()
        response.release()
        self.assertFalse(response.body_path.exists())
        self.assertEqual(self.captures(), [])
        with self.assertRaises(ValidationError):
            response.mapping()

    def test_release_survives_a_capture_deleted_underneath_it(self):
        client = self.client(FakeReply({"ok": True}))
        response = client.get("/session/s1")
        response.body_path.unlink()
        response.release()
        self.assertTrue(response.released)

    def test_context_manager_releases_even_on_a_decode_failure(self):
        client = self.client(FakeReply(b"{not json"))
        with self.assertRaises(ValidationError):
            with client.get("/session/s1") as response:
                response.json()
        self.assertEqual(self.captures(), [])

    def test_no_content_carries_no_json(self):
        client = self.client(FakeReply(b"", status=NO_CONTENT), FakeReply(b"{}", status=NO_CONTENT))
        response = client.get("/session/s1")
        self.assertIsNone(response.json())
        self.assertEqual(dict(response.mapping()), {})
        with self.assertRaises(ValidationError):
            client.get("/session/s1").json()

    def test_large_reply_is_refused_instead_of_truncated(self):
        payload = b'{"text": "' + b"x" * 4096 + b'"}'
        client = self.client(FakeReply(payload), max_bytes=1024)
        with self.assertRaises(ValidationError):
            client.get("/session/s1/message")
        self.assertEqual(self.captures(), [])

    def test_non_object_reply_is_refused(self):
        client = self.client(FakeReply([1, 2]))
        with self.assertRaises(ValidationError):
            client.get("/session/s1").mapping()

    def test_invalid_json_is_refused_from_the_file(self):
        client = self.client(FakeReply(b"{not json"))
        with self.assertRaises(ValidationError):
            client.get("/session/s1").json()


class CaptureLifetimeTests(ClientCase):
    def test_decoded_endpoints_keep_no_capture(self):
        client = self.client(
            FakeReply({"ok": True}),
            FakeReply({"data": {"ses_1": {"state": "idle"}}}),
            FakeReply({"data": {"id": "ses_1"}}),
            FakeReply(b"", status=NO_CONTENT),
        )
        client.health()
        client.session_status()
        client.create_session({"agent": "agent-run"})
        client.abort("ses_1")
        self.assertEqual(self.captures(), [])

    def test_only_the_transcript_capture_survives(self):
        client = self.client(FakeReply({"ok": True}), FakeReply([{"info": {"role": "user"}}]))
        client.health()
        transcript = client.messages("ses_1")
        self.assertEqual(self.captures(), [transcript.body_path.name])

    def test_transient_reply_body_is_deleted_before_the_retry(self):
        client = self.client(FakeReply({"retry": 1}, status=503), FakeReply({"ok": True}))
        self.assertEqual(dict(client.health()), {"ok": True})
        self.assertEqual(client.attempts, 2)
        self.assertEqual(self.captures(), [])

    def test_error_body_is_read_for_detail_then_deleted(self):
        client = self.client(FakeReply({"error": "no such session"}, status=404))
        with self.assertRaises(HttpError) as caught:
            client.health()
        self.assertIn("no such session", str(caught.exception))
        self.assertEqual(self.captures(), [])

    def test_poll_deletes_every_non_final_capture(self):
        client = self.client(
            FakeReply({"state": "running"}),
            FakeReply({"state": "running"}),
            FakeReply({"state": "completed"}),
        )
        payload = client.poll(
            "/session/s1",
            lambda item: item.get("state") == "completed",
            deadline_seconds=5,
            interval_seconds=0.25,
        )
        self.assertEqual(payload["state"], "completed")
        self.assertEqual(self.captures(), [])


class WaitEndpointTests(ClientCase):
    def test_long_wait_endpoint_is_refused(self):
        client = self.client()
        for path in ("/session/s1/wait", "/session/s1/message?wait=30", "/longpoll/s1"):
            with self.assertRaises(ValidationError):
                client.get(path)
        self.assertEqual(self.opener.calls, [])

    def test_poll_interval_is_bounded(self):
        client = self.client(FakeReply({"state": "running"}))
        with self.assertRaises(ValidationError):
            client.poll(
                "/session/s1",
                lambda payload: True,
                deadline_seconds=5,
                interval_seconds=MAX_POLL_INTERVAL_SECONDS + 0.1,
            )

    def test_poll_returns_on_first_ready_state(self):
        client = self.client(
            FakeReply({"state": "running"}),
            FakeReply({"state": "running"}),
            FakeReply({"state": "completed"}),
        )
        payload = client.poll(
            "/session/s1",
            lambda item: item.get("state") == "completed",
            deadline_seconds=5,
            interval_seconds=0.25,
        )
        self.assertEqual(payload["state"], "completed")
        self.assertEqual(self.slept, [0.25, 0.25])

    def test_poll_stops_at_the_deadline(self):
        ticks = iter([0.0, 1.0, 2.0, 3.0])
        client = self.client(
            FakeReply({"state": "running"}),
            FakeReply({"state": "running"}),
            monotonic=lambda: next(ticks),
        )
        with self.assertRaises(PollTimeout):
            client.poll("/session/s1", lambda item: False, deadline_seconds=1.0)
        self.assertEqual(self.captures(), [])


class RetryTests(ClientCase):
    def test_transient_status_is_retried_exactly_to_the_limit(self):
        client = self.client(http_error(503), http_error(429), FakeReply({"ok": True}))
        self.assertEqual(dict(client.health()), {"ok": True})
        self.assertEqual(client.attempts, 3)
        self.assertEqual(self.slept, [0.01, 0.02])

    def test_transient_failures_exhaust_and_raise(self):
        client = self.client(http_error(503), http_error(503), http_error(503))
        with self.assertRaises(TransientHttpError):
            client.health()
        self.assertEqual(client.attempts, 3)

    def test_ambiguous_server_error_is_not_retried(self):
        client = self.client(http_error(500))
        with self.assertRaises(HttpError) as caught:
            client.health()
        self.assertEqual(caught.exception.status, 500)
        self.assertEqual(client.attempts, 1)

    def test_prompt_is_never_retried(self):
        client = self.client(http_error(503))
        with self.assertRaises(TransientHttpError):
            client.prompt_async("ses_1", {"parts": []})
        self.assertEqual(client.attempts, 1)

    def test_connection_failure_counts_as_transient(self):
        client = self.client(urllib.error.URLError("refused"), FakeReply({"ok": True}))
        self.assertEqual(dict(client.health()), {"ok": True})
        self.assertEqual(client.attempts, 2)


class EndpointTests(ClientCase):
    def test_proven_v2_paths_are_used(self):
        client = self.client(
            FakeReply({"ok": True}),
            FakeReply({"data": {"ses_1": {"state": "busy"}}}),
            FakeReply({"data": {"id": "ses_1"}}),
            FakeReply({"data": {}}),
            FakeReply(b"", status=NO_CONTENT),
            FakeReply([]),
            FakeReply(b"", status=NO_CONTENT),
        )
        client.health()
        client.session_status()
        client.create_session({"agent": "agent-run"})
        client.prompt_async("ses_1", {"parts": []})
        client.abort("ses_1")
        client.permissions("ses_1").release()
        client.answer_permission("ses_1", "perm_1", {"response": "reject"})
        self.assertEqual(
            [(method, url) for method, url, _ in self.opener.calls],
            [
                ("GET", f"http://127.0.0.1:41777{HEALTH_PATH}"),
                ("GET", f"http://127.0.0.1:41777{SESSION_STATUS_PATH}"),
                ("POST", "http://127.0.0.1:41777/api/session"),
                ("POST", "http://127.0.0.1:41777/api/session/ses_1/prompt"),
                ("POST", "http://127.0.0.1:41777/api/session/ses_1/interrupt"),
                ("GET", "http://127.0.0.1:41777/api/session/ses_1/permission"),
                ("POST", "http://127.0.0.1:41777/api/session/ses_1/permission/perm_1/reply"),
            ],
        )
        self.assertEqual(HEALTH_PATH, "/api/health")

    def test_model_roster_uses_the_dedicated_route(self):
        client = self.client(FakeReply({"data": [{"providerID": "omniroute", "id": "x"}], "location": {}}))
        payload = client.models()
        self.assertEqual(dict(payload)["data"], [{"providerID": "omniroute", "id": "x"}])
        method, url, _ = self.opener.calls[0]
        self.assertEqual((method, url), ("GET", f"http://127.0.0.1:41777{MODEL_PATH}"))
        self.assertEqual(MODEL_PATH, "/api/model")

    def test_session_calls_use_json_bodies_and_safe_identifiers(self):
        client = self.client(FakeReply({"data": {"id": "ses_1"}}))
        client.create_session({"agent": "agent-run"})
        method, url, body = self.opener.calls[0]
        self.assertEqual((method, url), ("POST", "http://127.0.0.1:41777/api/session"))
        self.assertEqual(json.loads(body.decode("utf-8")), {"agent": "agent-run"})
        with self.assertRaises(ValidationError):
            client.abort("../other")
        with self.assertRaises(ValidationError):
            client.answer_permission("ses_1", "../escape", {"response": "reject"})

    def test_base_url_must_be_a_private_loopback_endpoint(self):
        for url in ("http://opencode.example:80", "https://127.0.0.1:41777", "http://127.0.0.1"):
            with self.assertRaises(ValidationError):
                OpenCodeHttpClient(url, self.directory)


class EmptyAcknowledgementTests(ClientCase):
    """The beta service may ack interrupt/permission-reply with an empty body
    on either a literal 204 or another successful status; neither is JSON."""

    def test_interrupt_accepts_an_empty_body(self):
        for status in (NO_CONTENT, 200):
            with self.subTest(status=status):
                client = self.client(FakeReply(b"", status=status))
                self.assertEqual(dict(client.abort("ses_1")), {})

    def test_permission_reply_accepts_an_empty_body(self):
        for status in (NO_CONTENT, 200):
            with self.subTest(status=status):
                client = self.client(FakeReply(b"", status=status))
                self.assertEqual(
                    dict(client.answer_permission("ses_1", "perm_1", {"reply": "once"})), {}
                )


if __name__ == "__main__":
    unittest.main()
