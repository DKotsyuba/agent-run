import io
import json
import sys
import tempfile
import unittest
import urllib.error
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.opencode.http import (
    MAX_POLL_INTERVAL_SECONDS,
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

    def __call__(self, request, timeout=None):
        self.calls.append((request.get_method(), request.full_url, request.data))
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


class CaptureTests(ClientCase):
    def test_reply_is_captured_to_a_private_file_before_decoding(self):
        client = self.client(FakeReply({"state": "completed"}))
        response = client.get("/session/s1")
        self.assertTrue(response.body_path.is_file())
        self.assertEqual(response.body_path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(response.body_path.parent, self.directory)
        self.assertEqual(response.mapping()["state"], "completed")
        self.assertEqual(response.raw_ref, str(response.body_path))

    def test_large_reply_is_refused_instead_of_truncated(self):
        payload = b'{"text": "' + b"x" * 4096 + b'"}'
        client = self.client(FakeReply(payload), max_bytes=1024)
        with self.assertRaises(ValidationError):
            client.get("/session/s1/message")
        self.assertEqual(list(self.directory.iterdir()), [])

    def test_non_object_reply_is_refused(self):
        client = self.client(FakeReply([1, 2]))
        with self.assertRaises(ValidationError):
            client.get("/session/s1").mapping()

    def test_invalid_json_is_refused_from_the_file(self):
        client = self.client(FakeReply(b"{not json"))
        with self.assertRaises(ValidationError):
            client.get("/session/s1").json()


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


class RetryTests(ClientCase):
    def test_transient_status_is_retried_exactly_to_the_limit(self):
        client = self.client(http_error(503), http_error(429), FakeReply({"ok": True}))
        self.assertEqual(client.get("/health").mapping(), {"ok": True})
        self.assertEqual(client.attempts, 3)
        self.assertEqual(self.slept, [0.01, 0.02])

    def test_transient_failures_exhaust_and_raise(self):
        client = self.client(http_error(503), http_error(503), http_error(503))
        with self.assertRaises(TransientHttpError):
            client.get("/health")
        self.assertEqual(client.attempts, 3)

    def test_ambiguous_server_error_is_not_retried(self):
        client = self.client(http_error(500))
        with self.assertRaises(HttpError) as caught:
            client.get("/health")
        self.assertEqual(caught.exception.status, 500)
        self.assertEqual(client.attempts, 1)

    def test_prompt_is_never_retried(self):
        client = self.client(http_error(503))
        with self.assertRaises(TransientHttpError):
            client.prompt("s1", {"parts": []})
        self.assertEqual(client.attempts, 1)

    def test_connection_failure_counts_as_transient(self):
        client = self.client(urllib.error.URLError("refused"), FakeReply({"ok": True}))
        self.assertEqual(client.get("/health").mapping(), {"ok": True})
        self.assertEqual(client.attempts, 2)


class EndpointTests(ClientCase):
    def test_session_calls_use_json_bodies_and_safe_identifiers(self):
        client = self.client(FakeReply({"id": "s1"}), FakeReply({"ok": True}))
        client.create_session({"agent": "agent-run"})
        client.interrupt("s1")
        method, url, body = self.opener.calls[0]
        self.assertEqual((method, url), ("POST", "http://127.0.0.1:41777/session"))
        self.assertEqual(json.loads(body.decode("utf-8")), {"agent": "agent-run"})
        self.assertEqual(self.opener.calls[1][1], "http://127.0.0.1:41777/session/s1/abort")
        with self.assertRaises(ValidationError):
            client.interrupt("../other")

    def test_base_url_must_be_a_private_loopback_endpoint(self):
        for url in ("http://opencode.example:80", "https://127.0.0.1:41777", "http://127.0.0.1"):
            with self.assertRaises(ValidationError):
                OpenCodeHttpClient(url, self.directory)


if __name__ == "__main__":
    unittest.main()
