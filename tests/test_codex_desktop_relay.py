"""Exercise the real Node wrapper and Python client without contacting Desktop."""
import json
import os
from pathlib import Path
import shutil
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import MagicMock, patch

from agent_run import cli
from agent_run.delivery.base import CompletionNotice, AmbiguousDeliveryError
from agent_run.delivery.codex_desktop_relay import CodexDesktopRelayClient, _frame, _read_frame
from agent_run.domain import AgentId, AgentStatus, OrchestratorRef
from agent_run.errors import ValidationError

NODE = shutil.which("node")
WRAPPER = Path(__file__).parents[1] / "src/agent_run/delivery/codex_desktop_host.cjs"
AGENT = AgentId("ag-20260825-120000-0123456789")
NOTICE = CompletionNotice("ntf_test", AGENT, AgentStatus.SUCCEEDED)
TARGET = OrchestratorRef("codex_queue", "thread-test")


from agent_run.delivery.base import DeliveryError


class RelayClientTests(unittest.TestCase):
    """Local failure classifications preserve the existing at-least-once contract."""

    def test_missing_relay_is_retryable(self):
        """An empty endpoint directory records unavailability, not success."""
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            client = CodexDesktopRelayClient(Path(directory))
            with self.assertRaises(DeliveryError) as caught:
                client.send(TARGET, NOTICE)
            self.assertNotIsInstance(caught.exception, AmbiguousDeliveryError)
            assert caught.exception.evidence is not None
            self.assertEqual(caught.exception.evidence.classifier, "relay_unavailable")

    def test_partial_send_is_ambiguous_and_stops_discovery(self):
        """Even BrokenPipe during sendall cannot permit another relay or queue."""
        client = CodexDesktopRelayClient(Path("/tmp"))
        connection = MagicMock()
        connection.__enter__.return_value = connection
        connection.sendall.side_effect = BrokenPipeError("partial")
        with patch.object(client, "_paths", return_value=(Path("/tmp/a"), Path("/tmp/b"))), patch(
            "agent_run.delivery.codex_desktop_relay.socket.socket", return_value=connection
        ):
            with self.assertRaises(AmbiguousDeliveryError):
                client.send(TARGET, NOTICE)
        connection.connect.assert_called_once()
        assert client.last_evidence is not None
        self.assertEqual(client.last_evidence.classifier, "relay_ambiguous")
        self.assertNotIn("thread-test", json.dumps(client.last_evidence.payload()))

    def test_frame_bound_is_enforced(self):
        """Oversized local requests fail before any socket transmission."""
        with self.assertRaises(ValidationError):
            _frame({"text": "x" * 8192})


@unittest.skipUnless(NODE, "optional Desktop Node host is unavailable")
class NodeWrapperTests(unittest.TestCase):
    """Real Node UDS roundtrips preserve exact notice text, stdio, and cleanup."""

    def run_delivery(self, mode: str) -> bool | None:
        """Run a fake host exchange for str mode and verify child/socket cleanup.

        Returns bool True on acceptance or None after asserting the expected
        typed error. Slow mode delays tools/list by two seconds. Unexpected
        exchange errors raise rather than masking a failed regression.
        """
        assert NODE is not None
        with tempfile.TemporaryDirectory(dir="/tmp") as directory:
            home = Path(directory)
            host = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            host.bind(str(home / "host.sock")); host.listen(1); host.settimeout(5)
            errors = []

            def reply(connection, value):
                """Write host-sized JSON, deliberately independent of local 8192 cap."""
                body = json.dumps(value).encode()
                connection.sendall(struct.pack("<I", len(body)) + body)

            def serve_host():
                """Answer one tools/list then tools/call and capture assertion failures."""
                try:
                    connection, _ = host.accept()
                    with connection:
                        connection.settimeout(3)
                        listed = _read_frame(connection)
                        self.assertEqual(listed["id"], 1)
                        if mode == "precall":
                            reply(connection, {"jsonrpc": "2.0", "id": 999, "result": {}})
                            return
                        if mode == "slow":
                            time.sleep(2)
                        reply(connection, {"jsonrpc": "2.0", "id": 1, "result": {"tools": [
                            {"name": "send_message_to_thread", "namespace": "codex_app", "description": "x" * 42000}
                        ]}})
                        called = _read_frame(connection)
                        self.assertEqual(called["id"], 2)
                        self.assertEqual(called["params"]["arguments"], {"threadId": TARGET.external_session_id, "prompt": NOTICE.render()})
                        self.assertEqual(called["params"]["callId"], NOTICE.notification_id)
                        if mode == "drop":
                            return
                        value = {"jsonrpc": "2.0", "id": 2, "result": {"success": mode != "false", "contentItems": []}}
                        if mode == "badid": value["id"] = 999
                        if mode == "error": value = {"jsonrpc": "2.0", "id": 2, "error": {"code": -1}}
                        reply(connection, value)
                except BaseException as error:
                    errors.append(error)
                finally:
                    host.close()

            thread = threading.Thread(target=serve_host, daemon=True)
            thread.start()
            child_code = "import os,sys,json;sys.stdin.buffer.read();print(json.dumps([os.getenv('CODEX_APP_TOOLS_PIPE_PATH'),os.getenv('CODEX_MCP_NODE_PATH')]))"
            env = dict(os.environ, CODEX_APP_TOOLS_PIPE_PATH=str(home / "host.sock"), CODEX_MCP_NODE_PATH=NODE)
            process = subprocess.Popen([NODE, str(WRAPPER), sys.executable, str(home), "-c", child_code],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
            try:
                deadline = time.monotonic() + 5
                while not list(home.glob("ar-cdx-*.sock")):
                    if process.poll() is not None or time.monotonic() >= deadline:
                        self.fail("Node wrapper did not bind relay")
                    time.sleep(0.01)
                endpoint = next(home.glob("ar-cdx-*.sock"))
                self.assertEqual(endpoint.stat().st_mode & 0o777, 0o600)
                client = CodexDesktopRelayClient(home)
                if mode in {"false", "precall"}:
                    with self.assertRaises(DeliveryError) as caught:
                        client.send(TARGET, NOTICE)
                    self.assertNotIsInstance(caught.exception, AmbiguousDeliveryError)
                    assert caught.exception.evidence is not None
                    self.assertEqual(caught.exception.evidence.classifier, "relay_rejected")
                    accepted = None
                elif mode in {"drop", "badid", "error"}:
                    with self.assertRaises(AmbiguousDeliveryError): client.send(TARGET, NOTICE)
                    accepted = None
                else:
                    accepted = client.send(TARGET, NOTICE)
            finally:
                out, err = process.communicate(b"", timeout=5)
                thread.join(timeout=5)
            self.assertEqual(process.returncode, 0, err.decode())
            self.assertEqual(json.loads(out), [None, None])
            self.assertFalse(list(home.glob("ar-cdx-*.sock")))
            if errors: raise errors[0]
            return accepted

    def test_large_inventory_and_exact_notice_are_accepted(self):
        """A real >8KiB host inventory passes the separate host frame ceiling."""
        self.assertTrue(self.run_delivery("true"))

    def test_slow_discovery_exceeds_the_former_deadline_and_succeeds(self):
        """A two-second tools/list response must not lose the completion."""
        self.assertTrue(self.run_delivery("slow"))

    def test_explicit_rejection_and_precall_failure_are_retryable(self):
        """Known non-delivery raises with evidence instead of using the UI queue."""
        for mode in ("false", "precall"):
            with self.subTest(mode=mode): self.run_delivery(mode)

    def test_postcall_uncertainty_never_allows_fallback(self):
        """EOF, id mismatch, and error after dispatch all remain ambiguous."""
        for mode in ("drop", "badid", "error"):
            with self.subTest(mode=mode): self.run_delivery(mode)


class ExecWrapperTests(unittest.TestCase):
    """Only a real configured MCP CLI replaces itself with the host Node wrapper."""

    def test_missing_capability_does_not_exec(self):
        """Ordinary CLI/MCP operation remains dependency-free without host env."""
        with patch.dict(os.environ, {}, clear=True), patch.object(os, "execv") as execute:
            cli._exec_desktop_relay(Path("/tmp"))
        execute.assert_not_called()

    def test_exec_uses_exact_node_and_python_child(self):
        """No shell or inherited arbitrary argv is used for the wrapper process."""
        with patch.dict(os.environ, {"CODEX_MCP_NODE_PATH": sys.executable, "CODEX_APP_TOOLS_PIPE_PATH": "/tmp/host.sock"}, clear=True), patch.object(os, "execv") as execute:
            cli._exec_desktop_relay(Path("/tmp/home"))
        node, args = execute.call_args.args
        self.assertEqual(node, sys.executable)
        self.assertTrue(args[1].endswith("codex_desktop_host.cjs"))
        self.assertEqual(args[2:], [sys.executable, "/tmp/home", "-m", "agent_run.cli", "--home", "/tmp/home", "mcp"])
