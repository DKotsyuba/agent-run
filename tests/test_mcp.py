import json
import tempfile
import unittest
from io import StringIO
from pathlib import Path

from agent_run.broker_client import BrokerClient
from agent_run.dispatch import TOOLS
from agent_run.errors import AgentRunError, BrokerUnavailable, ValidationError
from agent_run.mcp import MAX_LINE_BYTES, serve


class FakeBroker:
    def __init__(self, result=None, error=None) -> None:
        self.calls = []
        self.result = result if result is not None else {"ok": True}
        self.error = error

    def call(self, method, params=None, timeout=600):
        self.calls.append((method, params))
        if self.error is not None:
            raise self.error
        return self.result


class McpTests(unittest.TestCase):
    def run_server(self, lines, broker=None):
        source = StringIO("".join(
            line if isinstance(line, str) else json.dumps(line) + "\n"
            for line in lines
        ))
        output = StringIO()
        selected = broker or FakeBroker()
        self.assertEqual(serve(selected, source, output), 0)
        responses = [json.loads(line) for line in output.getvalue().splitlines()]
        return selected, responses

    def test_initialize_and_tools_list_are_local_without_broker(self) -> None:
        _, responses = self.run_server(
            [
                {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {"protocolVersion": "test-version"}},
                {"jsonrpc": "2.0", "method": "notifications/initialized", "params": {}},
                {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            ],
            broker=None,
        )
        self.assertEqual(len(responses), 2)
        self.assertEqual(responses[0]["result"]["protocolVersion"], "test-version")
        self.assertEqual(responses[0]["result"]["capabilities"], {"tools": {"listChanged": False}})
        self.assertEqual(responses[1]["result"]["tools"], list(TOOLS))

    def test_tools_call_forwards_name_and_arguments_verbatim(self) -> None:
        broker = FakeBroker({"answer": 42})
        arguments = {"agent_id": "ag-1", "nested": {"items": [1, True]}}
        _, responses = self.run_server([
            {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "limits", "arguments": arguments, "_meta": {"x": 1}}},
        ], broker)
        self.assertEqual(broker.calls, [("limits", arguments)])
        self.assertEqual(responses[0]["result"], {
            "content": [{"type": "text", "text": "result in structuredContent"}],
            "structuredContent": {"answer": 42},
            "isError": False,
        })

    def test_broker_validation_error_uses_mcp_error_envelope(self) -> None:
        _, responses = self.run_server(
            [{"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "status", "arguments": {}}}],
            FakeBroker(error=ValidationError("missing arguments: ['agent_id']")),
        )
        result = responses[0]["result"]
        self.assertTrue(result["isError"])
        self.assertEqual(result["structuredContent"]["error"], {
            "code": "ValidationError", "message": "missing arguments: ['agent_id']"
        })

    def test_broker_down_is_tool_error_but_tools_list_still_works(self) -> None:
        broker = FakeBroker(error=BrokerUnavailable(
            "agent-run broker is not running; start it with `agent-run api serve` or its launchd job (agent-run doc service)"
        ))
        _, responses = self.run_server([
            {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "limits", "arguments": {}}},
            {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
        ], broker)
        self.assertTrue(responses[0]["result"]["isError"])
        self.assertIn("agent-run broker is not running", responses[0]["result"]["structuredContent"]["error"]["message"])
        self.assertFalse("error" in responses[1])
        self.assertEqual(responses[1]["result"]["tools"], list(TOOLS))

    def test_unknown_tool_is_rejected_before_broker_call(self) -> None:
        broker = FakeBroker()
        _, responses = self.run_server([
            {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "not-a-tool", "arguments": {}}},
        ], broker)
        self.assertEqual(broker.calls, [])
        self.assertEqual(responses[0]["result"]["structuredContent"]["error"]["code"], "unknown_tool")

    def test_protocol_errors_and_size_limit_remain_bounded(self) -> None:
        oversized = "x" * (MAX_LINE_BYTES + 1) + "\n"
        broker, responses = self.run_server([
            "{bad json\n",
            oversized,
            {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": []},
        ])
        self.assertEqual([response["error"]["code"] for response in responses], [-32700, -32700, -32602])
        self.assertEqual(broker.calls, [])


if __name__ == "__main__":
    unittest.main()
