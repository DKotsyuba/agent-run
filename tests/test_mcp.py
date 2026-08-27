from __future__ import annotations

import json
import tempfile
import unittest
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

from agent_run.domain import AgentStatus, StartRequest
from agent_run.errors import ValidationError
from agent_run.mcp import MAX_LINE_BYTES, serve


@dataclass(frozen=True)
class SerializedResult:
    status: AgentStatus
    path: Path


class StubService:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def start(self, request):
        self.calls.append(("start", request))
        return {"agent_id": "ag-started", "created": True}

    def cancel(self, agent_id):
        self.calls.append(("cancel", agent_id))
        return {"agent_id": agent_id, "status": AgentStatus.CANCELLING}

    def steer(self, agent_id, text):
        self.calls.append(("steer", (agent_id, text)))
        return {"agent_id": agent_id, "kind": "steer", "state": "pending"}

    def get(self, agent_id):
        self.calls.append(("status", agent_id))
        if agent_id == "agent-error":
            raise ValidationError("typed failure")
        if agent_id == "internal-error":
            raise RuntimeError("credential=must-not-leak")
        if agent_id == "large":
            return {"blob": "x" * MAX_LINE_BYTES}
        return SerializedResult(AgentStatus.RUNNING, Path("/safe/path"))

    def list(self, query):
        self.calls.append(("list_agents", query))
        return {"items": [], "total": 7, "next_offset": None, "complete": True}

    def summary(self, **kwargs):
        self.calls.append(("summary", kwargs))
        return {"scope": "agent", "total": 1, "complete": True}

    def transcript(self, agent_id, cursor=0, limit=200):
        self.calls.append(("transcript", (agent_id, cursor, limit)))
        return {
            "agent_id": agent_id,
            "messages": [{"content": "x", "raw_ref": "raw/x.json"}],
            "cursor": cursor,
            "next_cursor": cursor + 1,
            "complete": False,
        }

    def answer(self, agent_id):
        self.calls.append(("answer", agent_id))
        return {
            "agent_id": agent_id,
            "available": True,
            "content": None,
            "inline_complete": False,
            "path": Path("/verified/answer.md"),
        }

    def models(self):
        self.calls.append(("models", None))
        return {"fake": [{"id": "model"}]}

    def limits(self):
        self.calls.append(("limits", None))
        return {"items": [{"risk": "unknown"}]}


class McpTests(unittest.TestCase):
    def run_server(self, lines, service=None):
        source = StringIO("".join(
            line if isinstance(line, str) else json.dumps(line) + "\n"
            for line in lines
        ))
        output = StringIO()
        selected = service or StubService()
        self.assertEqual(serve(selected, source, output), 0)
        responses = [json.loads(line) for line in output.getvalue().splitlines()]
        return selected, responses

    def test_initialize_notification_and_exact_tool_roster(self) -> None:
        _, responses = self.run_server(
            [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {"protocolVersion": "test-version"},
                },
                {
                    "jsonrpc": "2.0",
                    "method": "notifications/initialized",
                    "params": {},
                },
                {"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            ]
        )
        self.assertEqual(len(responses), 2)
        self.assertEqual(responses[0]["result"]["protocolVersion"], "test-version")
        self.assertEqual(
            [tool["name"] for tool in responses[1]["result"]["tools"]],
            [
                "start", "cancel", "steer", "status", "list_agents",
                "summary", "transcript", "answer", "models", "limits", "doc",
                "workflow_start", "workflow_status", "workflow_cancel", "workflow_answer",
            ],
        )
        self.assertEqual(responses[0]["result"]["capabilities"], {"tools": {"listChanged": False}})

    def test_tools_list_ignores_pagination_and_other_params(self) -> None:
        _, responses = self.run_server(
            [
                {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {"cursor": None}},
                {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
            ]
        )
        expected = [
            "start", "cancel", "steer", "status", "list_agents",
            "summary", "transcript", "answer", "models", "limits", "doc",
            "workflow_start", "workflow_status", "workflow_cancel", "workflow_answer",
        ]
        for response in responses:
            self.assertEqual([tool["name"] for tool in response["result"]["tools"]], expected)
            self.assertNotIn("nextCursor", response["result"])

    def test_tools_call_tolerates_meta_but_still_enforces_name_and_arguments(self) -> None:
        service, responses = self.run_server(
            [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {
                        "name": "limits",
                        "arguments": {},
                        "_meta": {"progressToken": "abc"},
                    },
                },
                {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"arguments": {}}},
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {"name": "limits", "arguments": "not-a-dict"},
                },
            ]
        )
        self.assertFalse(responses[0]["result"]["isError"])
        self.assertEqual(service.calls[-1], ("limits", None))
        self.assertEqual(responses[1]["error"]["code"], -32602)
        self.assertEqual(responses[2]["error"]["code"], -32602)

    def test_all_tools_decode_to_one_service_and_serialize_safely(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            workdir = Path(directory).resolve()
            calls = [
                ("start", {"runtime": "fake", "model": "model", "profile": "p", "task": "t", "workdir": str(workdir)}),
                ("cancel", {"agent_id": "ag-cancel"}),
                ("steer", {"agent_id": "ag-steer", "text": "finish"}),
                ("status", {"agent_id": "ag-status"}),
                ("list_agents", {"active": True, "limit": 2}),
                ("summary", {"agent_id": "ag-summary"}),
                ("transcript", {"agent_id": "ag-transcript", "cursor": 4, "limit": 2}),
                ("answer", {"agent_id": "ag-answer"}),
                ("models", {}),
                ("limits", {}),
            ]
            requests = [
                {"jsonrpc": "2.0", "id": index, "method": "tools/call", "params": {"name": name, "arguments": arguments}}
                for index, (name, arguments) in enumerate(calls, 1)
            ]
            service, responses = self.run_server(requests)

        self.assertEqual([name for name, _ in service.calls], [name for name, _ in calls])
        self.assertIsInstance(service.calls[0][1], StartRequest)
        self.assertTrue(all(not response["result"]["isError"] for response in responses))
        status = responses[3]["result"]["structuredContent"]
        self.assertEqual(status, {"status": "running", "path": "/safe/path"})
        transcript = responses[6]["result"]["structuredContent"]
        self.assertEqual((transcript["cursor"], transcript["next_cursor"], transcript["complete"]), (4, 5, False))
        self.assertEqual(transcript["messages"][0]["raw_ref"], "raw/x.json")
        answer = responses[7]["result"]["structuredContent"]
        self.assertIsNone(answer["content"])
        self.assertFalse(answer["inline_complete"])

    def test_start_preserves_omitted_and_explicit_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            arguments = {
                "runtime": "fake",
                "model": "model",
                "profile": "p",
                "task": "t",
                "workdir": directory,
            }
            service, _ = self.run_server(
                [
                    {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "start", "arguments": arguments}},
                    {"jsonrpc": "2.0", "id": 2, "method": "tools/call", "params": {"name": "start", "arguments": {**arguments, "timeout_seconds": 480}}},
                ]
            )
        starts = [value for name, value in service.calls if name == "start"]
        self.assertIsNone(starts[0].timeout_seconds)
        self.assertEqual(starts[1].timeout_seconds, 480)

    def test_protocol_and_tool_errors_are_bounded_and_loop_continues(self) -> None:
        service, responses = self.run_server(
            [
                "{bad json\n",
                {"jsonrpc": "2.0", "id": 2, "method": "unknown"},
                {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": []},
                {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "unknown", "arguments": {}}},
                {"jsonrpc": "2.0", "id": 5, "method": "tools/call", "params": {"name": "status", "arguments": {"agent_id": "agent-error"}}},
                {"jsonrpc": "2.0", "id": 6, "method": "tools/call", "params": {"name": "status", "arguments": {"agent_id": "internal-error"}}},
                {"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": {"name": "status", "arguments": {"agent_id": "ok"}}},
            ]
        )
        self.assertEqual([response.get("error", {}).get("code") for response in responses[:3]], [-32700, -32601, -32602])
        self.assertTrue(responses[3]["result"]["isError"])
        self.assertEqual(responses[4]["result"]["structuredContent"]["error"]["code"], "ValidationError")
        internal = responses[5]["result"]["structuredContent"]["error"]
        self.assertEqual(internal["code"], "internal_error")
        self.assertNotIn("credential", internal["message"])
        self.assertFalse(responses[6]["result"]["isError"])
        self.assertEqual(service.calls[-1], ("status", "ok"))

    def test_notifications_and_oversized_blobs_never_break_json_stdout(self) -> None:
        service = StubService()
        oversized = "x" * (MAX_LINE_BYTES + 1) + "\n"
        _, responses = self.run_server(
            [
                {"jsonrpc": "2.0", "method": "tools/call", "params": {"name": "status", "arguments": {"agent_id": "ignored"}}},
                oversized,
                {"jsonrpc": "2.0", "id": 3, "method": "tools/call", "params": {"name": "status", "arguments": {"agent_id": "large"}}},
                {"jsonrpc": "2.0", "id": 4, "method": "tools/call", "params": {"name": "status", "arguments": {"agent_id": "ok"}}},
            ],
            service,
        )
        self.assertEqual(len(responses), 3)
        self.assertEqual(responses[0]["error"]["code"], -32700)
        self.assertEqual(responses[1]["error"]["code"], -32603)
        self.assertFalse(responses[2]["result"]["isError"])
        self.assertNotIn(("status", "ignored"), service.calls)


if __name__ == "__main__":
    unittest.main()
