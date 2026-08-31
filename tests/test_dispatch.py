import json
from io import StringIO

from agent_run.dispatch import TOOL_NAMES, TOOLS, Session, call_tool
from agent_run.domain import StartRequest
from agent_run.mcp import serve


class _Service:
    def start(self, request):
        self.request = request
        return request


def test_start_accepts_account() -> None:
    service = _Service()
    result = call_tool(
        service,
        "start",
        {"runtime": "fake", "model": "m", "profile": "p", "task": "t", "workdir": "/tmp", "account": "personal2"},
        Session(),
    )
    assert isinstance(result, StartRequest)
    assert result.account == "personal2"


def test_tools_table_and_mcp_tools_list_are_exactly_pinned() -> None:
    assert len(TOOLS) == 17
    assert TOOL_NAMES == frozenset(tool["name"] for tool in TOOLS)

    output = StringIO()
    assert serve(
        _Service(),
        StringIO(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}) + "\n"),
        output,
    ) == 0
    response = json.loads(output.getvalue())
    assert response["result"]["tools"] == list(TOOLS)
