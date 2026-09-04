import json
from io import StringIO

from agent_run.dispatch import TOOL_NAMES, TOOLS, Session, call_tool
from agent_run.domain import StartRequest
from agent_run.delivery.completion_notice_contract import completion_notice_contract_text
from agent_run.doc import topic_text
from agent_run.mcp import serve


class _Broker:
    def __init__(self, service):
        self.service = service
        self.session = Session()

    def call(self, method, params=None, timeout=600):
        return call_tool(self.service, method, params or {}, self.session)


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


def test_start_description_includes_completion_contract_text() -> None:
    """Start discovery includes the exact doc contract and all four format fields."""
    start_tool = next(tool for tool in TOOLS if tool["name"] == "start")
    expected = completion_notice_contract_text()
    self_desc = start_tool["description"]
    assert self_desc.startswith("Start one asynchronous durable agent.")
    assert expected in self_desc
    assert expected == topic_text("completion")
    for label in ("- ID:", "- Status:", "- Runtime/model:", "- Notice:"):
        assert label in self_desc


def test_tools_table_and_mcp_tools_list_are_exactly_pinned() -> None:
    """Keep all nineteen shared tools identical through MCP discovery."""

    assert len(TOOLS) == 19
    assert TOOL_NAMES == frozenset(tool["name"] for tool in TOOLS)

    output = StringIO()
    assert serve(
        _Broker(_Service()),
        StringIO(json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/list"}) + "\n"),
        output,
    ) == 0
    response = json.loads(output.getvalue())
    assert response["result"]["tools"] == list(TOOLS)
