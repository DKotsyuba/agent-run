import json
from io import StringIO

from agent_run.dispatch import TOOL_NAMES, TOOLS
from agent_run.mcp import serve


class _Service:
    pass


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
