from agent_run.dispatch import TOOL_NAMES, TOOLS, call_tool
from agent_run.domain import StartRequest
from agent_run.delivery.completion_notice_contract import completion_notice_contract_text
from agent_run.doc import topic_text
from agent_run.errors import ValidationError
from agent_run.effective_policy import Constraint
from agent_run.service import AgentQuery
from unittest.mock import Mock


class _Service:
    def start(self, request):
        self.request = request
        return request

    def list(self, query):
        """Return the factual-list query for dispatch assertions."""

        return query


def test_start_accepts_account() -> None:
    service = _Service()
    result = call_tool(
        service,
        "start",
        {"runtime": "fake", "model": "m", "profile": "p", "task": "t", "workdir": "/tmp", "account": "personal2"},
    )
    assert isinstance(result, StartRequest)
    assert result.account == "personal2"


def test_list_agents_accepts_revision_long_poll_fields() -> None:
    """Translate factual list cursor/wait inputs into the typed service query."""

    result = call_tool(
        _Service(),
        "list_agents",
        {"after_revision": 12, "wait_seconds": 1.5, "limit": 7},
    )
    assert isinstance(result, AgentQuery)
    assert (result.after_revision, result.wait_seconds, result.limit) == (12, 1.5, 7)


def test_start_accepts_only_unique_known_policy_requirements() -> None:
    """Dispatch converts explicit names to typed immutable constraints."""

    service = _Service()
    result = _start(
        service,
        required_constraints=["external_network_isolation"],
    )
    assert result.required_constraints == frozenset(
        {Constraint.EXTERNAL_NETWORK_ISOLATION}
    )
    for invalid in (
        ["unknown"],
        ["external_network_isolation", "external_network_isolation"],
        "external_network_isolation",
    ):
        try:
            _start(service, required_constraints=invalid)
        except ValidationError:
            pass
        else:
            raise AssertionError("invalid required_constraints accepted")


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


def _start(service, **values):
    """Return a StartRequest using the stub and argument overrides."""
    args = {"runtime": "codex", "model": "m", "profile": "p", "task": "t", "workdir": "/tmp"}
    args.update(values)
    return call_tool(service, "start", args)




def test_tools_table_is_exactly_pinned() -> None:
    """Keep exactly the eleven canonical tools in the shared dispatch table."""

    assert TOOL_NAMES == {
        "start", "resume", "cancel", "steer", "list_agents", "answer",
        "transcript", "capacity_order", "doc", "models", "limits",
    }
    assert TOOL_NAMES == frozenset(tool["name"] for tool in TOOLS)


def test_removed_tools_are_rejected() -> None:
    """Reject every retired public method before service dispatch."""

    for name in {
        "fast", "status", "list_orchestrators", "summary",
        "chain",
    }:
        try:
            call_tool(_Service(), name, {})
        except ValidationError:
            pass
        else:
            raise AssertionError(f"retired tool accepted: {name}")


def test_retained_agent_tools_dispatch() -> None:
    """Dispatch the four direct agent operations with validated arguments."""

    service = Mock()
    call_tool(service, "cancel", {"agent_id": "agent"})
    call_tool(service, "steer", {"agent_id": "agent", "text": "continue"})
    call_tool(service, "answer", {"agent_id": "agent"})
    call_tool(service, "transcript", {"agent_id": "agent", "cursor": 2, "limit": 3})
    service.cancel.assert_called_once_with("agent")
    service.steer.assert_called_once_with("agent", "continue")
    service.answer.assert_called_once_with("agent")
    service.transcript.assert_called_once_with("agent", cursor=2, limit=3)


def test_restored_operator_tools_dispatch() -> None:
    """Dispatch current model and capacity reads through the shared table."""

    service = Mock()
    call_tool(service, "models", {})
    call_tool(service, "limits", {})
    service.models.assert_called_once_with()
    service.limits.assert_called_once_with()
