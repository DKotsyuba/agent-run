import json
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from agent_run.dispatch import TOOL_NAMES, TOOLS, Session, call_tool
from agent_run.domain import StartRequest
from agent_run.delivery.completion_notice_contract import completion_notice_contract_text
from agent_run.doc import topic_text
from agent_run.mcp import serve
from agent_run.errors import ValidationError
from agent_run.effective_policy import Constraint
from agent_run.config import RuntimeConfig
from agent_run.service import AgentQuery, AgentService


class _Broker:
    def __init__(self, service):
        self.service = service
        self.session = Session()

    def call(self, method, params=None, timeout=600):
        return call_tool(self.service, method, params or {}, self.session)


class _Service:
    accounts = {"default": "personal", "personal": "personal", "work": "work"}

    def resolve_account(self, runtime, account):
        """Resolve a str/None test account for the runtime str, or reject it."""
        if account is not None and account not in self.accounts and account != "personal2":
            raise ValidationError(f"unknown account: {account}")
        return account or self.accounts["default"]

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
        Session(),
    )
    assert isinstance(result, StartRequest)
    assert result.account == "personal2"


def test_list_agents_accepts_revision_long_poll_fields() -> None:
    """Translate factual list cursor/wait inputs into the typed service query."""

    result = call_tool(
        _Service(),
        "list_agents",
        {"after_revision": 12, "wait_seconds": 1.5, "limit": 7},
        Session(),
    )
    assert isinstance(result, AgentQuery)
    assert (result.after_revision, result.wait_seconds, result.limit) == (12, 1.5, 7)


def test_start_accepts_only_unique_known_policy_requirements() -> None:
    """Dispatch converts explicit names to typed immutable constraints."""

    service = _Service()
    result = _start(
        service,
        Session(),
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
            _start(service, Session(), required_constraints=invalid)
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


def _start(service, session, **values):
    """Return a StartRequest using the stub, Session and argument overrides."""
    args = {"runtime": "codex", "model": "m", "profile": "p", "task": "t", "workdir": "/tmp"}
    args.update(values)
    return call_tool(service, "start", args, session)


def test_fast_account_overrides_are_isolated_and_false_is_meaningful() -> None:
    """An account's false override leaves other accounts on the true default."""
    service, session = _Service(), Session()
    call_tool(service, "fast", {"runtime": "codex", "enabled": True}, session)
    call_tool(service, "fast", {"runtime": "codex", "account": "work", "enabled": False}, session)
    assert _start(service, session, account="personal").fast is True
    assert _start(service, session, account="work").fast is False
    assert call_tool(service, "fast", {}, session) == {"codex": True, "accounts": {"work": False}}


def test_start_explicit_fast_wins_and_default_account_is_resolved() -> None:
    """Omitted accounts resolve first, while an explicit start bool wins."""
    service, session = _Service(), Session()
    call_tool(service, "fast", {"runtime": "codex", "account": "personal", "enabled": True}, session)
    assert _start(service, session).fast is True
    assert _start(service, session, fast=True).fast is True
    assert _start(service, session, fast=False).fast is False


def test_fast_rejects_unknown_account_and_legacy_query_is_copy() -> None:
    """Unknown account input fails while legacy queries remain defensive copies."""
    service, session = _Service(), Session()
    original = call_tool(service, "fast", {}, session)
    original["codex"] = True
    assert call_tool(service, "fast", {}, session) == {"codex": False}
    try:
        call_tool(service, "fast", {"runtime": "codex", "account": "nope", "enabled": True}, session)
    except ValidationError:
        pass
    else:
        raise AssertionError("unknown account accepted")


def test_agent_service_resolve_account_validates_defaults_and_overrides() -> None:
    """The production resolver validates configured defaults and account sets."""
    service = object.__new__(AgentService)
    valid = RuntimeConfig(True, "adapter", Path("/bin/echo"), Path("/tmp"), ("m",), accounts=("personal", "work"), default_account="personal")
    with patch.object(service, "_runtime_config", return_value=valid):
        assert service.resolve_account("codex", None) == "personal"
        assert service.resolve_account("codex", "work") == "work"
    with patch.object(service, "_runtime_config", return_value=valid.__class__(**{**valid.__dict__, "default_account": "missing"})):
        try:
            service.resolve_account("codex", None)
        except ValidationError:
            pass
        else:
            raise AssertionError("invalid default accepted")
    empty = RuntimeConfig(True, "adapter", Path("/bin/echo"), Path("/tmp"), ("m",))
    with patch.object(service, "_runtime_config", return_value=empty):
        try:
            service.resolve_account("codex", "work")
        except ValidationError:
            pass
        else:
            raise AssertionError("account accepted for runtime without accounts")


def test_tools_table_is_exactly_pinned() -> None:
    """Keep all sixteen transport-neutral tools in the one dispatch table."""

    assert len(TOOLS) == 16
    assert TOOL_NAMES == frozenset(tool["name"] for tool in TOOLS)
