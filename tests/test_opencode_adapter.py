import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.base import ADAPTER_API_VERSION, Capability
from agent_run.adapters.home import content_hash
from agent_run.adapters.opencode import adapter as opencode_adapter
from agent_run.adapters import omniroute
from agent_run.adapters.omniroute import LIMITS_STALE_SECONDS, pool_samples
from agent_run.adapters.opencode.adapter import (
    ANSWER_NAME,
    CONFIG_RELATIVE_PATH,
    PRIMARY_AGENT,
    RUNTIME_NAME,
    VERIFY_AGENT,
    OpenCodeAdapter,
    OpenCodeRuntimeSession,
    PermissionBroker,
    extract_answer,
    has_reported_error,
    is_settled,
    is_working,
    model_reference,
    normalize_models,
    normalize_outcome,
    normalize_transcript,
    render_config,
    split_model,
)
from agent_run.adapters.opencode.http import HttpError, HttpResponse
from agent_run.adapters.opencode.service import (
    PASSWORD_ENV,
    SERVICE_HOST,
    SERVICE_PATH,
    ServiceIsolationError,
    build_service_plan,
    service_home_paths,
    skills_root_for,
    verify_isolation,
    write_service_descriptor,
)
from agent_run.config import McpConfig, RuntimeConfig, RuntimeHookConfig
from agent_run.domain import AgentStatus, MessageRole, StartRequest
from agent_run.errors import ValidationError
from agent_run.profiles import AgentProfile
from agent_run.state import StateStore

from test_opencode_service import runtime_config


PORT = 41777
MODEL = "omniroute/deepseek-v4-pro"
ALT_MODEL = "omniroute/minimax-m3"


def message(role, text, *, agent=PRIMARY_AGENT, at=1.0, error=None):
    """One flat v2 ``Session.Message.Info`` entry, proven live against beta-18286.

    A user/system message carries ``text`` directly; an assistant message
    carries a ``content`` parts array and, on a failed turn, a structured
    ``error``.
    """

    if role == "assistant":
        entry = {
            "id": "msg_1",
            "type": "assistant",
            "sessionID": "ses_1",
            "agent": agent,
            "model": {"providerID": "omniroute", "id": "deepseek-v4-pro"},
            "time": {"created": at},
            "content": [{"type": "text", "text": text}],
        }
        if error is not None:
            entry["error"] = error
        return entry
    return {
        "id": "msg_1",
        "type": role,
        "sessionID": "ses_1",
        "time": {"created": at},
        "text": text,
    }


def permission(identifier, action, resources, *, session="ses_1", **extra):
    """A v1 ``PermissionV2Request``, shaped exactly like the live captures.

    Verbatim from the pinned v1 1.18.18 canary service, an external read::

        {"id": "per_04062a5b300108nkvKMDl0lPvl",
         "sessionID": "ses_fbf9d98bbffeagK7kbCYE9CGwr",
         "action": "external_directory",
         "resources": ["/private/tmp/.../outside/*"],
         "save": ["/private/tmp/.../outside/*"],
         "source": {"type": "tool", "messageID": "msg_04062a266001Zf4mMiBBDcyYrA",
                    "callID": "call_875c994901054faab986d206"}}

    There is no ``type`` key and no ``path`` key anywhere in it, and the
    directory is stated as its glob.
    """

    item = {
        "id": identifier,
        "sessionID": session,
        "action": action,
        "resources": list(resources),
        "save": list(resources),
        "source": {"type": "tool", "messageID": "msg_1", "callID": "call_1"},
    }
    item.update(extra)
    return item


class PermissionBrokerTests(unittest.TestCase):
    def setUp(self):
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.root = Path(self._temp.name).resolve()
        self.allowed = self.root / "allowed"
        (self.allowed / "nested").mkdir(parents=True)
        (self.root / "other").mkdir()
        self.broker = PermissionBroker((self.allowed,))

    def test_contained_external_directory_is_granted_once(self):
        first = self.broker.decide(
            permission("p1", "external_directory", [f"{self.allowed / 'nested'}/*"])
        )
        self.assertTrue(first.granted)
        self.assertEqual(self.broker.granted_directory, str(self.allowed / "nested"))
        self.assertEqual(self.broker.reply(first), {"reply": "once"})
        second = self.broker.decide(
            permission("p2", "external_directory", [f"{self.allowed}/*"])
        )
        self.assertFalse(second.granted)
        self.assertIn("already granted once", second.reason)

    def test_reply_body_carries_only_the_reply(self):
        decision = self.broker.decide(permission("p1", "bash", ["ls"]))
        self.assertEqual(self.broker.reply(decision), {"reply": "reject"})
        self.assertNotIn("response", self.broker.reply(decision))

    def test_reply_and_blocked_summary_are_plain_json_safe_dicts(self):
        """Both values are handed to json.dumps() with no dict()-unwrapping
        step in between (OpenCodeHttpClient._once() for reply(), and
        EventSink.event() only shallow-copies its top level for
        blocked_summary()); a MappingProxyType blows up json.dumps() with
        "Object of type mappingproxy is not JSON serializable", so neither
        producer may return one, even though a Mapping is duck-type
        compatible."""

        decision = self.broker.decide(permission("p1", "bash", ["ls"]))
        self.assertIs(type(self.broker.reply(decision)), dict)
        self.assertIs(type(self.broker.blocked_summary()), dict)

    def test_directory_outside_read_roots_is_rejected(self):
        decision = self.broker.decide(
            permission("p1", "external_directory", [f"{self.root / 'other'}/*"])
        )
        self.assertFalse(decision.granted)
        self.assertIsNone(self.broker.granted_directory)
        self.assertEqual(self.broker.reply(decision)["reply"], "reject")

    def test_one_resource_outside_the_roots_rejects_the_whole_request(self):
        decision = self.broker.decide(
            permission(
                "p1",
                "external_directory",
                [f"{self.allowed}/*", f"{self.root / 'other'}/*"],
            )
        )
        self.assertFalse(decision.granted)
        self.assertIsNone(self.broker.granted_directory)

    def test_every_other_action_is_auto_rejected_under_its_own_name(self):
        """The blocked summary is keyed by the v1 ``action``. It used to be
        keyed by ``str(permission.get("type"))`` -- a key v1 never sends --
        so every rejection landed under the literal string "None" and the
        durable permissions_blocked event said nothing (proven live:
        ``permissions_blocked {"None": 2}``)."""

        for action in ("bash", "edit", "write", "webfetch", "read"):
            decision = self.broker.decide(permission(f"p-{action}", action, ["/"]))
            self.assertFalse(decision.granted)
        malformed = self.broker.decide({"id": "p-x", "sessionID": "ses_1", "resources": []})
        self.assertFalse(malformed.granted)
        self.assertEqual(
            dict(self.broker.blocked_summary()),
            {"bash": 1, "edit": 1, "read": 1, "unknown": 1, "webfetch": 1, "write": 1},
        )
        self.assertNotIn("None", self.broker.blocked_summary())

    def test_unusable_permission_payloads_are_refused(self):
        with self.assertRaises(ValidationError):
            self.broker.decide(permission(None, "external_directory", [f"{self.allowed}/*"]))
        relative = self.broker.decide(permission("p1", "external_directory", ["allowed/*"]))
        self.assertFalse(relative.granted)
        empty = self.broker.decide(permission("p2", "external_directory", []))
        self.assertFalse(empty.granted)


class TranscriptTests(unittest.TestCase):
    def test_primary_text_is_preserved_across_a_steer_and_a_retry(self):
        payload = [
            message("user", "real task"),
            message("assistant", "part one", at=2.0),
            message("user", "also check the parser", at=3.0),
            message("assistant", "part two", at=4.0),
            message("user", "retrying", at=5.0),
            message("assistant", "part three", at=6.0),
        ]
        self.assertEqual(extract_answer(payload), "part one\n\npart two\n\npart three")

    def test_sub_agent_output_is_excluded_from_the_answer(self):
        payload = {
            "data": [
                message("user", "task"),
                message("assistant", "verifier notes", agent=VERIFY_AGENT, at=2.0),
                message("assistant", "primary answer", at=3.0),
            ]
        }
        self.assertEqual(extract_answer(payload), "primary answer")
        self.assertEqual(extract_answer(payload, agent=VERIFY_AGENT), "verifier notes")

    def test_transcript_keeps_every_agent_and_drops_empty_text(self):
        payload = [
            message("user", "task"),
            {"id": "msg_2", "type": "assistant", "agent": PRIMARY_AGENT, "content": []},
            message("assistant", "answer", agent=VERIFY_AGENT, at=2.0),
        ]
        messages = normalize_transcript(payload, raw_ref="/tmp/reply.json")
        self.assertEqual([item.role for item in messages], [MessageRole.USER, MessageRole.ASSISTANT])
        self.assertEqual(messages[1].name, VERIFY_AGENT)
        self.assertEqual(messages[1].raw_ref, "/tmp/reply.json")
        self.assertEqual(messages[1].at, 2.0)

    def test_role_less_session_events_are_dropped_not_refused(self):
        payload = [
            message("user", "task"),
            {"id": "msg_2", "type": "synthetic", "text": "retrying", "time": {"created": 1.5}},
            message("assistant", "answer", at=2.0),
        ]
        messages = normalize_transcript(payload)
        self.assertEqual([item.content for item in messages], ["task", "answer"])

    def test_unknown_or_malformed_message_shape_is_refused(self):
        with self.assertRaises(ValidationError):
            normalize_transcript([message("tool", "x")])
        with self.assertRaises(ValidationError):
            normalize_transcript([{"type": "assistant", "content": "not-a-list"}])


class OutcomeTests(unittest.TestCase):
    def test_session_outcomes_normalize_to_terminal_statuses(self):
        self.assertEqual(normalize_outcome({"outcome": "succeeded"}).status, AgentStatus.SUCCEEDED)
        self.assertEqual(normalize_outcome({"outcome": "failed"}).status, AgentStatus.FAILED)
        self.assertEqual(normalize_outcome({"outcome": "interrupted"}).status, AgentStatus.CANCELLED)

    def test_failure_detail_comes_from_the_last_assistant_error(self):
        payload = [message("assistant", "partial", error={"type": "ProviderError", "message": "429"})]
        outcome = normalize_outcome(
            {"outcome": "failed"}, payload, runtime_session_id="ses_1"
        )
        self.assertEqual(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "ProviderError")
        self.assertEqual(outcome.runtime_session_id, "ses_1")

    def test_non_terminal_or_malformed_outcome_is_refused(self):
        for info in ({"outcome": "running"}, {}, {"outcome": None}, "not-a-mapping"):
            with self.subTest(info=info):
                with self.assertRaises(ValidationError):
                    normalize_outcome(info)

    def test_active_map_entry_reports_work_not_an_outcome(self):
        self.assertTrue(is_working({"type": "running"}))
        self.assertFalse(is_settled({"type": "running"}))
        self.assertTrue(is_settled({}))
        self.assertFalse(is_working({}))

    def test_v1_shape_infers_success_from_last_assistant_text(self):
        """v1 1.18.18 never reports ``outcome`` at all (proven live, T041):
        an empty session info still normalizes correctly from the message
        list alone once the caller has already decided the turn is settled."""

        # newest-first, matching the real GET .../message ordering.
        payload = [message("assistant", "PONG", at=2.0), message("user", "task", at=1.0)]
        outcome = normalize_outcome({}, payload, runtime_session_id="ses_1")
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertIsNone(outcome.failure_kind)
        self.assertEqual(outcome.runtime_session_id, "ses_1")

    def test_v1_shape_infers_failure_from_a_non_abort_error(self):
        payload = [message(
            "assistant", "", at=2.0,
            error={"name": "ProviderAuthError", "data": {"message": "no api key"}},
        )]
        outcome = normalize_outcome({}, payload)
        self.assertEqual(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "ProviderAuthError")
        self.assertEqual(outcome.failure_text, "no api key")

    def test_v1_shape_infers_cancelled_from_message_aborted_error(self):
        """``MessageAbortedError`` is the one named v1 error (proven live via
        v1's own ``/doc`` OpenAPI) that means the session's own ``/interrupt``
        fired, not that the turn failed."""

        payload = [message(
            "assistant", "", at=2.0,
            error={"name": "MessageAbortedError", "data": {"message": "aborted by user"}},
        )]
        outcome = normalize_outcome({}, payload)
        self.assertEqual(outcome.status, AgentStatus.CANCELLED)
        self.assertEqual(outcome.failure_kind, "MessageAbortedError")
        self.assertEqual(outcome.failure_text, "aborted by user")

    def test_v1_shape_with_no_settled_assistant_message_fails_closed(self):
        # Empty transcript, or the newest entry not a settled assistant turn
        # (here: only a user message) -- genuinely indeterminate either way.
        for payload in ((), [message("user", "task")]):
            with self.subTest(payload=payload):
                with self.assertRaises(ValidationError):
                    normalize_outcome({}, payload)

    # -- pinned to a real pinned v1 1.18.18 `opencode serve` session (T041):
    # session created and prompted with model omniroute/gpt-5.6-luna through
    # an isolated home, captured before any assistant reply landed.
    LIVE_V1_SESSION_INFO = {
        "agent": "agent-run",
        "cost": 0,
        "id": "ses_fc0617c30ffen2sajH7MOh213F",
        "location": {"directory": "/private/var/folders/q0/t041-opencode-v1-lt20shbs"},
        "model": {"id": "gpt-5.6-luna", "providerID": "openai", "variant": "default"},
        "projectID": "global",
        "subpath": "private/var/folders/q0/t041-opencode-v1-lt20shbs",
        "time": {"created": 1787773748181, "updated": 1787773748181},
        "title": "New session - 2026-08-26T19:49:08.181Z",
        "tokens": {"cache": {"read": 0, "write": 0}, "input": 0, "output": 0, "reasoning": 0},
    }
    LIVE_V1_MESSAGES = {
        "cursor": {"next": "eyJpZCI6...", "previous": "eyJpZCI6..."},
        "data": [
            {
                "id": "msg_03f8d9067001DluE7kkEbyO0Wn",
                "text": "Reply with exactly one word: PONG",
                "time": {"created": 1787772637288},
                "type": "user",
            }
        ],
    }

    def test_live_v1_session_info_has_no_outcome_key(self):
        self.assertNotIn("outcome", self.LIVE_V1_SESSION_INFO)

    def test_live_v1_indeterminate_state_still_fails_closed(self):
        """The real session settled with only its user message durably
        recorded (no assistant reply yet): a genuinely indeterminate ending,
        which must still refuse rather than guess "succeeded"."""

        with self.assertRaises(ValidationError):
            normalize_outcome(self.LIVE_V1_SESSION_INFO, self.LIVE_V1_MESSAGES)

    def test_live_v1_indeterminate_state_is_cancelled_when_a_cancel_was_requested(self):
        """The same real capture as above -- settled with only the user
        message, no assistant reply ever landed -- but this time the caller
        (this adapter's own ``cancel()``) already knows it asked the engine
        to interrupt: nothing left to check in the message shape, so this
        must resolve to ``CANCELLED``, not raise."""

        outcome = normalize_outcome(
            self.LIVE_V1_SESSION_INFO, self.LIVE_V1_MESSAGES, cancelled=True
        )
        self.assertEqual(outcome.status, AgentStatus.CANCELLED)

    def test_cancelled_overrides_a_non_abort_error_to_cancelled_not_failed(self):
        """A real v1 error's ``type`` is always ``"unknown"`` on the
        persisted REST message (proven live, T041 -- see
        ``LIVE_V1_ERROR_MESSAGES`` below), never a named union member like
        ``MessageAbortedError``. ``cancelled`` is what actually carries the
        interrupt signal for that shape, not the error's own kind."""

        payload = [message("assistant", "", at=2.0, error={"type": "unknown", "message": "aborted"})]
        outcome = normalize_outcome({}, payload, cancelled=True)
        self.assertEqual(outcome.status, AgentStatus.CANCELLED)
        self.assertEqual(outcome.failure_kind, "unknown")

    def test_cancelled_does_not_override_a_real_success(self):
        """A cancel racing in after the answer already landed must not
        relabel a real success as cancelled."""

        payload = [message("assistant", "done", at=2.0)]
        outcome = normalize_outcome({}, payload, cancelled=True)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)

    # -- a second real pinned v1 1.18.18 session (T041), this one settled
    # with a genuine assistant reply: prompted "Reply with exactly one word:
    # PONG" with model omniroute/gpt-5.6-luna through an isolated home,
    # answered "PONG" and consumed real tokens.
    LIVE_V1_SUCCESS_MESSAGES = {
        "data": [
            {
                "agent": "agent-run",
                "content": [
                    {
                        "id": "msg_0b180982cff69f70016a8f45919cb087d2bb8cf1799b1ab190",
                        "text": "PONG",
                        "type": "text",
                    }
                ],
                "cost": 0,
                "finish": "stop",
                "id": "msg_03fa7c10a0010dQTOb6ds4KzN2",
                "model": {"id": "gpt-5.6-luna", "providerID": "openai", "variant": "default"},
                "time": {"completed": 1787774353892, "created": 1787774353674},
                "tokens": {"cache": {"read": 0, "write": 0}, "input": 1905, "output": 6, "reasoning": 0},
                "type": "assistant",
            },
            {
                "id": "msg_03fa7b815001KzdD3nP86tuLTH",
                "text": "Reply with exactly one word: PONG",
                "time": {"created": 1787774351382},
                "type": "user",
            },
        ]
    }

    def test_live_v1_success_reply_normalizes_and_extracts(self):
        outcome = normalize_outcome({}, self.LIVE_V1_SUCCESS_MESSAGES)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertIsNone(outcome.failure_kind)
        self.assertEqual(extract_answer(self.LIVE_V1_SUCCESS_MESSAGES), "PONG")

    # -- a third real pinned v1 1.18.18 session (T041): a genuine provider
    # 401 (misconfigured key), captured to pin the real persisted-message
    # error shape -- flat ``{"type": "unknown", "message": ...}``, not the
    # named ``{"name": ..., "data": {...}}`` shape the SSE event stream uses.
    LIVE_V1_ERROR_MESSAGES = {
        "data": [
            {
                "agent": "agent-run",
                "content": [],
                "error": {
                    "message": (
                        "Provider request failed with HTTP 401: "
                        '{\n  "error": {\n    "message": "Missing bearer or basic '
                        'authentication in header",\n    "type": "invalid_request_error",'
                        '\n    "param": null,\n    "code": null\n  }\n}'
                    ),
                    "type": "unknown",
                },
                "finish": "error",
                "id": "msg_03faea835001BLHvlsnYD96tJq",
                "model": {"id": "gpt-5.6-luna", "providerID": "openai", "variant": "default"},
                "time": {"completed": 1787774806072, "created": 1787774806069},
                "type": "assistant",
            },
            {
                "id": "msg_03faea3b4001dM1S8K7dvPP6Z4",
                "text": "Reply with exactly one word: PONG",
                "time": {"created": 1787774804917},
                "type": "user",
            },
        ]
    }

    def test_live_v1_error_reply_normalizes_to_failed_with_unknown_kind(self):
        """The real shape's ``type`` is always ``"unknown"`` -- the useful
        detail is in ``message``, not ``kind``."""

        outcome = normalize_outcome({}, self.LIVE_V1_ERROR_MESSAGES)
        self.assertEqual(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "unknown")
        self.assertIn("HTTP 401", outcome.failure_text)

    # -- the exact live capture that broke wait() mid-tool-round (T17B): a
    # review-profile canary, model omniroute/deepseek-v4-pro, called `read`
    # and was interrupted -- captured verbatim from
    # ~/.agent-run/canaries/M011-final/home/agents/ag-20260826-221722-83e24773f8/
    # reply.w2x7f4tq.json. ``/api/session/active`` had already dropped the
    # session (the gap between tool rounds, not a finished turn) when this
    # was fetched, and the newest message is a bare tool-call part with
    # neither text nor a top-level error.
    LIVE_V1_TOOL_ROUND_MESSAGES = {
        "data": [
            {
                "id": "msg_04026722e001LDHgLOu7cacRhf",
                "time": {"created": 1787782656558},
                "type": "assistant",
                "agent": "agent-run",
                "model": {
                    "id": "opencode/deepseek-v4-pro",
                    "providerID": "omniroute",
                    "variant": "default",
                },
                "content": [
                    {
                        "type": "tool",
                        "id": "call_ccae0374dbe54877bdf6cfe3",
                        "name": "read",
                        "provider": {"executed": False},
                        "state": {
                            "status": "error",
                            "input": {"path": "."},
                            "content": [],
                            "structured": {},
                            "error": {
                                "type": "unknown",
                                "message": "Tool execution interrupted",
                            },
                        },
                        "time": {
                            "created": 1787782656561,
                            "ran": 1787782656812,
                            "completed": 1787782656848,
                        },
                    }
                ],
            },
            {
                "id": "msg_040263a2c001EEURmfHUCSzSkw",
                "time": {"created": 1787782642221},
                "text": (
                    "Review the assigned scope independently. Do not edit "
                    "files. Report only actionable findings with concise "
                    "file:line evidence, verification performed, and a "
                    "clear verdict.\n\nReturn exactly CANARY_OK"
                ),
                "type": "user",
            },
        ],
        "cursor": {
            "previous": (
                "eyJpZCI6Im1zZ18wNDAyNjcyMmUwMDFMREhnTE91N2NhY1JoZiIsIm9yZGVyIjoi"
                "ZGVzYyIsImRpcmVjdGlvbiI6InByZXZpb3VzIn0"
            ),
            "next": (
                "eyJpZCI6Im1zZ18wNDAyNjNhMmMwMDFFRVVSbWZIVUNTelNrdyIsIm9yZGVyIjoi"
                "ZGVzYyIsImRpcmVjdGlvbiI6Im5leHQifQ"
            ),
        },
    }

    def test_live_tool_round_gap_has_no_extractable_answer_or_error(self):
        """The exact live capture (T17B): a bare tool-call part is neither
        real text nor a structured error, so both signals wait() checks for
        finality must independently report nothing here."""

        self.assertEqual(extract_answer(self.LIVE_V1_TOOL_ROUND_MESSAGES), "")
        self.assertFalse(has_reported_error(self.LIVE_V1_TOOL_ROUND_MESSAGES))

    def test_live_tool_round_gap_still_fails_closed_if_ever_treated_as_settled(self):
        """Reached directly -- as the old ``wait()`` did via its sticky
        ``working`` flag, crashing supervision with "opencode session outcome
        is not terminal: None" -- this exact payload is correctly refused as
        non-terminal. The bug was ``wait()`` calling ``_finish()`` at all on
        a mid-round gap, not this function's verdict on it."""

        with self.assertRaises(ValidationError):
            normalize_outcome({}, self.LIVE_V1_TOOL_ROUND_MESSAGES)

    def test_message_order_is_newest_first_not_oldest_first(self):
        """Guards the newest-first contract itself (proven live for both
        beta-18286 and v1 1.18.18, see the module docstring): read tail-first
        instead, an old failed turn would incorrectly outrank a later real
        success, and a stale error would incorrectly outrank fresh output."""

        payload = {
            "data": [
                message("assistant", "final answer", at=3.0),
                message("user", "retry", at=2.0),
                message(
                    "assistant", "", at=1.0,
                    error={"type": "ProviderError", "message": "429"},
                ),
            ]
        }
        outcome = normalize_outcome({}, payload)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(extract_answer(payload), "final answer")
        self.assertFalse(has_reported_error(payload))


class ModelTests(unittest.TestCase):
    def test_canonical_identifiers_split_into_provider_and_model(self):
        self.assertEqual(split_model(MODEL), ("omniroute", "deepseek-v4-pro"))
        self.assertEqual(
            dict(model_reference(MODEL)), {"providerID": "omniroute", "id": "deepseek-v4-pro"}
        )

    def test_non_canonical_identifiers_are_refused(self):
        for value in ("minimax-m3", "a/b/c", "/model", "provider/", 7, None):
            with self.assertRaises(ValidationError):
                split_model(value)

    def test_roster_is_intersected_with_the_allowlist_in_config_order(self):
        payload = {
            "data": [
                {"providerID": "opencode", "id": "grok-4-fast", "enabled": True, "status": "active"},
                {"providerID": "omniroute", "id": "minimax-m3", "enabled": True, "status": "active", "name": "MiniMax M3"},
                {"providerID": "omniroute", "id": "deepseek-v4-pro", "enabled": True, "status": "active"},
                {"providerID": "omniroute", "id": "unlisted", "enabled": True, "status": "active"},
            ],
            "location": {},
        }
        models = normalize_models(payload, (ALT_MODEL, MODEL, "omniroute/absent"))
        self.assertEqual([item.id for item in models], [ALT_MODEL, MODEL])
        self.assertEqual(models[0].description, "MiniMax M3")

    def test_disabled_or_inactive_entries_are_not_reported(self):
        payload = {
            "data": [
                {"providerID": "omniroute", "id": "deepseek-v4-pro", "enabled": False, "status": "active"},
                {"providerID": "omniroute", "id": "minimax-m3", "enabled": True, "status": "disabled"},
            ]
        }
        self.assertEqual(normalize_models(payload, (MODEL, ALT_MODEL)), ())

    def test_malformed_roster_is_refused(self):
        bad_payloads = [
            {"data": ["not-a-mapping"]},
            {"data": [{"id": "deepseek-v4-pro"}]},  # missing providerID
            {"data": [{"providerID": "omniroute"}]},  # missing id
            {"data": [{"providerID": "  ", "id": "deepseek-v4-pro"}]},  # blank providerID
            {"data": [{"providerID": "omniroute", "id": ""}]},  # blank id
        ]
        for payload in bad_payloads:
            with self.subTest(payload=payload):
                with self.assertRaises(ValidationError):
                    normalize_models(payload, (MODEL,))

    def test_live_beta_18286_triple_normalizes_in_allowlist_order(self):
        allowed = ("omniroute/deepseek-v4-pro", "omniroute/gpt-5.6-luna", "omniroute/MiniMaxM3")
        payload = {
            "data": [
                {"providerID": "opencode", "id": "grok-4-fast", "enabled": True, "status": "active"},
                {"providerID": "omniroute", "id": "gpt-5.6-luna", "enabled": True, "status": "active", "name": "GPT 5.6 Luna"},
                {"providerID": "omniroute", "id": "deepseek-v4-pro", "enabled": True, "status": "active", "name": "DeepSeek V4 Pro"},
                {"providerID": "omniroute", "id": "MiniMaxM3", "enabled": True, "status": "active", "name": "MiniMax M3"},
            ],
            "location": {},
        }
        models = normalize_models(payload, allowed)
        self.assertEqual([item.id for item in models], list(allowed))


class FakeSink:
    def __init__(self):
        self.messages = []
        self.sessions = []
        self.events = []

    def message(self, message):
        self.messages.append(message)

    def session(self, runtime_session_id):
        self.sessions.append(runtime_session_id)

    def event(self, kind, data):
        self.events.append((kind, dict(data)))


class AdapterCase(unittest.TestCase):
    def setUp(self):
        self._auth = mock.patch.dict(os.environ, {PASSWORD_ENV: "fixture-password"})
        self._auth.start()
        self.addCleanup(self._auth.stop)
        self._temp = tempfile.TemporaryDirectory()
        self.addCleanup(self._temp.cleanup)
        self.root = Path(self._temp.name).resolve()
        self.home = self.root / "home"
        self.home.mkdir()
        self.workdir = self.root / "work"
        self.workdir.mkdir()
        self.read_root = self.root / "read"
        self.read_root.mkdir()
        self.request_root = self.root / "also-read"
        self.request_root.mkdir()
        self.agent_dir = self.root / "agent"
        self.agent_dir.mkdir()
        self.binary = self.root / "opencode2"
        self.binary.write_text("#!/bin/sh\n", encoding="utf-8")
        self.mcp_command = self.root / "docs-mcp"
        self.mcp_command.write_text("#!/bin/sh\n", encoding="utf-8")
        self.mcp_servers = {
            "docs": McpConfig("stdio", self.mcp_command, ("--root", "/docs"), ("DOCS_TOKEN",)),
            "unused": McpConfig("stdio", self.mcp_command),
        }
        self.environment = {"DOCS_TOKEN": "secret", "PATH": "/tmp/shim"}
        self.config = runtime_config(
            self.binary,
            self.home,
            models=(MODEL, ALT_MODEL),
            skills=("review",),
            mcp=("docs",),
        )
        self.adapter = OpenCodeAdapter()
        self.profile = AgentProfile("implement", "profile body", False, (self.read_root,))
        self.request = StartRequest(
            runtime=RUNTIME_NAME,
            model=MODEL,
            profile="implement",
            task="do the thing",
            workdir=self.workdir,
            read_roots=(self.request_root,),
        )

    def materialize(self, config=None, mcp_servers=None):
        return self.adapter.materialize(
            config or self.config,
            self.home,
            mcp_servers=self.mcp_servers if mcp_servers is None else mcp_servers,
            inherited_environment=self.environment,
        )

    def prove_service(self, *, pid=None, digest=None):
        digest = self.materialize() if digest is None else digest
        plan = build_service_plan(
            self.config, self.home, port=PORT, inherited_environment=self.environment
        )
        config_home, data_home = service_home_paths(self.home)
        descriptor = verify_isolation(
            plan,
            {
                "healthy": True,
                "pid": os.getpid() if pid is None else pid,
                "version": "2.1.0",
            },
            pid=os.getpid() if pid is None else pid,
            config_hash=digest,
        )
        write_service_descriptor(self.home, descriptor)
        return descriptor

    def prepare(self, request=None, profile=None, mcp_servers=None):
        return self.adapter.prepare(
            request or self.request,
            profile or self.profile,
            self.config,
            self.home,
            self.agent_dir,
            mcp_servers=self.mcp_servers if mcp_servers is None else mcp_servers,
            inherited_environment=self.environment,
        )


class DescribeValidateTests(AdapterCase):
    def test_describe_reports_the_frozen_api_and_no_write(self):
        info = self.adapter.describe()
        self.assertEqual((info.name, info.adapter_api_version), (RUNTIME_NAME, ADAPTER_API_VERSION))
        self.assertIn(Capability.STEER, info.capabilities)
        # Without this the capacity collector skips the runtime outright and
        # limits() is never called at all.
        self.assertIn(Capability.LIVE_LIMITS, info.capabilities)
        self.assertNotIn(Capability.WRITE, info.capabilities)
        self.assertNotIn(Capability.EFFORT, info.capabilities)
        self.assertNotIn(Capability.HOOKS, info.capabilities)

    def test_validate_refuses_cli_mode_hooks_and_uncanonical_models(self):
        with self.assertRaises(ValidationError):
            self.adapter.validate(runtime_config(self.binary, self.home, service_mode=None))
        with self.assertRaises(ValidationError):
            self.adapter.validate(
                runtime_config(
                    self.binary,
                    self.home,
                    hooks=(RuntimeHookConfig("PostToolUse", ("agent-run",)),),
                )
            )
        with self.assertRaises(ValidationError):
            self.adapter.validate(runtime_config(self.binary, self.home, models=("minimax-m3",)))

    def test_prepare_refuses_network_profiles(self):
        profile = AgentProfile("research", "Research.", False, (self.read_root,), True)
        with self.assertRaisesRegex(
            ValidationError, "opencode runtime does not support network profiles"
        ):
            self.prepare(profile=profile)


class MaterializeTests(AdapterCase):
    def test_generated_config_is_the_exact_proven_v2_document(self):
        digest = self.materialize()
        path = self.home / CONFIG_RELATIVE_PATH
        text = path.read_text(encoding="utf-8")
        document = json.loads(text)
        self.assertEqual(digest, content_hash(text))
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(document["model"], MODEL)
        self.assertEqual(document["default_agent"], PRIMARY_AGENT)
        provider = document["provider"]["omniroute"]
        self.assertEqual(provider["env"], ["OMNIROUTE_API_KEY"])
        self.assertEqual(provider["npm"], "@ai-sdk/openai-compatible")
        self.assertEqual(provider["options"]["baseURL"], "http://127.0.0.1:20128/v1")
        self.assertEqual(provider["models"]["deepseek-v4-pro"]["id"], "opencode/deepseek-v4-pro")
        self.assertNotIn("providers", document)
        self.assertNotIn("agents", document)
        # v1 resolves skills.paths against the *session* directory, so a bare
        # name is looked up under the agent's workdir and dropped.
        self.assertEqual(
            document["skills"],
            {"paths": [str(skills_root_for(self.home) / "review")]},
        )
        self.assertEqual(document["mcp"]["docs"]["enabled"], True)
        self.assertNotIn("disabled", document["mcp"]["docs"])
        self.assertNotIn("servers", document["mcp"])
        self.assertEqual(digest, self.materialize())


    def test_permission_order_is_preserved_on_disk(self):
        self.materialize()
        text = (self.home / CONFIG_RELATIVE_PATH).read_text(encoding="utf-8")
        document = json.loads(text)
        rules = document["agent"][PRIMARY_AGENT]["permission"]
        self.assertEqual(rules["external_directory"], "ask")
        self.assertEqual(list(rules)[-1], "external_directory")
        self.assertLess(text.index('"bash"'), text.index('"external_directory"'))
        self.assertEqual(
            document["agent"][VERIFY_AGENT]["permission"]["external_directory"], "deny"
        )

    def test_every_permission_action_v1_knows_is_stated_explicitly(self):
        """v1 1.18.18 treats an *unstated* action as "ask", not as a default
        (proven live: with only bash/edit/write/webfetch/external_directory
        set, reading a file inside the session's own directory raised a
        pending permission, and so did ``glob`` and ``todowrite``). An ask
        nobody can answer blocks the tool, so every action v1's own
        PermissionConfig schema names must carry an explicit verdict."""

        self.materialize()
        document = json.loads((self.home / CONFIG_RELATIVE_PATH).read_text(encoding="utf-8"))
        # The key set of v1 1.18.18's PermissionConfig object, read off the
        # running service's own /doc OpenAPI.
        declared = {
            "read", "edit", "glob", "grep", "list", "bash", "task",
            "external_directory", "todowrite", "question", "webfetch",
            "websearch", "lsp", "doom_loop",
        }
        for agent in (PRIMARY_AGENT, VERIFY_AGENT):
            with self.subTest(agent=agent):
                rules = document["agent"][agent]["permission"]
                self.assertEqual(declared - set(rules), set())
                self.assertEqual(rules["read"], "allow")
                self.assertEqual(rules["bash"], "deny")
                self.assertEqual(rules["write"], "deny")
                self.assertEqual(rules["websearch"], "deny")
                self.assertNotIn("ask", set(rules.values()) - {rules["external_directory"]})

    def test_mcp_servers_is_a_required_keyword(self):
        with self.assertRaises(TypeError):
            self.adapter.materialize(self.config, self.home)

    def test_only_selected_servers_are_written(self):
        self.materialize()
        document = json.loads((self.home / CONFIG_RELATIVE_PATH).read_text(encoding="utf-8"))
        self.assertEqual(list(document["mcp"]), ["docs"])

    def test_unresolved_non_stdio_and_unset_mcp_definitions_are_refused(self):
        with self.assertRaises(ValidationError) as missing:
            self.materialize(mcp_servers={})
        self.assertIn("not in the resolved", str(missing.exception))
        with self.assertRaises(ValidationError) as transport:
            self.materialize(
                mcp_servers={"docs": McpConfig("http", self.mcp_command, (), ())}
            )
        self.assertIn("stdio", str(transport.exception))
        with self.assertRaises(ValidationError):
            self.adapter.materialize(
                self.config,
                self.home,
                mcp_servers=self.mcp_servers,
                inherited_environment={"PATH": "/usr/bin"},
            )
        with self.assertRaises(ValidationError):
            self.materialize(mcp_servers={"docs": {"transport": "stdio"}})

    def test_render_is_pure_and_writes_nothing(self):
        before = sorted(path.name for path in self.home.iterdir())
        render_config(
            self.config,
            self.mcp_servers,
            skills_root=self.root / "skills",
            inherited_environment=self.environment,
        )
        self.assertEqual(sorted(path.name for path in self.home.iterdir()), before)

    def test_skill_paths_are_absolute_directories_of_the_physical_copies(self):
        """The child saw only opencode's built-in skill because bare names in
        ``skills.paths`` are resolved against the session's workdir: the live
        service logged 'skill path not found path=<workdir>/delegate' for every
        configured skill. Absolute directories of the materialized copies are
        what v1 scans for ``**/SKILL.md``."""

        document = json.loads(
            render_config(
                self.config,
                self.mcp_servers,
                skills_root=self.root / "skills" / "opencode",
                inherited_environment=self.environment,
            )
        )
        self.assertEqual(
            document["skills"]["paths"],
            [str(self.root / "skills" / "opencode" / "review")],
        )
        for path in document["skills"]["paths"]:
            self.assertTrue(Path(path).is_absolute())

    def test_prepare_renders_the_same_skill_paths_materialize_wrote(self):
        """prepare re-renders the config and refuses the service on any hash
        drift, so its skills root has to be the one materialize used."""

        digest = self.materialize()
        self.prove_service(digest=digest)
        plan = self.adapter.prepare(
            self.request,
            self.profile,
            self.config,
            self.home,
            self.agent_dir,
            mcp_servers=self.mcp_servers,
            inherited_environment=self.environment,
        )
        self.assertEqual(plan.adapter_state["service"]["config_hash"], digest)


class ProbeAndPrepareTests(AdapterCase):
    def test_probe_stays_unavailable_until_isolation_is_proven(self):
        health = self.adapter.probe(self.config, self.home)
        self.assertFalse(health.available)
        self.assertIn("unproven", health.reason)
        self.prove_service()
        proven = self.adapter.probe(self.config, self.home)
        self.assertTrue(proven.available)
        self.assertEqual(proven.version, "2.1.0")

    def test_probe_refuses_a_proven_service_without_password(self):
        self.prove_service()
        with mock.patch.dict(os.environ, {}, clear=True), mock.patch(
            "agent_run.adapters.opencode.service.keychain_server_password",
            return_value=None,
        ):
            health = self.adapter.probe(self.config, self.home)
        self.assertFalse(health.available)
        self.assertEqual(
            health.reason,
            f"{PASSWORD_ENV} must be set to a nonblank value",
        )

    def test_probe_reproves_the_pid_and_the_config_hash(self):
        self.prove_service(pid=2 ** 31 - 1)
        dead = self.adapter.probe(self.config, self.home)
        self.assertFalse(dead.available)
        self.assertIn("is gone", dead.reason)

        self.prove_service()
        self.assertTrue(self.adapter.probe(self.config, self.home).available)
        self.adapter.materialize(
            self.config,
            self.home,
            mcp_servers={"docs": McpConfig("stdio", self.mcp_command, ("--root", "/other"), ())},
            inherited_environment=self.environment,
        )
        changed = self.adapter.probe(self.config, self.home)
        self.assertFalse(changed.available)
        self.assertIn("changed after the service was proven", changed.reason)

    def test_models_and_prepare_refuse_without_a_proven_service(self):
        self.assertEqual(self.adapter.models(self.config, self.home), ())
        with self.assertRaises(ServiceIsolationError):
            self.prepare()

    def test_prepare_attaches_to_the_proven_service_and_starts_no_second_serve(self):
        descriptor = self.prove_service()
        plan = self.prepare()
        config_home, _ = service_home_paths(self.home)
        self.assertEqual(plan.argv, ())
        self.assertEqual(plan.cwd, self.workdir)
        self.assertEqual(plan.environment["XDG_CONFIG_HOME"], str(config_home))
        self.assertEqual(plan.environment["OPENCODE_DISABLE_CLAUDE_CODE"], "1")
        self.assertEqual(plan.environment["PATH"], SERVICE_PATH)
        self.assertNotIn("DOCS_TOKEN", plan.environment)
        self.assertEqual(plan.runtime_stream_path, self.agent_dir / "runtime.jsonl")
        self.assertIn("do the thing", plan.initial_input)
        self.assertEqual(plan.adapter_state["service"]["pid"], descriptor.pid)
        self.assertEqual(plan.adapter_state["service"]["config_hash"], descriptor.config_hash)
        self.assertEqual(
            plan.adapter_state["model"], {"providerID": "omniroute", "id": "deepseek-v4-pro"}
        )
        self.assertEqual(plan.adapter_state["agent"], PRIMARY_AGENT)
        self.assertNotIn("write_roots", plan.adapter_state)

    def test_prepare_unions_the_profile_and_request_read_roots(self):
        self.prove_service()
        plan = self.prepare()
        self.assertEqual(
            sorted(plan.adapter_state["read_roots"]),
            sorted([str(self.read_root), str(self.request_root)]),
        )

    def test_prepare_collapses_a_nested_request_root_into_the_profile_root(self):
        self.prove_service()
        nested = self.read_root / "inner"
        nested.mkdir()
        request = _replace(self.request, read_roots=(nested,))
        plan = self.prepare(request=request)
        self.assertEqual(plan.adapter_state["read_roots"], [str(self.read_root)])

    def test_prepare_refuses_a_config_the_service_was_not_proven_with(self):
        self.prove_service()
        with self.assertRaises(ServiceIsolationError) as caught:
            self.prepare(
                mcp_servers={"docs": McpConfig("stdio", self.mcp_command, ("--root", "/other"), ())}
            )
        self.assertIn("proven with a different generated config", str(caught.exception))

    def test_prepare_refuses_write(self):
        self.prove_service()
        writable = AgentProfile("implement", "body", True, (self.read_root,))
        with self.assertRaises(ValidationError) as caught:
            self.prepare(request=_replace(self.request, write=True), profile=writable)
        self.assertIn("no write capability", str(caught.exception))

    def test_prepare_refuses_unsupported_requests(self):
        self.prove_service()
        with self.assertRaises(ValidationError):
            self.prepare(request=_replace(self.request, model="opencode/unlisted"))
        with self.assertRaises(ValidationError):
            self.prepare(request=_replace(self.request, effort="high"))
        with self.assertRaises(ValidationError):
            self.prepare(
                request=_replace(self.request, output_schema={"type": "object", "raw": True})
            )
        with self.assertRaises(ValidationError):
            self.prepare(request=_replace(self.request, output_schema={"type": "string"}))

    def test_prepare_requires_the_mcp_servers_keyword(self):
        self.prove_service()
        with self.assertRaises(TypeError):
            self.adapter.prepare(
                self.request, self.profile, self.config, self.home, self.agent_dir
            )


class FakeService:
    """Enough of the proven v2 service to drive one session end to end.

    ``statuses`` entries are ``/api/session/active`` shapes: ``{"type":
    "running"}`` while a turn is in flight, or ``None`` when the session is
    absent from that map (idle or settled). ``outcome`` is what
    ``GET /api/session/{id}`` reports once ``wait()`` decides the turn is
    final. ``outcome=None`` (the default is ``"succeeded"``, not ``None``)
    simulates the pinned v1 1.18.18 shape proven live: no ``outcome`` key at
    all, forcing the terminal status to be inferred from the last message.
    """

    def __init__(
        self,
        directory,
        statuses,
        message_pages,
        permission_pages=(),
        outcome="succeeded",
        permission_error=None,
    ):
        self.directory = Path(directory)
        self.statuses = list(statuses)
        self.message_pages = list(message_pages)
        self.permission_pages = list(permission_pages)
        self.outcome = outcome
        self.permission_error = permission_error
        self.calls = []

    def _capture(self, payload):
        body = json.dumps(payload).encode("utf-8")
        descriptor, name = tempfile.mkstemp(dir=self.directory, prefix="reply.", suffix=".json")
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
        return HttpResponse(status=200, path="/fake", body_path=Path(name), body_bytes=len(body))

    def _next(self, pages, default):
        if not pages:
            return default
        return pages.pop(0) if len(pages) > 1 else pages[0]

    def session_status(self):
        self.calls.append(("session_status",))
        entry = self._next(self.statuses, None)
        return {} if entry is None else {"ses_1": entry}

    def session_info(self, session_id):
        self.calls.append(("session_info", session_id))
        return {} if self.outcome is None else {"outcome": self.outcome}

    def messages(self, session_id):
        self.calls.append(("messages", session_id))
        return self._capture(self._next(self.message_pages, []))

    def permissions(self, session_id):
        self.calls.append(("permissions", session_id))
        if self.permission_error is not None:
            raise self.permission_error
        return self._capture({"data": self._next(self.permission_pages, [])})

    def answer_permission(self, session_id, permission_id, payload):
        self.calls.append(("answer_permission", permission_id, dict(payload)))
        return {}

    def create_session(self, payload):
        self.calls.append(("create_session", dict(payload)))
        return {"id": "ses_1"}

    def prompt_async(self, session_id, payload):
        self.calls.append(("prompt_async", session_id, dict(payload)))
        return {}

    def abort(self, session_id):
        self.calls.append(("abort", session_id))
        return {}


class SessionTests(AdapterCase):
    def setUp(self):
        super().setUp()
        self.captures = self.root / "captures"
        self.captures.mkdir()
        self.clock = 0.0
        self.slept = []

    def advance(self, seconds):
        self.slept.append(seconds)
        self.clock += seconds

    def session(self, service, broker=None, **kwargs):
        self.sink = FakeSink()
        return OpenCodeRuntimeSession(
            service,
            "ses_1",
            self.sink,
            broker=broker if broker is not None else PermissionBroker((self.read_root,)),
            pid=4242,
            response_dir=self.agent_dir,
            model={"providerID": "omniroute", "id": "deepseek-v4-pro"},
            sleep=self.advance,
            monotonic=lambda: self.clock,
            **kwargs,
        )

    def remaining(self):
        return sorted(path.name for path in self.captures.iterdir())

    def test_initial_idle_is_ignored_until_the_session_is_busy(self):
        answer = "готово ✅"
        service = FakeService(
            self.captures,
            [None, {"type": "running"}, {"type": "running"}, None],
            # One page per fetch: the first absence finds nothing, the last one the answer.
            [[], [message("user", "task"), message("assistant", answer, at=2.0)]],
        )
        session = self.session(service)
        outcome = session.wait(30.0)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(outcome.runtime_session_id, "ses_1")
        self.assertEqual(session.pid, 4242)
        self.assertEqual(self.sink.sessions, ["ses_1"])
        self.assertEqual([item.content for item in self.sink.messages], ["task", answer])
        # The first absence was probed for output, found none, and kept waiting.
        self.assertEqual([call for call in service.calls if call[0] == "messages"].__len__(), 2)
        self.assertEqual(len(self.remaining()), 1)
        # A bare filename, not the absolute capture path: the persistence
        # guard (agent_run.state.db.validate_message_storage) refuses an
        # absolute raw_ref, so this is what the supervisor must be able to
        # store (see test_message_raw_ref_is_a_normalized_relative_path_the_store_accepts).
        self.assertEqual(self.sink.messages[0].raw_ref, self.remaining()[0])

    def test_message_raw_ref_is_a_normalized_relative_path_the_store_accepts(self):
        """Drive the exact call the supervisor makes: StoreEventSink.message
        persists every emitted Message via StateStore.append_message, which
        enforces agent_run.state.db.validate_message_storage. A raw_ref taken
        straight from the HTTP capture's absolute body_path fails that guard
        with "raw_ref must be a normalized relative path"; the bare capture
        filename is what must reach the store instead.
        """

        service = FakeService(
            self.captures, [{"type": "running"}, None], [[message("assistant", "done", at=1.0)]]
        )
        self.session(service).wait(30.0)
        recorded = self.sink.messages[0]
        self.assertEqual(recorded.raw_ref, self.remaining()[0])

        store = StateStore.initialize(self.root / "state.db")
        self.addCleanup(store.close)
        agent_id = store.create_agent(
            _replace(self.request, timeout_seconds=60),
            task_summary="t",
            config_revision="r",
        ).agent_id
        store.append_message(agent_id, recorded)
        self.assertEqual(len(store.transcript(agent_id)), 1)

    def test_answer_is_recorded_as_exact_utf8_bytes_and_hash(self):
        from agent_run.verify import DEFAULT_SENTINEL

        answer = "готово ✅"
        service = FakeService(
            self.captures, [{"type": "running"}, None], [[message("assistant", answer, at=2.0)]]
        )
        outcome = self.session(service).wait(30.0)
        path = self.agent_dir / ANSWER_NAME
        expected = f"{answer}\n{DEFAULT_SENTINEL}\n".encode("utf-8")
        self.assertEqual(path.read_bytes(), expected)
        self.assertEqual(outcome.answer_path, path)
        self.assertEqual(outcome.answer_bytes, len(expected))
        self.assertEqual(outcome.answer_sha256, hashlib.sha256(expected).hexdigest())
        self.assertNotEqual(outcome.answer_bytes, len(answer))

    def test_idle_with_primary_output_settles_immediately(self):
        service = FakeService(self.captures, [None], [[message("assistant", "done", at=1.0)]])
        outcome = self.session(service).wait(30.0)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(self.slept, [])

    def test_sub_agent_output_alone_does_not_settle_an_idle_session(self):
        service = FakeService(
            self.captures,
            [None],
            [[message("assistant", "verifier notes", agent=VERIFY_AGENT, at=1.0)]],
        )
        session = self.session(service)
        self.assertIsNone(session.wait(0.5))
        self.assertEqual(self.remaining(), [])

    def test_wait_times_out_without_leaking_captures(self):
        service = FakeService(self.captures, [None], [[]])
        session = self.session(service)
        self.assertIsNone(session.wait(1.0))
        self.assertEqual(self.remaining(), [])
        self.assertGreater(len(self.slept), 0)

    def test_error_state_reports_a_failure_and_still_emits_the_transcript(self):
        service = FakeService(
            self.captures,
            [{"type": "running"}, None],
            [[message(
                "assistant", "partial", at=2.0,
                error={"type": "ProviderError", "message": "429"},
            )]],
            outcome="failed",
        )
        outcome = self.session(service).wait(30.0)
        self.assertEqual(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "ProviderError")
        self.assertEqual([item.content for item in self.sink.messages], ["partial"])

    def test_v1_shaped_service_settles_from_the_last_message_with_no_outcome_field(self):
        """v1 1.18.18's ``GET /api/session/{id}`` never carries ``outcome``
        (proven live, T041): ``wait()`` must still reach a terminal Outcome
        from ``/api/session/active`` absence plus the last assistant message."""

        answer = "PONG"
        service = FakeService(
            self.captures, [{"type": "running"}, None], [[message("assistant", answer, at=2.0)]],
            outcome=None,
        )
        outcome = self.session(service).wait(30.0)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(outcome.runtime_session_id, "ses_1")

    def test_v1_shaped_service_reports_cancelled_on_message_aborted_error(self):
        service = FakeService(
            self.captures,
            [{"type": "running"}, None],
            [[message(
                "assistant", "", at=2.0,
                error={"name": "MessageAbortedError", "data": {"message": "aborted"}},
            )]],
            outcome=None,
        )
        outcome = self.session(service).wait(30.0)
        self.assertEqual(outcome.status, AgentStatus.CANCELLED)
        self.assertEqual(outcome.failure_kind, "MessageAbortedError")

    def test_tool_round_gap_does_not_settle_the_wait_prematurely(self):
        """Live, T17B: ``/api/session/active`` drops the session between the
        tool-round's two HTTP calls, and the only message on record at that
        instant is a bare tool-call part -- the exact capture in
        ``OutcomeTests.LIVE_V1_TOOL_ROUND_MESSAGES``. The old ``wait()``
        treated any settled poll as final once it had ever seen "running",
        so it called ``_finish()`` here and crashed with "opencode session
        outcome is not terminal: None". It must instead keep waiting -- and,
        since this fixture never produces a real answer, time out cleanly
        rather than raise or leak the capture."""

        service = FakeService(
            self.captures,
            [{"type": "running"}, None],
            [OutcomeTests.LIVE_V1_TOOL_ROUND_MESSAGES],
        )
        session = self.session(service)
        self.assertIsNone(session.wait(1.0))
        self.assertEqual(self.remaining(), [])

    def test_multi_round_tool_session_settles_once_the_final_answer_lands(self):
        """Schema-faithful multi-round tool session (T17B): the first settled
        poll lands mid-round on a bare tool-call message (must keep waiting),
        the engine resumes ("running" again), and the second settled poll's
        newest message is the real answer that followed the completed tool
        call -- newest-first, per the proven ``GET .../message`` contract."""

        tool_call = {
            "id": "msg_tool",
            "type": "assistant",
            "agent": PRIMARY_AGENT,
            "content": [
                {
                    "type": "tool",
                    "id": "call_1",
                    "name": "read",
                    "provider": {"executed": True},
                    "state": {
                        "status": "completed",
                        "input": {"path": "README.md"},
                        "content": [{"type": "text", "text": "file contents"}],
                    },
                }
            ],
        }
        mid_round = [tool_call, message("user", "first read a file then answer")]
        final = [message("assistant", "TOOLPATH_OK", at=2.0), tool_call]
        service = FakeService(
            self.captures,
            [{"type": "running"}, None, {"type": "running"}, None],
            [mid_round, final],
        )
        outcome = self.session(service).wait(30.0)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(
            [call for call in service.calls if call[0] == "messages"].__len__(), 2
        )

    def test_cancel_before_any_message_lands_still_settles_as_cancelled(self):
        """v1 1.18.18: an ``/interrupt`` fired right after ``/prompt`` can
        settle with zero assistant messages and "running" never observed
        (proven live, T041). Without ``cancel()`` recording its own intent,
        ``wait()``'s settle condition (working, or an extracted answer) would
        never become true, and this would spin to a timeout -- never even
        reaching ``_finish()`` -- instead of resolving to ``CANCELLED``."""

        service = FakeService(self.captures, [None], [[]], outcome=None)
        session = self.session(service)
        session.cancel(2.0)
        outcome = session.wait(1.0)
        self.assertIsNotNone(outcome)
        self.assertEqual(outcome.status, AgentStatus.CANCELLED)

    def test_permissions_of_this_session_are_answered_exactly_once(self):
        pending = [
            permission("p1", "external_directory", [f"{self.read_root}/*"]),
            permission("p2", "bash", ["ls"]),
            permission("p3", "external_directory", ["/etc/*"], session="other"),
        ]
        service = FakeService(
            self.captures,
            [{"type": "running"}, None],
            [[message("assistant", "done", at=1.0)]],
            [pending, pending],
        )
        outcome = self.session(service).wait(30.0)
        answered = [call for call in service.calls if call[0] == "answer_permission"]
        self.assertEqual(
            [(call[1], call[2]["reply"]) for call in answered],
            [("p1", "once"), ("p2", "reject")],
        )
        self.assertEqual(self.sink.events, [("permissions_blocked", {"bash": 1})])
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)

    def test_a_400_on_the_permission_list_is_reported_once_and_never_fatal(self):
        """v1 1.18.18 fails its own response schema whenever a pending
        permission's metadata carries a present-but-undefined property --
        proven live twice against the canary service::

            400 {"_tag": "InvalidRequestError",
                 "message": "Expected JSON value, got undefined\\n
                             at [\\"data\\"][0][\\"metadata\\"][\\"path\\"]",
                 "kind": "Body"}

        It fires exactly while there is something to answer, and it used to
        propagate out of wait() and kill the run with supervision_failed."""

        service = FakeService(
            self.captures,
            [{"type": "running"}, None, {"type": "running"}, None],
            [[], [message("assistant", "done", at=2.0)]],
            permission_error=HttpError(400, "/api/session/ses_1/permission", "InvalidRequestError"),
        )
        outcome = self.session(service).wait(30.0)
        self.assertEqual(outcome.status, AgentStatus.SUCCEEDED)
        self.assertEqual(self.sink.events, [("permissions_unreadable", {"status": 400})])

    def test_a_non_400_permission_failure_still_fails_closed(self):
        service = FakeService(
            self.captures,
            [{"type": "running"}, None],
            [[message("assistant", "done", at=1.0)]],
            permission_error=HttpError(500, "/api/session/ses_1/permission"),
        )
        with self.assertRaises(HttpError):
            self.session(service).wait(30.0)

    def test_a_rejected_permission_settles_the_dead_turn_instead_of_timing_out(self):
        """Proven live on v1 1.18.18: replying "reject" interrupts the tool
        within ~100ms and ends the turn, leaving one assistant message that
        carries only the errored tool part -- no text, no message-level
        error. That is exactly the shape the T17B mid-tool-round rule keeps
        waiting on, so the run polled a dead session to its deadline and
        timed out with no answer at all."""

        interrupted = {
            "id": "msg_tool",
            "type": "assistant",
            "agent": PRIMARY_AGENT,
            "time": {"created": 1.0},
            "content": [
                {
                    "type": "tool",
                    "id": "call_1",
                    "name": "read",
                    "provider": {"executed": False},
                    "state": {
                        "status": "error",
                        "input": {"path": "/etc/passwd"},
                        "content": [],
                        "structured": {},
                        "error": {"type": "unknown", "message": "Tool execution interrupted"},
                    },
                }
            ],
        }
        service = FakeService(
            self.captures,
            [{"type": "running"}, None],
            [[interrupted, message("user", "read /etc/passwd")]],
            [[permission("p1", "external_directory", ["/etc/*"])]],
            outcome=None,
        )
        outcome = self.session(service).wait(30.0)
        self.assertEqual(outcome.status, AgentStatus.FAILED)
        self.assertEqual(outcome.failure_kind, "permission_rejected")
        self.assertEqual(outcome.failure_text, "Tool execution interrupted")
        # One poll interval, not the 30s deadline this used to burn.
        self.assertEqual(self.slept, [0.25])
        # Exactly the one capture the emitted transcript's raw_ref points at.
        self.assertEqual(len(self.remaining()), 1)

    def test_steer_and_cancel_use_engine_native_calls(self):
        service = FakeService(
            self.captures, [{"type": "running"}, None], [[]], outcome="interrupted"
        )
        session = self.session(service)
        session.steer("focus on tests")
        session.cancel(2.0)
        steered = [call for call in service.calls if call[0] == "prompt_async"][0]
        # v1's prompt body has no per-turn agent/model field; "delivery":
        # "steer" is the engine-native way to interject into a running turn,
        # proven live via v1's own /doc OpenAPI enum ["steer", "queue"].
        self.assertEqual(steered[2], {"prompt": {"text": "focus on tests"}, "delivery": "steer"})
        self.assertIn(("abort", "ses_1"), service.calls)
        self.assertEqual(session.wait(30.0).status, AgentStatus.CANCELLED)
        with self.assertRaises(ValidationError):
            session.steer("  ")

    def test_malformed_status_payloads_are_refused(self):
        service = FakeService(self.captures, [["not", "a", "status"]], [[]])
        with self.assertRaises(ValidationError):
            self.session(service).wait(30.0)

    def test_launch_opens_a_session_and_prompts_it_asynchronously(self):
        self.prove_service()
        plan = self.prepare()
        service = FakeService(self.captures, [{"type": "running"}], [[]])
        sink = FakeSink()
        session = self.adapter.launch(plan, sink, client=service)
        self.assertEqual(sink.sessions, ["ses_1"])
        self.assertEqual(service.calls[0][0], "create_session")
        self.assertEqual(service.calls[1][0], "prompt_async")
        # v1's prompt body carries no per-turn agent/model at all -- proven
        # live via v1's own /doc OpenAPI (additionalProperties: false on
        # {id?, prompt, delivery?, resume?}); selection is fixed once, above,
        # at session-create time.
        self.assertEqual(service.calls[1][2], {"prompt": {"text": plan.initial_input}})
        self.assertEqual(session.pid, plan.adapter_state["service"]["pid"])

    def test_launch_session_create_uses_the_v2_model_ref_shape(self):
        """Regression: beta-18286 400s ``/api/session`` with body detail
        ``{"_tag": "InvalidRequestError", "message": "Missing key\\n  at
        [\\"model\\"][\\"id\\"]"}`` when the create body sends ``modelID``
        instead of the ``Model.Ref`` schema's ``id``; proven live against the
        canary service before this fix landed. v1 additionally rejects a
        "title" field outright (additionalProperties: false on
        {id?, agent?, model?, location?}), proven live via v1's own /doc.
        """
        self.prove_service()
        plan = self.prepare()
        service = FakeService(self.captures, [{"type": "running"}], [[]])
        self.adapter.launch(plan, FakeSink(), client=service)
        created = [call for call in service.calls if call[0] == "create_session"][0]
        self.assertEqual(created[1]["model"], {"providerID": "omniroute", "id": "deepseek-v4-pro"})
        self.assertNotIn("modelID", created[1]["model"])
        self.assertNotIn("title", created[1])

    def test_launch_anchors_the_session_at_the_requests_workdir(self):
        """Without ``location``, a v1 session inherits the *service* process's
        cwd -- the generated runtime home -- so every read of the task's own
        files is an external directory and raises a permission ask. Both final
        canaries died on exactly that ask (proven live: the pending request
        named the instance directory, not anything the task asked for)."""

        self.prove_service()
        plan = self.prepare()
        service = FakeService(self.captures, [{"type": "running"}], [[]])
        self.adapter.launch(plan, FakeSink(), client=service)
        created = [call for call in service.calls if call[0] == "create_session"][0]
        self.assertEqual(created[1]["location"], {"directory": str(self.request.workdir)})


def _replace(request, **changes):
    values = {
        "runtime": request.runtime,
        "model": request.model,
        "profile": request.profile,
        "task": request.task,
        "workdir": request.workdir,
        "write": request.write,
        "effort": request.effort,
        "timeout_seconds": request.timeout_seconds,
        "read_roots": request.read_roots,
        "output_schema": request.output_schema,
    }
    values.update(changes)
    return StartRequest(**values)


#: The rows the in-container SQL produces from the newest real opencode-go
#: round (``quota_snapshots`` ids 27506-27511, copied out of the live store):
#: newest snapshot per active, quota-visible connection. Averaged they
#: reproduce the pool figures OmniRoute itself recorded that round:
#: weekly 92, session 95, mcp_monthly 99.
_REAL_ROWS = (
    {"window_key": "session", "remaining_percentage": 90.0,
     "next_reset_at": "2026-08-24T19:20:58.435Z", "created_at": "2026-08-24T19:17:02.435Z"},
    {"window_key": "weekly", "remaining_percentage": 84.0,
     "next_reset_at": "2026-08-31T00:00:00.435Z", "created_at": "2026-08-24T19:17:02.436Z"},
    {"window_key": "mcp_monthly", "remaining_percentage": 98.0,
     "next_reset_at": "2026-09-24T13:23:17.435Z", "created_at": "2026-08-24T19:17:02.437Z"},
    {"window_key": "session", "remaining_percentage": 100.0,
     "next_reset_at": "2026-08-24T19:24:59.554Z", "created_at": "2026-08-24T19:17:02.555Z"},
    {"window_key": "weekly", "remaining_percentage": 100.0,
     "next_reset_at": "2026-08-31T00:00:00.555Z", "created_at": "2026-08-24T19:17:02.555Z"},
    {"window_key": "mcp_monthly", "remaining_percentage": 100.0,
     "next_reset_at": "2026-09-23T22:16:31.555Z", "created_at": "2026-08-24T19:17:02.555Z"},
)
_NEWEST_OBSERVATION = "2026-08-24T19:17:02.555Z"


class OmniRouteQuotaPoolTests(unittest.TestCase):
    """limits() over the only real OmniRoute quota surface: its own sqlite."""

    def setUp(self):
        self.root = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.root, True)
        self.fresh = (
            datetime.fromisoformat(_NEWEST_OBSERVATION).timestamp() + 60.0
        )

    def samples_by_window(self, now, rows=_REAL_ROWS):
        return {
            sample.window: sample
            for sample in pool_samples(now, fetch_rows=lambda: rows)
        }

    def test_pool_windows_average_equal_accounts_and_take_the_soonest_reset(self):
        samples = self.samples_by_window(self.fresh)
        self.assertEqual(sorted(samples), ["mcp_monthly", "session_5h", "weekly"])
        self.assertEqual(
            {window: sample.remaining_percent for window, sample in samples.items()},
            {"weekly": 92.0, "session_5h": 95.0, "mcp_monthly": 99.0},
        )
        for sample in samples.values():
            self.assertEqual(sample.source, "omniroute_quota_pool")
            self.assertEqual((sample.lane, sample.target), ("pool", "opencode-go:pool"))
            self.assertEqual(
                sample.observed_at,
                min(
                    datetime.fromisoformat(row["created_at"])
                    for row in _REAL_ROWS
                    if omniroute.WINDOWS.get(row["window_key"]) == sample.window
                ),
            )
            self.assertEqual(sample.valid_for_seconds, LIMITS_STALE_SECONDS)
        # The soonest reset in the pool is the one that bites first.
        self.assertEqual(
            samples["session_5h"].reset_at,
            datetime.fromisoformat("2026-08-24T19:20:58.435Z"),
        )
        self.assertEqual(
            samples["mcp_monthly"].reset_at,
            datetime.fromisoformat("2026-09-23T22:16:31.555Z"),
        )

    def test_a_stale_round_keeps_its_reset_but_reports_unknown_not_zero(self):
        samples = self.samples_by_window(self.fresh + LIMITS_STALE_SECONDS)
        self.assertEqual(len(samples), 3)
        for sample in samples.values():
            self.assertIsNone(sample.remaining_percent)
            self.assertEqual(sample.source, "unknown")
            self.assertIsNotNone(sample.reset_at)
            self.assertIsNotNone(sample.observed_at)

    def test_unnamed_windows_and_unusable_rows_are_not_reported(self):
        rows = (
            {"window_key": "some_new_window", "remaining_percentage": 50.0,
             "next_reset_at": None, "created_at": _NEWEST_OBSERVATION},
            {"window_key": "session", "remaining_percentage": 100.0,
             "next_reset_at": None, "created_at": _NEWEST_OBSERVATION},
        )
        samples = self.samples_by_window(self.fresh, rows)
        self.assertEqual(sorted(samples), ["session_5h"])
        self.assertEqual(samples["session_5h"].remaining_percent, 100.0)
        self.assertIsNone(samples["session_5h"].reset_at)

        malformed = dict(rows[1])
        malformed["remaining_percentage"] = None
        with self.assertRaises(omniroute.CapacitySourceError):
            pool_samples(self.fresh, fetch_rows=lambda: [malformed])

    def test_a_failing_or_garbage_row_source_is_no_evidence(self):
        def broken():
            raise OSError("docker is gone")

        with self.assertRaises(omniroute.CapacitySourceError):
            pool_samples(self.fresh, fetch_rows=broken)
        with self.assertRaises(omniroute.CapacitySourceError):
            pool_samples(self.fresh, fetch_rows=lambda: None)
        with self.assertRaises(omniroute.CapacitySourceError):
            pool_samples(self.fresh, fetch_rows=lambda: "oops")
        self.assertEqual(pool_samples(self.fresh, fetch_rows=lambda: []), ())

    def test_invalid_window_state_does_not_inflate_pool_remaining(self):
        rows = [
            {"window_key": "session", "remaining_percentage": 90.0,
             "next_reset_at": "2026-08-25T00:00:00Z", "created_at": _NEWEST_OBSERVATION},
            {"window_key": "session", "remaining_percentage": 10.0,
             "next_reset_at": "2026-08-25T00:00:00Z", "created_at": "2026-08-24T17:17:02.437Z"},
        ]
        sample = self.samples_by_window(self.fresh, rows)["session_5h"]
        self.assertIsNone(sample.remaining_percent)
        self.assertEqual(sample.observed_at, datetime.fromisoformat(rows[1]["created_at"]))

    def test_future_and_reset_boundary_make_window_unknown(self):
        future = [{"window_key": "weekly", "remaining_percentage": 99.0,
                   "next_reset_at": "2026-09-01T00:00:00Z", "created_at": "2026-09-01T00:00:00Z"}]
        self.assertIsNone(self.samples_by_window(self.fresh, future)["weekly"].remaining_percent)
        boundary = [{"window_key": "weekly", "remaining_percentage": 99.0,
                     "next_reset_at": _NEWEST_OBSERVATION, "created_at": _NEWEST_OBSERVATION}]
        self.assertIsNone(self.samples_by_window(self.fresh, boundary)["weekly"].remaining_percent)

    def test_row_cap_overflow_is_a_source_failure(self):
        row = {"window_key": "weekly", "remaining_percentage": 99.0,
               "next_reset_at": None, "created_at": _NEWEST_OBSERVATION}
        with self.assertRaises(omniroute.CapacitySourceError):
            pool_samples(self.fresh, fetch_rows=lambda: [row] * 65)

    def test_a_first_failed_quota_read_is_retried_once_and_stays_silent(self):
        # The docker exec is flaky in place; a single failure must not log,
        # because a warning per flake would drown the real outages.
        failing = subprocess.CompletedProcess([], 1, stdout="", stderr="transient")
        recovered = subprocess.CompletedProcess([], 0, stdout="[]", stderr="")
        with mock.patch.object(
            omniroute.subprocess, "run", side_effect=[failing, recovered]
        ), mock.patch.object(omniroute.time, "sleep") as slept, self.assertNoLogs(
            "agent_run.capacity", level="WARNING"
        ):
            self.assertEqual(omniroute._docker_rows(), [])
        self.assertEqual(slept.call_count, 1)
        slept.assert_called_once_with(omniroute._DOCKER_RETRY_DELAY_SECONDS)

    def test_a_successful_quota_read_is_silent_and_runs_once(self):
        good = subprocess.CompletedProcess([], 0, stdout="[]", stderr="")
        with mock.patch.object(
            omniroute.subprocess, "run", return_value=good
        ) as run, mock.patch.object(
            omniroute.time, "sleep"
        ) as slept, self.assertNoLogs(
            "agent_run.capacity", level="WARNING"
        ):
            self.assertEqual(omniroute._docker_rows(), [])
        self.assertEqual(run.call_count, 1)
        self.assertEqual(slept.call_count, 0)

    def test_a_repeatedly_failing_quota_read_warns_with_kind_and_tail(self):
        failing = subprocess.CompletedProcess(
            [], 1, stdout="", stderr="docker: no such container\nfurther noise"
        )
        with mock.patch.object(
            omniroute.subprocess, "run", return_value=failing
        ), mock.patch.object(
            omniroute.time, "sleep"
        ), self.assertLogs(
            "agent_run.capacity", level="WARNING"
        ) as logs:
            with self.assertRaises(omniroute.CapacitySourceError):
                omniroute._docker_rows()
        self.assertEqual(len(logs.output), 1)
        message = logs.output[0]
        self.assertIn("kind=exit_code", message)
        self.assertIn("rc=1", message)
        self.assertNotIn("no such container", message)
        self.assertNotIn("\n", message)

    def test_limits_reads_the_omniroute_store_and_never_the_runtime_home(self):
        with mock.patch.object(omniroute, "_docker_rows", lambda: list(_REAL_ROWS)):
            with mock.patch.object(opencode_adapter.time, "time", lambda: self.fresh):
                samples = OpenCodeAdapter().limits(
                    runtime_config(self.root / "bin", self.root / "home"),
                    self.root / "nonexistent-home",
                )
        self.assertEqual(
            sorted(sample.window for sample in samples),
            ["mcp_monthly", "session_5h", "weekly"],
        )
        self.assertTrue(all(s.source == "omniroute_quota_pool" for s in samples))


if __name__ == "__main__":
    unittest.main()
