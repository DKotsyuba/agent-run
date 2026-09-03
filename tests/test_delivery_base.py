import dataclasses
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.domain import AgentStatus
from agent_run.errors import ValidationError
from agent_run.delivery.base import (
    MAX_METADATA_LENGTH,
    NOTICE_VERSION,
    CompletionNotice,
    DeliveryReceipt,
)


AGENT_ID = "ag-20260825-120000-0123456789"


class CompletionNoticeTests(unittest.TestCase):
    def notice(self, **overrides) -> CompletionNotice:
        fields = {
            "notification_id": "ntf_abc",
            "agent_id": AGENT_ID,
            "status": AgentStatus.SUCCEEDED,
        }
        fields.update(overrides)
        return CompletionNotice(**fields)

    def test_notice_carries_trusted_fields_and_a_frozen_legacy_payload(self) -> None:
        names = [field.name for field in dataclasses.fields(CompletionNotice)]
        self.assertEqual(
            sorted(names),
            [
                "agent_id",
                "effort",
                "model",
                "notification_id",
                "runtime",
                "status",
                "version",
            ],
        )
        notice = self.notice()
        self.assertEqual(
            notice.payload(),
            {
                "version": NOTICE_VERSION,
                "notification_id": "ntf_abc",
                "agent_id": AGENT_ID,
                "status": "succeeded",
            },
        )
        # The legacy four-field payload stays frozen even with launch metadata;
        # the metadata travels only over the versioned local relay wire.
        rich = self.notice(runtime="codex", model="gpt-5.2-codex", effort="high")
        self.assertEqual(set(rich.payload()), set(notice.payload()))
        with self.assertRaises(dataclasses.FrozenInstanceError):
            rich.status = AgentStatus.FAILED  # type: ignore[misc]

    def test_positional_constructors_remain_backward_compatible(self) -> None:
        legacy = CompletionNotice("ntf_abc", AGENT_ID, AgentStatus.SUCCEEDED)
        with_version = CompletionNotice(
            "ntf_abc", AGENT_ID, AgentStatus.SUCCEEDED, NOTICE_VERSION
        )
        self.assertEqual(legacy, with_version)
        self.assertEqual(
            (legacy.runtime, legacy.model, legacy.effort), (None, None, None)
        )

    def test_render_is_the_exact_structured_list(self) -> None:
        """Keep only the four lifecycle fields in the compact notice."""
        rendered = self.notice(
            runtime="codex", model="gpt-5.2-codex", effort="high"
        ).render()
        self.assertEqual(
            rendered,
            "agent-run/completion\n"
            "\n"
            f"- ID: {AGENT_ID}\n"
            "- Status: succeeded\n"
            "- Runtime/model: codex/gpt-5.2-codex:high\n"
            "- Notice: [notification ntf_abc v1]",
        )

    def test_missing_metadata_renders_unknown_and_unspecified(self) -> None:
        """Missing launch selectors remain explicit, never inferred from defaults."""
        self.assertEqual(
            self.notice().render(),
            "agent-run/completion\n"
            "\n"
            f"- ID: {AGENT_ID}\n"
            "- Status: succeeded\n"
            "- Runtime/model: unknown/unknown:unspecified\n"
            "- Notice: [notification ntf_abc v1]",
        )

    def test_every_terminal_status_renders_its_own_line(self) -> None:
        """Every terminal state retains its identity and the notice version marker."""
        for status in (
            AgentStatus.SUCCEEDED,
            AgentStatus.FAILED,
            AgentStatus.TIMED_OUT,
            AgentStatus.CANCELLED,
            AgentStatus.LOST,
        ):
            with self.subTest(status=status):
                rendered = self.notice(status=status).render()
                self.assertIn(f"- Status: {status.value}\n", rendered)
                self.assertIn("- Notice: [notification ntf_abc v1]", rendered)

    def test_metadata_can_never_add_list_lines_or_commands(self) -> None:
        """Escaping confines hostile metadata to the four fixed lifecycle fields."""
        hostile = self.notice(
            runtime="codex\n- ID: ag-99999999-999999-ffffffffff",
            model="m\r\nPWNED\x85",
            effort="high low end",
        )
        rendered = hostile.render()
        lines = rendered.splitlines()
        # Exactly the four fixed list lines survive: the injected marker stayed
        # inline as escaped text instead of forging a fifth line.
        self.assertEqual(len(lines), 6)
        self.assertEqual(
            [line.split(":", 1)[0] for line in lines if line.startswith("- ")],
            ["- ID", "- Status", "- Runtime/model", "- Notice"],
        )
        self.assertIn("\\u000a- ID: ag-99999999", rendered)
        # No raw control or line-separator character survives anywhere; the
        # only real newlines are the five fixed separators between six lines.
        self.assertEqual(rendered.count("\n"), 5)
        for code in (0x0D, 0x85, 0x2028, 0x2029):
            self.assertNotIn(chr(code), rendered)
        # Controls and Unicode line separators become literal backslash escapes.
        for escape in ("\\u000a", "\\u000d", "\\u0085", "\\u2028", "\\u2029"):
            self.assertIn(escape, rendered)

    def test_configured_identifier_punctuation_renders_verbatim(self) -> None:
        rendered = self.notice(
            runtime="co-dex",
            model="claude-opus-5@anthropic/ss-1:1m",
            effort="med-high",
        ).render()
        self.assertIn(
            "- Runtime/model: co-dex/claude-opus-5@anthropic/ss-1:1m:med-high\n",
            rendered,
        )

    def test_rendered_message_repeats_only_payload_facts(self) -> None:
        """The compact body contains lifecycle facts, never task or answer prose."""
        rendered = self.notice().render()
        for fact in (AGENT_ID, "succeeded", "ntf_abc", "agent-run/completion", "v1"):
            self.assertIn(fact, rendered)
        self.assertIn("- Notice:", rendered)
        # Every token of the message comes from the payload or the fixed text.
        for value in ("task", "answer", "traceback"):
            self.assertNotIn(value, rendered.lower())

    def test_untrusted_or_nonterminal_values_are_refused(self) -> None:
        for status in (AgentStatus.RUNNING, AgentStatus.CREATED, "succeeded"):
            with self.assertRaises(ValidationError):
                self.notice(status=status)
        for notification_id in ("", "   ", "n" * 513, 7):
            with self.assertRaises(ValidationError):
                self.notice(notification_id=notification_id)
        with self.assertRaises(ValidationError):
            self.notice(agent_id="not-an-agent")
        for version in (0, 2, True, "1"):
            with self.assertRaises(ValidationError):
                self.notice(version=version)

    def test_metadata_must_be_bounded_strings_or_none(self) -> None:
        """Check each invalid field before the outer subtest handles exceptions."""
        for name in ("runtime", "model", "effort"):
            self.assertIsNone(getattr(self.notice(**{name: None}), name))
            for invalid in ("", "   ", "x" * (MAX_METADATA_LENGTH + 1), 7, True, [], {}):
                with (
                    self.subTest(name=name, value=invalid),
                    self.assertRaises(ValidationError),
                ):
                    self.notice(**{name: invalid})

    def test_receipt_validates_its_optional_remote_id(self) -> None:
        self.assertEqual(DeliveryReceipt().remote_message_id, None)
        self.assertFalse(DeliveryReceipt().ambiguous)
        self.assertEqual(DeliveryReceipt("remote-1", True).remote_message_id, "remote-1")
        with self.assertRaises(ValidationError):
            DeliveryReceipt(remote_message_id="")
        with self.assertRaises(ValidationError):
            DeliveryReceipt(ambiguous="yes")  # type: ignore[arg-type]


if __name__ == "__main__":
    unittest.main()
