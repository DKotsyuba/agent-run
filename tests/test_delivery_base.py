import dataclasses
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.domain import AgentStatus
from agent_run.errors import ValidationError
from agent_run.delivery.base import (
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

    def test_notice_carries_only_the_four_trusted_fields(self) -> None:
        names = [field.name for field in dataclasses.fields(CompletionNotice)]
        self.assertEqual(
            sorted(names), ["agent_id", "notification_id", "status", "version"]
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
        with self.assertRaises(dataclasses.FrozenInstanceError):
            notice.status = AgentStatus.FAILED  # type: ignore[misc]

    def test_rendered_message_repeats_only_payload_facts(self) -> None:
        rendered = self.notice().render()
        for fact in (AGENT_ID, "succeeded", "ntf_abc", "summary", "transcript"):
            self.assertIn(fact, rendered)
        self.assertIn("Do not start a replacement agent", rendered)
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
