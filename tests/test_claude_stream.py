import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.adapters.claude.stream import StreamDecoder, sanitize_line, terminal_event_data
from agent_run.domain import MessageRole
from agent_run.state.db import json_text


class StreamDecoderTests(unittest.TestCase):
    def test_blank_and_malformed_lines_warn_without_raising(self) -> None:
        decoder = StreamDecoder()
        self.assertEqual(decoder.feed("   ", at=0.0).messages, ())
        result = decoder.feed("not json", at=0.0)
        self.assertEqual(result.warning, "malformed_json_line")
        self.assertEqual(decoder.diagnostic_count, 1)
        result = decoder.feed(json.dumps({"no_type": True}), at=0.0)
        self.assertEqual(result.warning, "missing_type_field")
        self.assertEqual(decoder.diagnostic_count, 2)
        result = decoder.feed(json.dumps({"type": "mystery"}), at=0.0)
        self.assertEqual(result.warning, "unknown_type:mystery")
        self.assertEqual(decoder.diagnostic_count, 3)

    def test_assistant_text_and_tool_use_become_messages(self) -> None:
        decoder = StreamDecoder()
        line = json.dumps(
            {
                "type": "assistant",
                "session_id": "sess-1",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "text", "text": "hello"},
                        {"type": "tool_use", "id": "t1", "name": "Read", "input": {"path": "/tmp/x"}},
                        {"type": "text", "text": "   "},
                    ],
                },
            }
        )
        result = decoder.feed(line, at=1.0)
        self.assertEqual(result.session_id, "sess-1")
        self.assertEqual(decoder.session_id, "sess-1")
        self.assertEqual(len(result.messages), 2)
        self.assertEqual(result.messages[0].role, MessageRole.ASSISTANT)
        self.assertEqual(result.messages[0].content, "hello")
        self.assertEqual(result.messages[1].role, MessageRole.TOOL_CALL)
        self.assertEqual(result.messages[1].name, "Read")
        self.assertIn("/tmp/x", result.messages[1].content)

    def test_tool_result_becomes_message(self) -> None:
        decoder = StreamDecoder()
        line = json.dumps(
            {
                "type": "user",
                "message": {
                    "role": "user",
                    "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "ok"}],
                },
            }
        )
        result = decoder.feed(line, at=1.0)
        self.assertEqual(len(result.messages), 1)
        self.assertEqual(result.messages[0].role, MessageRole.TOOL_RESULT)
        self.assertEqual(result.messages[0].content, "ok")
        self.assertEqual(result.messages[0].name, "t1")

    def test_system_event_redacts_secret_looking_fields(self) -> None:
        decoder = StreamDecoder()
        line = json.dumps({"type": "system", "subtype": "init", "apiKey": "sk-secret", "model": "sonnet"})
        result = decoder.feed(line, at=0.0)
        self.assertEqual(result.event[0], "system")
        self.assertEqual(result.event[1]["apiKey"], "<redacted>")
        self.assertEqual(result.event[1]["model"], "sonnet")

    def test_terminal_line_captures_metadata_and_rejects_duplicates(self) -> None:
        decoder = StreamDecoder()
        line = json.dumps(
            {
                "type": "result",
                "session_id": "sess-1",
                "subtype": "success",
                "is_error": False,
                "result": "done",
                "duration_ms": 120,
                "num_turns": 3,
                "total_cost_usd": 0.02,
                "usage": {"input_tokens": 10, "output_tokens": 20},
            }
        )
        result = decoder.feed(line, at=2.0)
        self.assertIsNotNone(result.terminal)
        self.assertEqual(result.terminal.subtype, "success")
        self.assertFalse(result.terminal.is_error)
        self.assertEqual(result.terminal.total_cost_usd, 0.02)
        self.assertEqual(result.terminal.usage["input_tokens"], 10)
        self.assertEqual(decoder.terminal, result.terminal)

        duplicate = decoder.feed(line, at=3.0)
        self.assertEqual(duplicate.warning, "duplicate_terminal_line")
        self.assertIsNone(duplicate.terminal)
        self.assertEqual(decoder.terminal, result.terminal)

    def test_finalize_distinguishes_no_answer_from_cut_off_answer(self) -> None:
        no_answer = StreamDecoder()
        metadata = no_answer.finalize()
        self.assertEqual(metadata.subtype, "no_answer")
        self.assertTrue(metadata.is_error)

        cut_off = StreamDecoder()
        cut_off.feed(
            json.dumps(
                {"type": "assistant", "message": {"role": "assistant", "content": [{"type": "text", "text": "partial"}]}}
            ),
            at=0.0,
        )
        metadata = cut_off.finalize()
        self.assertEqual(metadata.subtype, "cut_off")

    def test_finalize_after_clean_terminal_returns_same_metadata_and_does_not_recount(self) -> None:
        decoder = StreamDecoder()
        decoder.feed(json.dumps({"type": "result", "subtype": "success", "is_error": False}), at=0.0)
        before = decoder.diagnostic_count
        metadata = decoder.finalize()
        self.assertEqual(metadata.subtype, "success")
        self.assertEqual(decoder.diagnostic_count, before)

    def test_replays_the_captured_double_init_result_cycle_and_settles_on_the_first(self) -> None:
        """Regression for a real claude 2.1.245 canary (shapes embedded, long

        text redacted): the engine emitted a full system/init...result
        cycle, then -- on the very same session id -- a second full
        system/init...result cycle. The decoder must settle on the first
        result/success and discard the second as a duplicate, exactly as it
        would need to for ``ClaudeSession.wait`` to ever report succeeded.
        """

        # Line-for-line shape of the captured 12-line runtime.jsonl, with
        # long/irrelevant fields (thinking signatures, tool lists) redacted.
        lines = [
            {"type": "system", "subtype": "init", "session_id": "sess-live", "model": "claude-sonnet-5"},
            {"type": "rate_limit_event", "session_id": "sess-live"},
            {"type": "system", "subtype": "thinking_tokens", "session_id": "sess-live", "estimated_tokens": 50},
            {"type": "system", "subtype": "thinking_tokens", "session_id": "sess-live", "estimated_tokens": 250},
            {"type": "system", "subtype": "thinking_tokens", "session_id": "sess-live", "estimated_tokens": 318},
            {
                "type": "assistant",
                "session_id": "sess-live",
                "message": {"role": "assistant", "content": [{"type": "thinking", "thinking": "<redacted>"}]},
            },
            {
                "type": "assistant",
                "session_id": "sess-live",
                "message": {"role": "assistant", "content": [{"type": "text", "text": "CANARY_OK"}]},
            },
            {
                "type": "result",
                "session_id": "sess-live",
                "subtype": "success",
                "is_error": False,
                "result": "CANARY_OK",
                "duration_ms": 4376,
                "num_turns": 1,
                "total_cost_usd": 0.136404,
                "usage": {"input_tokens": 2, "output_tokens": 334},
            },
            # Second cycle: the live engine re-emitted a full init-to-result
            # sequence on the SAME session id, unprompted, minutes later.
            {"type": "system", "subtype": "init", "session_id": "sess-live", "model": "claude-sonnet-5"},
            {
                "type": "assistant",
                "session_id": "sess-live",
                "message": {"role": "assistant", "content": [{"type": "text", "text": "CANARY_OK"}]},
            },
            {"type": "rate_limit_event", "session_id": "sess-live"},
            {
                "type": "result",
                "session_id": "sess-live",
                "subtype": "success",
                "is_error": False,
                "result": "CANARY_OK",
                "duration_ms": 1779,
                "num_turns": 1,
                "total_cost_usd": 0.1447108,
                "usage": {"input_tokens": 4, "output_tokens": 9},
            },
        ]
        self.assertEqual(len(lines), 12)
        decoder = StreamDecoder()
        warnings = []
        for index, payload in enumerate(lines):
            result = decoder.feed(json.dumps(payload), at=float(index))
            if result.warning:
                warnings.append(result.warning)
        self.assertIsNotNone(decoder.terminal)
        self.assertEqual(decoder.terminal.subtype, "success")
        self.assertFalse(decoder.terminal.is_error)
        self.assertEqual(decoder.terminal.result_text, "CANARY_OK")
        # First cycle's metadata wins, not the second cycle's.
        self.assertEqual(decoder.terminal.duration_ms, 4376)
        self.assertEqual(decoder.terminal.total_cost_usd, 0.136404)
        self.assertEqual(warnings.count("duplicate_terminal_line"), 1)

    def test_result_with_error_subtype_is_terminal_and_marked_as_error(self) -> None:
        decoder = StreamDecoder()
        line = json.dumps(
            {
                "type": "result",
                "session_id": "sess-1",
                "subtype": "error_during_execution",
                "is_error": True,
                "result": "boom",
            }
        )
        result = decoder.feed(line, at=0.0)
        self.assertIsNotNone(result.terminal)
        self.assertEqual(result.terminal.subtype, "error_during_execution")
        self.assertTrue(result.terminal.is_error)


class SanitizeLineTests(unittest.TestCase):
    def test_literal_secret_is_redacted_even_in_a_malformed_line(self) -> None:
        line = "not json but contains sk-super-secret anyway"
        sanitized = sanitize_line(line, ["sk-super-secret"])
        self.assertNotIn("sk-super-secret", sanitized)
        self.assertIn("<redacted>", sanitized)

    def test_secret_shaped_key_is_redacted_and_text_blocks_are_preserved(self) -> None:
        line = json.dumps(
            {
                "type": "assistant",
                "apiKey": "sk-super-secret",
                "message": {"role": "assistant", "content": [{"type": "text", "text": "hello there"}]},
            }
        )
        sanitized = sanitize_line(line, [])
        payload = json.loads(sanitized)
        self.assertEqual(payload["apiKey"], "<redacted>")
        self.assertEqual(payload["message"]["content"][0]["text"], "hello there")

    def test_literal_secret_inside_well_formed_json_is_also_redacted(self) -> None:
        line = json.dumps({"type": "system", "subtype": "init", "note": "token is sk-super-secret"})
        sanitized = sanitize_line(line, ["sk-super-secret"])
        self.assertNotIn("sk-super-secret", sanitized)

    def test_blank_line_and_empty_secret_pass_through_unchanged(self) -> None:
        self.assertEqual(sanitize_line("   ", ["sk-super-secret"]), "   ")
        self.assertEqual(sanitize_line("hello", [""]), "hello")


class TerminalEventDataTests(unittest.TestCase):
    def test_bounded_event_excludes_result_text_and_session_id(self) -> None:
        decoder = StreamDecoder()
        line = json.dumps(
            {
                "type": "result",
                "session_id": "sess-1",
                "subtype": "success",
                "is_error": False,
                "result": "the full answer text",
                "duration_ms": 5,
                "num_turns": 1,
                "total_cost_usd": 0.01,
                "usage": {"input_tokens": 1},
            }
        )
        result = decoder.feed(line, at=0.0)
        event = terminal_event_data(result.terminal)
        self.assertNotIn("result_text", event)
        self.assertNotIn("runtime_session_id", event)
        self.assertNotIn("the full answer text", " ".join(f"{key}={value}" for key, value in event.items()))
        self.assertEqual(event["subtype"], "success")
        self.assertEqual(event["duration_ms"], 5)

    def test_event_with_usage_is_actually_json_serializable(self) -> None:
        """Regression: ``usage`` must not be a nested MappingProxyType.

        ``StoreEventSink.event`` only converts the *outer* mapping with
        ``dict(data)`` before persisting; a MappingProxyType nested one level
        deeper (previously produced by ``_safe_event_data``) survives that
        shallow conversion and blows up ``json_text`` -- masking a real
        outcome as ``supervision_failed`` on the terminal commit.
        """

        decoder = StreamDecoder()
        line = json.dumps(
            {
                "type": "result",
                "subtype": "success",
                "is_error": False,
                "result": "done",
                "usage": {"input_tokens": 10, "output_tokens": 20},
            }
        )
        result = decoder.feed(line, at=0.0)
        event = dict(terminal_event_data(result.terminal))  # mirrors StoreEventSink.event
        json_text(event)  # must not raise ValidationError("value must be JSON serializable")


if __name__ == "__main__":
    unittest.main()
