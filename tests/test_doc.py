import io
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run import cli
from agent_run.doc import TOPICS, topic_text
from agent_run.errors import ValidationError
from agent_run.dispatch import Session, call_tool
from agent_run.mcp import serve


class _Broker:
    def __init__(self, service):
        self.service = service
        self.session = Session()

    def call(self, method, params=None, timeout=600):
        return call_tool(self.service, method, params or {}, self.session)


_MAX_BYTES = 8192


class DocTopicsTests(unittest.TestCase):
    def test_every_topic_loads_and_is_bounded(self):
        """Load file-backed and generated topics through their real shared API."""
        for name in ("index", *TOPICS):
            text = topic_text(name)
            self.assertTrue(text.strip())
            self.assertLessEqual(len(text.encode("utf-8")), _MAX_BYTES, name)

    def test_topic_text_defaults_to_index(self):
        self.assertEqual(topic_text(), topic_text("index"))
        self.assertIn("agent-run", topic_text())

    def test_topic_text_loads_every_declared_topic(self):
        for name in TOPICS:
            self.assertTrue(topic_text(name).strip())

    def test_topic_text_completion_is_contract_template(self):
        """Completion is discoverable and serves its shared format."""
        self.assertIn("completion", TOPICS)
        text = topic_text("completion")
        self.assertIn("agent-run/completion", text)
        self.assertIn("- Notice:", text)

    def test_topic_text_rejects_unknown_topic(self):
        with self.assertRaises(ValidationError):
            topic_text("not-a-real-topic")


class DocCliTests(unittest.TestCase):
    def run_cli(self, args):
        stdout = io.StringIO()
        stderr = io.StringIO()
        code = cli.main(args, stdin=io.StringIO(), stdout=stdout, stderr=stderr)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_doc_with_no_topic_returns_index(self):
        code, output, error = self.run_cli(["doc"])
        self.assertEqual((code, error), (0, ""))
        payload = json.loads(output)
        self.assertEqual(payload["topic"], "index")
        self.assertIn("agent-run", payload["text"])

    def test_doc_with_topic_returns_that_topic(self):
        code, output, error = self.run_cli(["doc", "config"])
        self.assertEqual((code, error), (0, ""))
        payload = json.loads(output)
        self.assertEqual(payload["topic"], "config")
        self.assertIn("config.toml", payload["text"])

    def test_doc_with_completion_topic_returns_contract_text(self):
        """CLI readers receive the generated completion contract as a doc topic."""
        code, output, error = self.run_cli(["doc", "completion"])
        self.assertEqual((code, error), (0, ""))
        payload = json.loads(output)
        self.assertEqual(payload["topic"], "completion")
        self.assertIn("agent-run/completion", payload["text"])

    def test_doc_with_unknown_topic_is_refused(self):
        code, output, error = self.run_cli(["doc", "not-a-real-topic"])
        self.assertEqual((code, output), (2, ""))
        self.assertEqual(json.loads(error)["error"]["type"], "ValidationError")


class DocMcpTests(unittest.TestCase):
    def run_server(self, lines):
        source = io.StringIO("".join(json.dumps(line) + "\n" for line in lines))
        output = io.StringIO()

        class _NoService:
            pass

        self.assertEqual(serve(_Broker(_NoService()), source, output), 0)
        return [json.loads(line) for line in output.getvalue().splitlines()]

    def test_doc_tool_is_listed(self):
        """Expose the documentation tool among all nineteen shared tools."""

        responses = self.run_server([{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}])
        names = [tool["name"] for tool in responses[0]["result"]["tools"]]
        self.assertEqual(len(names), 19)
        self.assertIn("doc", names)

    def test_doc_tool_call_returns_index_and_topic(self):
        """MCP serves the index, Markdown topics and generated contract uniformly."""
        responses = self.run_server(
            [
                {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "doc", "arguments": {}}},
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": "doc", "arguments": {"topic": "service"}},
                },
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "tools/call",
                    "params": {"name": "doc", "arguments": {"topic": "completion"}},
                },
            ]
        )
        first = responses[0]["result"]["structuredContent"]
        second = responses[1]["result"]["structuredContent"]
        third = responses[2]["result"]["structuredContent"]
        self.assertEqual(first["topic"], "index")
        self.assertEqual(second["topic"], "service")
        self.assertIn("opencode", second["text"])
        self.assertEqual(third["topic"], "completion")
        self.assertIn("agent-run/completion", third["text"])

    def test_doc_tool_call_rejects_unknown_topic(self):
        responses = self.run_server(
            [
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "tools/call",
                    "params": {"name": "doc", "arguments": {"topic": "not-a-real-topic"}},
                }
            ]
        )
        self.assertTrue(responses[0]["result"]["isError"])


if __name__ == "__main__":
    unittest.main()
