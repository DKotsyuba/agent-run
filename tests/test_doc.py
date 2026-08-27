import io
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run import cli
from agent_run.doc import TOPICS, topic_text
from agent_run.errors import ValidationError
from agent_run.mcp import serve


_MAX_BYTES = 8192
_GUIDE_DIR = Path(__file__).resolve().parents[1] / "src" / "agent_run" / "operator_guide"


class DocTopicsTests(unittest.TestCase):
    def test_every_topic_file_exists_loads_and_is_bounded(self):
        for name in ("index", *TOPICS):
            path = _GUIDE_DIR / f"{name}.md"
            self.assertTrue(path.is_file(), f"missing topic file: {name}")
            text = path.read_text(encoding="utf-8")
            self.assertTrue(text.strip())
            self.assertLessEqual(len(text.encode("utf-8")), _MAX_BYTES, name)

    def test_topic_text_defaults_to_index(self):
        self.assertEqual(topic_text(), topic_text("index"))
        self.assertIn("agent-run", topic_text())

    def test_topic_text_loads_every_declared_topic(self):
        for name in TOPICS:
            self.assertTrue(topic_text(name).strip())

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

        self.assertEqual(serve(_NoService(), source, output), 0)
        return [json.loads(line) for line in output.getvalue().splitlines()]

    def test_doc_tool_is_listed(self):
        responses = self.run_server([{"jsonrpc": "2.0", "id": 1, "method": "tools/list"}])
        names = [tool["name"] for tool in responses[0]["result"]["tools"]]
        self.assertEqual(len(names), 11)
        self.assertEqual(names[-1], "doc")

    def test_doc_tool_call_returns_index_and_topic(self):
        responses = self.run_server(
            [
                {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "doc", "arguments": {}}},
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "tools/call",
                    "params": {"name": "doc", "arguments": {"topic": "service"}},
                },
            ]
        )
        first = responses[0]["result"]["structuredContent"]
        second = responses[1]["result"]["structuredContent"]
        self.assertEqual(first["topic"], "index")
        self.assertEqual(second["topic"], "service")
        self.assertIn("opencode", second["text"])

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
