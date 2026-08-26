import plistlib
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from agent_run.capacity.launchd import argv, build_job, render_plist
from agent_run.errors import ValidationError


class CapacityLaunchdTests(unittest.TestCase):
    def paths(self):
        return {
            "binary": Path("/opt/agent&run"),
            "stdout_log": Path("/tmp/capacity<out>.log"),
            "stderr_log": Path("/tmp/capacity&err.log"),
        }

    def test_interval_must_be_a_positive_integer(self) -> None:
        for invalid in (True, 1.5, 0, -1):
            with self.subTest(invalid=invalid), self.assertRaises(ValidationError):
                build_job(
                    "com.example.capacity",
                    interval_seconds=invalid,  # type: ignore[arg-type]
                    **self.paths(),
                )

    def test_plist_is_escaped_bounded_and_one_shot(self) -> None:
        job = build_job("com.example.<capacity&>", interval_seconds=60, **self.paths())
        self.assertEqual(
            argv(job),
            ("/opt/agent&run", "capacity", "collect", "--once"),
        )
        rendered = render_plist(job)
        parsed = plistlib.loads(rendered.encode("utf-8"))

        self.assertIn("com.example.&lt;capacity&amp;&gt;", rendered)
        self.assertEqual(parsed["Label"], "com.example.<capacity&>")
        self.assertEqual(parsed["ProgramArguments"], list(argv(job)))
        self.assertEqual(parsed["StartInterval"], 60)
        self.assertIs(parsed["RunAtLoad"], False)
        self.assertNotIn("KeepAlive", parsed)
        self.assertEqual(parsed["StandardOutPath"], "/tmp/capacity<out>.log")
        self.assertEqual(parsed["StandardErrorPath"], "/tmp/capacity&err.log")


if __name__ == "__main__":
    unittest.main()
