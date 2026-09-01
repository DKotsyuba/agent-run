import os
import threading
import time
import unittest

from agent_run.launch import ChildReaper


class ChildReaperTests(unittest.TestCase):
    def test_reaps_registered_short_lived_child_without_stealing_other_waits(self) -> None:
        report: list[tuple[int, int]] = []
        finished = threading.Event()
        reaper = ChildReaper()
        self.addCleanup(reaper.close)

        child_pid = os.fork()
        if child_pid == 0:
            os._exit(7)
        unrelated_pid = os.fork()
        if unrelated_pid == 0:
            os._exit(9)

        def callback(pid: int, status: int) -> None:
            report.append((pid, status))
            finished.set()

        reaper.register(child_pid, callback)
        deadline = time.monotonic() + 5
        self.assertTrue(finished.wait(5))
        self.assertLess(time.monotonic(), deadline)
        self.assertEqual(report[0][0], child_pid)
        self.assertTrue(os.WIFEXITED(report[0][1]))
        self.assertEqual(os.WEXITSTATUS(report[0][1]), 7)
        with self.assertRaises(ChildProcessError):
            os.waitpid(child_pid, os.WNOHANG)
        waited, status = os.waitpid(unrelated_pid, 0)
        self.assertEqual(waited, unrelated_pid)
        self.assertTrue(os.WIFEXITED(status))
        self.assertEqual(os.WEXITSTATUS(status), 9)


if __name__ == "__main__":
    unittest.main()
