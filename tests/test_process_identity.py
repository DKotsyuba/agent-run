"""Unit tests for PID birth-time process identity observations."""

from unittest import TestCase
from unittest.mock import patch

import psutil

from agent_run.process_identity import ProcessState, observe_process


class ProcessIdentityTests(TestCase):
    """Keep process identity distinctions independent of live host processes."""

    def test_observations_distinguish_proof_states(self):
        """Unavailable proof, same birth, reuse, denial, and absence remain distinct."""
        process = type("Process", (), {"create_time": lambda self: 12.5})()
        with patch("agent_run.process_identity.psutil.Process", return_value=process):
            self.assertEqual(observe_process(7, None).state, ProcessState.UNKNOWN)
            self.assertEqual(observe_process(7, 12.5).state, ProcessState.ALIVE)
            self.assertEqual(observe_process(7, 11.5).state, ProcessState.REUSED)
        with patch("agent_run.process_identity.psutil.Process", side_effect=psutil.AccessDenied(7)):
            self.assertEqual(observe_process(7, 12.5).state, ProcessState.DENIED)
        with patch("agent_run.process_identity.psutil.Process", side_effect=psutil.NoSuchProcess(7)):
            self.assertEqual(observe_process(7, 12.5).state, ProcessState.DEAD)
