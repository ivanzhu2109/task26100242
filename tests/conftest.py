import os
import sys
import threading

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from failback import Coordinator  # noqa: E402


class RecordingDevice:
    """Thread-safe device action recorder with controllable gates."""

    def __init__(self):
        self.lock = threading.Lock()
        self.calls = []  # (op, action_id)
        self.recover_gates = {}
        self.failback_gates = {}
        self.fail_recover = set()
        self.recover_delay = 0.0
        self.failback_delay = 0.0

    def _wait(self, gates, action_id):
        gate = gates.get(action_id)
        if gate is not None:
            gate.wait(timeout=60)

    def recover(self, action_id):
        with self.lock:
            self.calls.append(("recover_start", action_id))
        self._wait(self.recover_gates, action_id)
        if self.recover_delay:
            threading.Event().wait(self.recover_delay)
        if action_id in self.fail_recover:
            with self.lock:
                self.calls.append(("recover_fail", action_id))
            raise RuntimeError(f"recover failed: {action_id}")
        with self.lock:
            self.calls.append(("recover_ok", action_id))

    def failback(self, action_id):
        with self.lock:
            self.calls.append(("failback_start", action_id))
        self._wait(self.failback_gates, action_id)
        if self.failback_delay:
            threading.Event().wait(self.failback_delay)
        with self.lock:
            self.calls.append(("failback_ok", action_id))

    def ops(self, op):
        with self.lock:
            return [aid for o, aid in self.calls if o == op]


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "failback.db")


@pytest.fixture
def device():
    return RecordingDevice()


@pytest.fixture
def coordinator(db_path, device):
    c = Coordinator(db_path, device)
    yield c
    c.close()
