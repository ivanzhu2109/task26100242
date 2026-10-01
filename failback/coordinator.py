"""Public coordination API.

A :class:`Coordinator` is one library instance.  Many instances (across
threads or processes) may share the same SQLite file; device actions run
on background threads so slow device operations never block API calls.
"""

from __future__ import annotations

import concurrent.futures
import threading
import time
import uuid
from typing import Optional, Sequence

from .models import (
    ActionSnapshot,
    DeviceActions,
    RegisterResult,
    RequestSnapshot,
)
from .store import Store

MAX_PREREQUISITES = 8


class Coordinator:
    """Coordinates registration, failback requests and status queries."""

    def __init__(
        self,
        db_path: str,
        device_actions: DeviceActions,
        *,
        max_workers: int = 64,
        poll_interval: float = 0.02,
    ) -> None:
        self._store = Store(db_path)
        self._device = device_actions
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="failback-device"
        )
        self._poll_interval = poll_interval
        self._stopped = threading.Event()
        self._scheduler = threading.Thread(
            target=self._schedule_loop, name="failback-scheduler", daemon=True
        )
        self._scheduler.start()

    # -- API ----------------------------------------------------------------

    def register_action(
        self,
        action_id: str,
        execution_id: str,
        prerequisites: Sequence[str] = (),
    ) -> RegisterResult:
        """Register a recovery action and, if accepted, start recovery."""
        prereqs = tuple(prerequisites)
        if len(prereqs) > MAX_PREREQUISITES:
            return RegisterResult(
                False, action_id, f"at most {MAX_PREREQUISITES} prerequisites"
            )
        if len(set(prereqs)) != len(prereqs):
            return RegisterResult(False, action_id, "duplicate prerequisites")
        result = self._store.try_register(action_id, execution_id, prereqs)
        if result.accepted:
            self._executor.submit(self._run_recover, action_id)
        return result

    def request_failback(self, target_action_id: str, request_id: Optional[str] = None) -> str:
        """Accept a failback request for a target and its dependents."""
        if request_id is None:
            request_id = uuid.uuid4().hex
        self._store.accept_failback(request_id, target_action_id)
        self._pump()
        return request_id

    def query_action(self, action_id: str) -> Optional[ActionSnapshot]:
        return self._store.get_action(action_id)

    def query_request(self, request_id: str) -> Optional[RequestSnapshot]:
        return self._store.get_request(request_id)

    def wait_idle(self, timeout: Optional[float] = None) -> bool:
        """Block until no recover/failback is running locally or in the DB."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self._store.count_unfinished() == 0:
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            time.sleep(self._poll_interval)

    def close(self) -> None:
        self._stopped.set()
        self._scheduler.join(timeout=5)
        self._executor.shutdown(wait=True)
        self._store.close()

    # -- internals ----------------------------------------------------------

    def _run_recover(self, action_id: str) -> None:
        try:
            self._device.recover(action_id)
            succeeded = True
        except Exception:
            succeeded = False
        self._store.finish_recover(action_id, succeeded)
        self._pump()

    def _run_failback(self, action_id: str) -> None:
        try:
            self._device.failback(action_id)
            succeeded = True
        except Exception:
            succeeded = False
        self._store.finish_failback(action_id, succeeded)
        self._pump()

    def _pump(self) -> None:
        """Claim ready failbacks and retire completed requests."""
        for action_id in self._store.claim_failback_ready():
            self._executor.submit(self._run_failback, action_id)
        self._store.complete_requests()

    def _schedule_loop(self) -> None:
        while not self._stopped.is_set():
            try:
                self._pump()
            except Exception:
                pass
            self._stopped.wait(self._poll_interval)
