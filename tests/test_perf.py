"""Performance acceptance: 200k terminal history rows, 8 instances,
1:1:1 mix of register / failback-request / single-action query for 10s,
>= 300 calls/s in total, and no missed failback afterwards."""

import random
import sqlite3
import threading
import time

import pytest

from failback import Coordinator, RequestState
from failback.store import SCHEMA

from conftest import RecordingDevice

HISTORY_TOTAL = 200_000
DURATION_S = 10.0
MIN_CALLS_PER_S = 300.0


def seed_history(db_path):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode = WAL")
    conn.executescript(SCHEMA)
    rows = []
    for i in range(HISTORY_TOTAL):
        if i % 4 == 0:
            rows.append((f"hist-rev-{i}", "hist-exec", "SUCCEEDED", 1, "DONE"))
        elif i % 4 == 1:
            rows.append((f"hist-fail-{i}", "hist-exec", "FAILED", 0, "NONE"))
        else:
            rows.append((f"hist-ok-{i}", "hist-exec", "SUCCEEDED", 0, "NONE"))
    with conn:
        conn.executemany(
            "INSERT INTO actions (action_id, execution_id, recover_state,"
            " revoked, failback_state) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
    conn.close()


class NullDevice:
    def recover(self, action_id):
        pass

    def failback(self, action_id):
        pass


@pytest.mark.perf
def test_mixed_workload_throughput(tmp_path):
    db_path = str(tmp_path / "perf.db")
    seed_history(db_path)

    coords = [Coordinator(db_path, NullDevice(), poll_interval=0.01) for _ in range(8)]
    counters = [0] * 8
    stop = threading.Event()
    rng = random.Random(42)

    def worker(idx):
        coord = coords[idx]
        n = 0
        local_rng = random.Random(idx)
        while not stop.is_set():
            op = n % 3
            if op == 0:
                coord.register_action(f"live-{idx}-{n}", f"exec-{idx}")
            elif op == 1:
                i = local_rng.randrange(HISTORY_TOTAL // 4) * 4 + 2
                target = f"hist-ok-{i}"
                coord.request_failback(target)
            else:
                coord.query_action(f"hist-ok-{local_rng.randrange(HISTORY_TOTAL // 4) * 4 + 2}")
            n += 1
        counters[idx] = n

    threads = [
        threading.Thread(target=worker, args=(i,)) for i in range(8)
    ]
    start = time.monotonic()
    for t in threads:
        t.start()
    stop.wait(DURATION_S)
    stop.set()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - start

    total = sum(counters)
    rate = total / elapsed
    print(f"\n{total} calls in {elapsed:.1f}s -> {rate:.0f} calls/s")
    assert rate >= MIN_CALLS_PER_S, f"{rate:.0f} calls/s < {MIN_CALLS_PER_S}"

    for c in coords:
        assert c.wait_idle(120), "failback work did not drain"

    conn = sqlite3.connect(db_path)
    missed = conn.execute(
        "SELECT COUNT(*) FROM actions"
        " WHERE revoked = 1 AND recover_state = 'SUCCEEDED'"
        " AND failback_state != 'DONE'"
    ).fetchone()[0]
    open_reqs = conn.execute(
        "SELECT COUNT(*) FROM requests WHERE state = ?",
        (RequestState.ACCEPTED.value,),
    ).fetchone()[0]
    conn.close()
    assert missed == 0, f"{missed} actions missed their failback"
    assert open_reqs == 0, f"{open_reqs} requests never completed"

    for c in coords:
        c.close()


@pytest.mark.perf
def test_unrelated_calls_progress_during_stuck_device_action(db_path):
    """A device action stuck for ~1s must not stall unrelated calls;
    95% of calls return within 100ms."""
    device = RecordingDevice()
    stuck = threading.Event()
    device.recover_gates["stuck"] = stuck
    coord = Coordinator(db_path, device, poll_interval=0.01)
    try:
        assert coord.register_action("stuck", "exec0").accepted
        time.sleep(0.05)  # let the stuck recovery actually start

        latencies = []
        deadline = time.monotonic() + 1.0
        n = 0
        while time.monotonic() < deadline:
            t0 = time.monotonic()
            coord.register_action(f"free-{n}", "exec1")
            coord.query_action("stuck")
            coord.request_failback(f"free-{n}")
            latencies.append(time.monotonic() - t0)
            n += 1
        stuck.set()
        assert coord.wait_idle(10)

        latencies.sort()
        p95 = latencies[int(len(latencies) * 0.95)]
        print(f"\n{len(latencies)} call-groups, p95={p95 * 1000:.1f}ms")
        assert p95 < 0.1, f"p95 {p95 * 1000:.1f}ms >= 100ms"
    finally:
        stuck.set()
        coord.close()
