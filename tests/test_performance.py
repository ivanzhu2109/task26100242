"""性能用例：20 万终态历史 + 8 实例 1:1:1 混合负载 10 秒 >= 300 调用/秒。"""
import random
import sqlite3
import threading
import time

import pytest

from failback import FailbackCoordinator, RequestStatus
from failback.db import SCHEMA

HISTORY_ROWS = 200_000
DURATION_S = 10.0
MIN_QPS = 300.0
N_INSTANCES = 8


def _seed_history(db):
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    now = time.time()
    batch = []
    for i in range(HISTORY_ROWS):
        batch.append(
            ("hist-%d" % i, "exec-hist", "owner-hist",
             "FAILBACK_DONE", 1, 1, 1, now)
        )
        if len(batch) == 5000:
            conn.executemany("INSERT INTO actions VALUES (?,?,?,?,?,?,?,?)", batch)
            conn.commit()
            batch.clear()
    if batch:
        conn.executemany("INSERT INTO actions VALUES (?,?,?,?,?,?,?,?)", batch)
        conn.commit()
    conn.close()


@pytest.mark.slow
def test_mixed_workload_throughput(tmp_path):
    db = str(tmp_path / "perf.db")
    _seed_history(db)

    coords = [FailbackCoordinator(db, instance_id="perf-%d" % i)
              for i in range(N_INSTANCES)]
    try:
        # 种子根动作（立即完成的设备动作）
        roots = ["root-%d" % i for i in range(16)]
        for r in roots:
            coords[0].register_action(r, "exec-root")
        deadline = time.time() + 10
        while time.time() < deadline:
            if all(coords[0].query_action(r).status.value == "RECOVERED"
                   for r in roots):
                break
            time.sleep(0.01)

        known_ids = list(roots)
        known_lock = threading.Lock()
        counters = {"calls": 0, "register": 0, "failback": 0, "query": 0}
        count_lock = threading.Lock()
        stop = threading.Event()

        def bump(op):
            with count_lock:
                counters["calls"] += 1
                counters[op] += 1

        def worker(idx):
            inst = coords[idx]
            rnd = random.Random(idx)
            seq = 0
            op = 0
            while not stop.is_set():
                try:
                    if op == 0:
                        aid = "w%d-%d" % (idx, seq)
                        seq += 1
                        prereqs = []
                        with known_lock:
                            pool = list(known_ids)
                        if rnd.random() < 0.5 and pool:
                            prereqs = [rnd.choice(pool)]
                        try:
                            inst.register_action(aid, "exec-w%d" % idx,
                                                 prerequisites=prereqs)
                            with known_lock:
                                known_ids.append(aid)
                        except Exception:
                            pass  # 前提被撤销等拒绝也计为一次完成的调用
                        bump("register")
                    elif op == 1:
                        with known_lock:
                            target = rnd.choice(known_ids)
                        try:
                            inst.request_failback([target])
                        except Exception:
                            pass
                        bump("failback")
                    else:
                        with known_lock:
                            target = rnd.choice(known_ids)
                        try:
                            inst.query_action(target)
                        except Exception:
                            pass
                        bump("query")
                finally:
                    op = (op + 1) % 3

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(N_INSTANCES)]
        start = time.monotonic()
        for t in threads:
            t.start()
        time.sleep(DURATION_S)
        stop.set()
        for t in threads:
            t.join()
        elapsed = time.monotonic() - start

        qps = counters["calls"] / elapsed
        print("\nperf: calls=%d elapsed=%.1fs qps=%.1f mix=%s" % (
            counters["calls"], elapsed, qps,
            {k: v for k, v in counters.items() if k != "calls"}))
        assert qps >= MIN_QPS, "qps %.1f < %.1f" % (qps, MIN_QPS)

        # 结束后不得遗留应执行而未执行的回切（实例存活时等待排空）
        deadline = time.time() + 120
        pending_actions = open_requests = -1
        while time.time() < deadline:
            conn = sqlite3.connect(db)
            pending_actions = conn.execute(
                "SELECT COUNT(*) FROM actions "
                "WHERE failback_requested=1 "
                "AND status NOT IN ('RECOVER_FAILED','FAILBACK_DONE')"
            ).fetchone()[0]
            open_requests = conn.execute(
                "SELECT COUNT(*) FROM requests WHERE status='ACCEPTED'"
            ).fetchone()[0]
            conn.close()
            if pending_actions == 0 and open_requests == 0:
                break
            time.sleep(0.2)
        print("drain: pending_actions=%d open_requests=%d"
              % (pending_actions, open_requests))
        assert pending_actions == 0, "遗留 %d 个未完成的回切" % pending_actions
        assert open_requests == 0, "遗留 %d 个未完成的请求" % open_requests
    finally:
        for c in coords:
            c.close()
