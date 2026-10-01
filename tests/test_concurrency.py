"""验收场景：多实例并发登记/撤销/查询。"""
import random
import sqlite3
import threading
import time

import pytest

from failback import (
    ActionStatus,
    FailbackCoordinator,
    RegistrationRejected,
    RequestStatus,
)


def wait_until(pred, timeout=10.0, interval=0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


class DeviceLog:
    """线程安全的设备动作记录。"""

    def __init__(self):
        self._lock = threading.Lock()
        self.events = []  # (kind, action_id, timestamp)

    def record(self, kind, action_id):
        with self._lock:
            self.events.append((kind, action_id, time.monotonic()))

    def count(self, kind, action_id):
        with self._lock:
            return sum(1 for k, a, _ in self.events if k == kind and a == action_id)

    def time_of(self, kind, action_id):
        with self._lock:
            for k, a, t in self.events:
                if k == kind and a == action_id:
                    return t
        return None


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "shared.db")


@pytest.fixture
def make_instances(db_path):
    pool = []

    def make(n):
        cs = [FailbackCoordinator(db_path) for _ in range(n)]
        pool.extend(cs)
        return cs

    yield make
    for c in pool:
        c.close()


# ---- 场景 1：多实例竞争登记依赖与撤销前提 ----


def test_race_register_vs_revoke(db_path, make_instances):
    (owner,) = make_instances(1)
    log = DeviceLog()
    owner.register_action(
        "A", "exec-root",
        recover=lambda: log.record("rec", "A"),
        failback=lambda: log.record("fb", "A"),
    )
    assert wait_until(
        lambda: owner.query_action("A").status == ActionStatus.RECOVERED
    )

    workers = make_instances(4)
    n_threads, per_thread = 8, 15
    accepted, rejected = [], []
    result_lock = threading.Lock()
    barrier = threading.Barrier(n_threads + 1)

    def register_loop(inst, tag):
        barrier.wait()
        for i in range(per_thread):
            aid = "act-%s-%d" % (tag, i)
            try:
                inst.register_action(
                    aid, "exec-%s" % tag, prerequisites=["A"],
                    recover=lambda a=aid: log.record("rec", a),
                    failback=lambda a=aid: log.record("fb", a),
                )
                with result_lock:
                    accepted.append(aid)
            except RegistrationRejected:
                with result_lock:
                    rejected.append(aid)
            time.sleep(random.uniform(0, 0.003))

    threads = [
        threading.Thread(target=register_loop, args=(workers[t % 4], t))
        for t in range(n_threads)
    ]
    for t in threads:
        t.start()
    barrier.wait()
    time.sleep(random.uniform(0.005, 0.03))
    rid = owner.request_failback(["A"])
    for t in threads:
        t.join()

    assert wait_until(
        lambda: owner.query_request(rid).status == RequestStatus.COMPLETED,
        timeout=30,
    )

    # 登记被拒绝的动作不能启动恢复
    for aid in rejected:
        assert log.count("rec", aid) == 0
    # 登记成功的动作恢复恰好一次，且最终全部完成回切
    for aid in accepted:
        assert log.count("rec", aid) == 1
        assert log.count("fb", aid) == 1
        assert owner.query_action(aid).status == ActionStatus.FAILBACK_DONE

    # 实际撤销集合与完整依赖记录算出的范围一致
    conn = sqlite3.connect(db_path)
    closure = {row[0] for row in conn.execute(
        "WITH RECURSIVE c(aid) AS ("
        "  SELECT 'A' UNION"
        "  SELECT d.action_id FROM action_deps d JOIN c ON d.prereq_id=c.aid"
        ") SELECT aid FROM c"
    )}
    scope = {row[0] for row in conn.execute(
        "SELECT action_id FROM request_scope WHERE request_id=?", (rid,)
    )}
    conn.close()
    assert scope == closure == set(accepted) | {"A"}


# ---- 场景 2：并发撤销 B、C，D 恢复未结束随后成功 ----


def test_diamond_concurrent_revoke(db_path, make_instances):
    c1, c2 = make_instances(2)
    log = DeviceLog()
    d_gate = threading.Event()

    def rec(a):
        return lambda: log.record("rec", a)

    def fb(a):
        return lambda: (log.record("fb_start", a), log.record("fb_end", a))

    c1.register_action("A", "exec1", recover=rec("A"), failback=fb("A"))
    assert wait_until(
        lambda: c1.query_action("A").status == ActionStatus.RECOVERED
    )
    c1.register_action("B", "exec1", prerequisites=["A"],
                       recover=rec("B"), failback=fb("B"))
    c2.register_action("C", "exec2", prerequisites=["A"],
                       recover=rec("C"), failback=fb("C"))
    assert wait_until(
        lambda: c1.query_action("B").status == ActionStatus.RECOVERED
        and c2.query_action("C").status == ActionStatus.RECOVERED
    )

    # 无关执行：在撤销进行期间仍能完成恢复
    unrelated_done = threading.Event()

    def unrelated_rec():
        time.sleep(0.2)
        log.record("rec", "U")
        unrelated_done.set()

    c2.register_action("U", "exec-unrelated", recover=unrelated_rec)

    # D 依赖 B 和 C，恢复长时间不结束
    def d_recover():
        log.record("rec", "D")
        d_gate.wait(10)

    c1.register_action("D", "exec1", prerequisites=["B", "C"],
                       recover=d_recover, failback=fb("D"))
    assert wait_until(lambda: log.count("rec", "D") == 1)

    # 两个实例并发撤销 B 和 C
    results = {}

    def revoke(inst, target, key):
        results[key] = inst.request_failback([target])

    t1 = threading.Thread(target=revoke, args=(c1, "B", "rb"))
    t2 = threading.Thread(target=revoke, args=(c2, "C", "rc"))
    t1.start(); t2.start(); t1.join(); t2.join()
    rid_b, rid_c = results["rb"], results["rc"]

    # 受理后 D 仍在恢复：请求不能完成，B/C/D 都不能回切
    time.sleep(0.3)
    assert c1.query_request(rid_b).status == RequestStatus.ACCEPTED
    assert c2.query_request(rid_c).status == RequestStatus.ACCEPTED
    assert log.count("fb_start", "D") == 0
    assert log.count("fb_start", "B") == 0
    assert log.count("fb_start", "C") == 0

    # 无关执行在撤销期间完成恢复
    assert unrelated_done.wait(5)
    assert wait_until(
        lambda: c2.query_action("U").status == ActionStatus.RECOVERED
    )

    # D 随后恢复成功
    d_gate.set()
    assert wait_until(
        lambda: c1.query_request(rid_b).status == RequestStatus.COMPLETED
        and c2.query_request(rid_c).status == RequestStatus.COMPLETED,
        timeout=15,
    )

    # D 恰好回切一次，且先于 B、C；A 不回切
    assert log.count("fb_start", "D") == 1
    assert log.count("fb_start", "B") == 1
    assert log.count("fb_start", "C") == 1
    assert log.count("fb_start", "A") == 0
    assert log.time_of("fb_end", "D") <= log.time_of("fb_start", "B")
    assert log.time_of("fb_end", "D") <= log.time_of("fb_start", "C")
    assert c1.query_action("A").status == ActionStatus.RECOVERED
    for aid in ("B", "C", "D"):
        assert c1.query_action(aid).status == ActionStatus.FAILBACK_DONE


# ---- 场景 3：多实例反复提交重叠回切并查询 ----


def test_overlapping_revocations_with_queries(db_path, make_instances):
    instances = make_instances(4)
    c0 = instances[0]
    log = DeviceLog()

    # 构造跨执行的 DAG：A；B、C 依赖 A；D 依赖 B、C；E 依赖 D；F 依赖 D
    def rec(a):
        return lambda: log.record("rec", a)

    def fb(a):
        def run():
            log.record("fb_start", a)
            time.sleep(random.uniform(0, 0.005))
            log.record("fb_end", a)
        return run

    topo = [
        ("A", "exec1", []),
        ("B", "exec1", ["A"]),
        ("C", "exec2", ["A"]),
        ("D", "exec2", ["B", "C"]),
        ("E", "exec3", ["D"]),
        ("F", "exec3", ["D"]),
    ]
    for aid, ex, deps in topo:
        c0.register_action(aid, ex, prerequisites=deps,
                           recover=rec(aid), failback=fb(aid))
        assert wait_until(
            lambda a=aid: c0.query_action(a).status == ActionStatus.RECOVERED
        )

    stop_queries = threading.Event()
    violations = []

    def query_loop(inst):
        # 查询不得读到违反顺序约束的混合视图：
        # 若前提已回切完成，则其所有已接纳依赖必须已终态。
        while not stop_queries.is_set():
            for aid, _, _ in topo:
                try:
                    view = inst.query_action(aid)
                except Exception as exc:  # noqa: BLE001
                    violations.append("query error: %r" % exc)
                    continue
                if view.status == ActionStatus.FAILBACK_DONE:
                    for dep, _, deps in topo:
                        if aid in deps:
                            st = inst.query_action(dep).status
                            if st not in (
                                ActionStatus.RECOVER_FAILED,
                                ActionStatus.FAILBACK_DONE,
                            ):
                                violations.append(
                                    "prereq %s done before dependent %s (%s)"
                                    % (aid, dep, st)
                                )

    query_threads = [
        threading.Thread(target=query_loop, args=(instances[i % 4],))
        for i in range(3)
    ]
    for t in query_threads:
        t.start()

    # 多实例反复提交重叠回切
    request_ids = []
    req_lock = threading.Lock()

    def revoke_loop(inst, targets, tag):
        for i in range(3):
            for target in targets:
                rid = inst.request_failback(
                    [target], request_id="req-%s-%s-%d" % (tag, target, i)
                )
                with req_lock:
                    request_ids.append(rid)
            time.sleep(random.uniform(0, 0.01))

    revoke_threads = [
        threading.Thread(target=revoke_loop, args=(instances[1], ["B", "D"], "i1")),
        threading.Thread(target=revoke_loop, args=(instances[2], ["C", "D"], "i2")),
        threading.Thread(target=revoke_loop, args=(instances[3], ["B", "C"], "i3")),
    ]
    for t in revoke_threads:
        t.start()
    for t in revoke_threads:
        t.join()

    assert wait_until(
        lambda: all(
            c0.query_request(r).status == RequestStatus.COMPLETED
            for r in request_ids
        ),
        timeout=30,
    )
    stop_queries.set()
    for t in query_threads:
        t.join()

    assert violations == []

    # 无漏回切、无重复回切：B/C/D/E/F 各恰好一次，A 未回切
    for aid in ("B", "C", "D", "E", "F"):
        assert log.count("fb_start", aid) == 1, aid
        assert log.count("rec", aid) == 1, aid
    assert log.count("fb_start", "A") == 0

    # 前提不得先于依赖回切：fb_start(前提) >= fb_end(依赖)
    edges = [("B", "A"), ("C", "A"), ("D", "B"), ("D", "C"),
             ("E", "D"), ("F", "D")]
    for dep, prereq in edges:
        dep_end = log.time_of("fb_end", dep)
        pre_start = log.time_of("fb_start", prereq)
        if dep_end is not None and pre_start is not None:
            assert dep_end <= pre_start, (dep, prereq)
