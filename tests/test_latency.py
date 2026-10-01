"""长动作不阻塞无关联调用：95% 的调用在 100ms 内返回。"""
import statistics
import threading
import time

from failback import ActionStatus, FailbackCoordinator, RequestStatus


def wait_until(pred, timeout=10.0, interval=0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if pred():
            return True
        time.sleep(interval)
    return False


def test_long_actions_do_not_block_unrelated_calls(tmp_path):
    db = str(tmp_path / "lat.db")
    c1 = FailbackCoordinator(db, instance_id="lat-1")
    c2 = FailbackCoordinator(db, instance_id="lat-2")
    try:
        # 一个持续 2 秒的恢复动作
        slow_rec_started = threading.Event()

        def slow_recover():
            slow_rec_started.set()
            time.sleep(2.0)

        c1.register_action("slow-rec", "exec-slow", recover=slow_recover)
        assert slow_rec_started.wait(5)

        # 一个恢复成功但回切持续 2 秒的动作
        slow_fb_started = threading.Event()

        def slow_failback():
            slow_fb_started.set()
            time.sleep(2.0)

        c1.register_action("slow-fb", "exec-slow", failback=slow_failback)
        assert wait_until(
            lambda: c1.query_action("slow-fb").status == ActionStatus.RECOVERED
        )
        c1.request_failback(["slow-fb"], request_id="req-slow-fb")
        assert slow_fb_started.wait(5)

        # 无关联的登记、回切受理、查询仍须推进
        c2.register_action("root", "exec-main")
        assert wait_until(
            lambda: c2.query_action("root").status == ActionStatus.RECOVERED
        )

        latencies = []
        n = 300
        for i in range(n):
            op = i % 3
            start = time.monotonic()
            if op == 0:
                c2.register_action("m-%d" % i, "exec-main")
            elif op == 1:
                c2.request_failback(["root"], request_id="req-m-%d" % i)
            else:
                c2.query_action("root")
            latencies.append(time.monotonic() - start)

        assert len(latencies) == n  # 全部推进完成
        ordered = sorted(latencies)
        p95 = ordered[int(0.95 * n) - 1]
        print("\nlatency: p50=%.4fs p95=%.4fs max=%.4fs"
              % (statistics.median(latencies), p95, max(latencies)))
        assert p95 < 0.1, "p95 %.4fs >= 0.1s" % p95

        # 长动作最终正常收尾
        assert wait_until(
            lambda: c1.query_action("slow-rec").status == ActionStatus.RECOVERED,
            timeout=5,
        )
        assert wait_until(
            lambda: c1.query_request("req-slow-fb").status
            == RequestStatus.COMPLETED,
            timeout=5,
        )
    finally:
        c1.close()
        c2.close()
