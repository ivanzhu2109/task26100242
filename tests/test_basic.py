"""基础行为测试：登记校验、状态机、查询。"""
import threading
import time

import pytest

from failback import (
    ActionStatus,
    DuplicateAction,
    FailbackCoordinator,
    RegistrationRejected,
    RequestStatus,
    TooManyPrerequisites,
    UnknownAction,
    UnknownRequest,
)


@pytest.fixture
def coord(tmp_path):
    c = FailbackCoordinator(str(tmp_path / "t.db"), instance_id="i1")
    yield c
    c.close()


def wait_status(c, aid, st, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if c.query_action(aid).status == st:
            return True
        time.sleep(0.01)
    return False


def wait_request(c, rid, st, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if c.query_request(rid).status == st:
            return True
        time.sleep(0.01)
    return False


def test_register_and_recover(coord):
    done = []
    coord.register_action("A", "e1", recover=lambda: done.append("A"))
    assert wait_status(coord, "A", ActionStatus.RECOVERED)
    assert done == ["A"]
    view = coord.query_action("A")
    assert view.execution_id == "e1"
    assert view.prerequisites == ()
    assert not view.failback_requested


def test_register_unknown_prereq_rejected(coord):
    started = []
    with pytest.raises(RegistrationRejected):
        coord.register_action(
            "B", "e1", prerequisites=["nope"], recover=lambda: started.append(1)
        )
    time.sleep(0.2)
    assert started == []  # 整体拒绝，设备动作未启动
    with pytest.raises(UnknownAction):
        coord.query_action("B")


def test_register_prereq_not_recovered_rejected(coord):
    gate = threading.Event()
    coord.register_action("A", "e1", recover=lambda: gate.wait(2))
    with pytest.raises(RegistrationRejected):
        coord.register_action("B", "e1", prerequisites=["A"])
    gate.set()
    assert wait_status(coord, "A", ActionStatus.RECOVERED)


def test_duplicate_action(coord):
    coord.register_action("A", "e1")
    with pytest.raises(DuplicateAction):
        coord.register_action("A", "e1")


def test_too_many_prerequisites(coord):
    with pytest.raises(TooManyPrerequisites):
        coord.register_action("X", "e1", prerequisites=[str(i) for i in range(9)])


def test_recover_failure_is_terminal(coord):
    def boom():
        raise RuntimeError("device error")

    fb = []
    coord.register_action("A", "e1", recover=boom, failback=lambda: fb.append(1))
    assert wait_status(coord, "A", ActionStatus.RECOVER_FAILED)
    rid = coord.request_failback(["A"])
    # 恢复失败无需回切，请求应直接完成
    assert wait_request(coord, rid, RequestStatus.COMPLETED)
    time.sleep(0.2)
    assert fb == []
    assert coord.query_action("A").status == ActionStatus.RECOVER_FAILED


def test_failback_request_unknown_target(coord):
    with pytest.raises(UnknownAction):
        coord.request_failback(["ghost"])


def test_query_unknowns(coord):
    with pytest.raises(UnknownAction):
        coord.query_action("ghost")
    with pytest.raises(UnknownRequest):
        coord.query_request("ghost")


def test_accepted_is_not_completed(coord):
    release = threading.Event()
    fb_started = threading.Event()

    def slow_failback():
        fb_started.set()
        release.wait(5)

    coord.register_action("A", "e1", failback=slow_failback)
    assert wait_status(coord, "A", ActionStatus.RECOVERED)
    rid = coord.request_failback(["A"])
    assert fb_started.wait(5)
    # 回切动作未结束：请求保持 ACCEPTED，动作处于回切中
    view = coord.query_request(rid)
    assert view.status == RequestStatus.ACCEPTED
    assert view.scope_size == 1 and view.finished_size == 0
    assert coord.query_action("A").status == ActionStatus.FAILBACK_RUNNING
    release.set()
    assert wait_request(coord, rid, RequestStatus.COMPLETED)
    assert coord.query_action("A").status == ActionStatus.FAILBACK_DONE


def test_no_new_dependents_after_revocation(coord):
    coord.register_action("A", "e1")
    assert wait_status(coord, "A", ActionStatus.RECOVERED)
    coord.request_failback(["A"])
    started = []
    with pytest.raises(RegistrationRejected):
        coord.register_action(
            "B", "e1", prerequisites=["A"], recover=lambda: started.append(1)
        )
    time.sleep(0.2)
    assert started == []
