"""Registration, recovery and query business rules."""

import threading

from failback import ActionPhase, Coordinator, RequestState


def test_register_without_prerequisites(coordinator, device):
    assert coordinator.register_action("a1", "exec1").accepted
    assert coordinator.wait_idle(5)
    snap = coordinator.query_action("a1")
    assert snap.phase is ActionPhase.RECOVERED
    assert device.ops("recover_ok") == ["a1"]


def test_register_rejects_unknown_prerequisite(coordinator, device):
    result = coordinator.register_action("a1", "exec1", ["ghost"])
    assert not result.accepted
    assert "not registered" in result.reason
    assert device.ops("recover_start") == []


def test_register_rejects_unrecovered_prerequisite(coordinator, device):
    gate = threading.Event()
    device.recover_gates["p1"] = gate
    assert coordinator.register_action("p1", "exec1").accepted
    result = coordinator.register_action("a1", "exec1", ["p1"])
    assert not result.accepted
    assert "not recovered" in result.reason
    gate.set()
    assert coordinator.wait_idle(5)


def test_register_rejects_failed_prerequisite(coordinator, device):
    device.fail_recover.add("p1")
    assert coordinator.register_action("p1", "exec1").accepted
    assert coordinator.wait_idle(5)
    assert coordinator.query_action("p1").phase is ActionPhase.RECOVER_FAILED
    result = coordinator.register_action("a1", "exec1", ["p1"])
    assert not result.accepted
    assert device.ops("recover_start") == ["p1"]


def test_register_rejects_duplicate_id(coordinator):
    assert coordinator.register_action("a1", "exec1").accepted
    result = coordinator.register_action("a1", "exec2")
    assert not result.accepted
    assert "duplicate" in result.reason


def test_register_rejects_too_many_prerequisites(coordinator):
    result = coordinator.register_action("a1", "exec1", [f"p{i}" for i in range(9)])
    assert not result.accepted
    assert "at most 8" in result.reason


def test_register_allows_eight_prerequisites(coordinator):
    for i in range(8):
        assert coordinator.register_action(f"p{i}", "exec1").accepted
    assert coordinator.wait_idle(5)
    assert coordinator.register_action("a1", "exec1", [f"p{i}" for i in range(8)]).accepted


def test_cross_execution_prerequisite(coordinator):
    assert coordinator.register_action("a1", "exec1").accepted
    assert coordinator.wait_idle(5)
    assert coordinator.register_action("b1", "exec2", ["a1"]).accepted
    assert coordinator.wait_idle(5)
    assert coordinator.query_action("b1").phase is ActionPhase.RECOVERED


def test_recover_failure_leaves_no_failback(coordinator, device):
    device.fail_recover.add("a1")
    assert coordinator.register_action("a1", "exec1").accepted
    assert coordinator.wait_idle(5)
    coordinator.request_failback("a1", request_id="r1")
    assert coordinator.wait_idle(5)
    assert device.ops("failback_start") == []
    assert coordinator.query_request("r1").state is RequestState.COMPLETED


def test_query_unknown_ids(coordinator):
    assert coordinator.query_action("nope") is None
    assert coordinator.query_request("nope") is None


def test_request_snapshot_tracks_pending(coordinator, device):
    gate = threading.Event()
    device.failback_gates["a1"] = gate
    assert coordinator.register_action("a1", "exec1").accepted
    assert coordinator.wait_idle(5)
    coordinator.request_failback("a1", request_id="r1")
    snap = coordinator.query_request("r1")
    assert snap.state is RequestState.ACCEPTED
    assert snap.scope == ("a1",)
    gate.set()
    assert coordinator.wait_idle(5)
    snap = coordinator.query_request("r1")
    assert snap.state is RequestState.COMPLETED
    assert snap.pending == ()
    assert coordinator.query_action("a1").phase is ActionPhase.REVOKED


def test_failback_waits_for_inflight_recovery(coordinator, device):
    gate = threading.Event()
    device.recover_gates["a1"] = gate
    assert coordinator.register_action("a1", "exec1").accepted
    coordinator.request_failback("a1", request_id="r1")
    # recovery still running: no failback may start
    assert device.ops("failback_start") == []
    assert coordinator.query_action("a1").phase is ActionPhase.REVOKING
    gate.set()
    assert coordinator.wait_idle(5)
    assert device.ops("failback_ok") == ["a1"]
    assert coordinator.query_request("r1").state is RequestState.COMPLETED


def test_failback_of_action_whose_recovery_fails(coordinator, device):
    gate = threading.Event()
    device.recover_gates["a1"] = gate
    device.fail_recover.add("a1")
    assert coordinator.register_action("a1", "exec1").accepted
    coordinator.request_failback("a1", request_id="r1")
    gate.set()
    assert coordinator.wait_idle(5)
    assert device.ops("failback_start") == []
    assert coordinator.query_request("r1").state is RequestState.COMPLETED


def test_duplicate_failback_requests_run_failback_once(coordinator, device):
    assert coordinator.register_action("a1", "exec1").accepted
    assert coordinator.wait_idle(5)
    r1 = coordinator.request_failback("a1")
    r2 = coordinator.request_failback("a1")
    assert coordinator.wait_idle(5)
    assert device.ops("failback_ok") == ["a1"]
    assert coordinator.query_request(r1).state is RequestState.COMPLETED
    assert coordinator.query_request(r2).state is RequestState.COMPLETED


def test_registration_rejected_after_revocation(coordinator, device):
    assert coordinator.register_action("a1", "exec1").accepted
    assert coordinator.wait_idle(5)
    coordinator.request_failback("a1")
    result = coordinator.register_action("b1", "exec2", ["a1"])
    assert not result.accepted
    assert "revoked" in result.reason
    assert "b1" not in device.ops("recover_start")
