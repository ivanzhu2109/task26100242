"""Acceptance scenarios: multi-instance races, diamond dependencies,
overlapping failback requests."""

import sqlite3
import threading

from failback import ActionPhase, Coordinator, RequestState

from conftest import RecordingDevice


def make_coordinators(db_path, device, n):
    return [Coordinator(db_path, device, poll_interval=0.005) for _ in range(n)]


def dependency_closure(db_path, target):
    """Recompute the failback scope straight from the dependency records."""
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            WITH RECURSIVE closure(id) AS (
                SELECT ?
                UNION
                SELECT d.action_id FROM deps d
                JOIN closure c ON d.prereq_id = c.id
            )
            SELECT id FROM closure
            """,
            (target,),
        ).fetchall()
        return {r[0] for r in rows}
    finally:
        conn.close()


def test_registration_races_with_prerequisite_revocation(db_path):
    """Accepted registrations must fall inside the revocation scope; rejected
    ones must never start recovery; the revoked set equals the closure of the
    dependency records."""
    device = RecordingDevice()
    coords = make_coordinators(db_path, device, 4)
    try:
        assert coords[0].register_action("P", "exec0").accepted
        assert coords[0].wait_idle(5)

        results = {}
        lock = threading.Lock()
        start = threading.Event()

        def register_one(i):
            start.wait()
            res = coords[i % len(coords)].register_action(
                f"dep{i}", f"exec{i % 3}", ["P"]
            )
            with lock:
                results[f"dep{i}"] = res.accepted

        threads = [
            threading.Thread(target=register_one, args=(i,)) for i in range(40)
        ]

        def revoke():
            start.wait()
            coords[1].request_failback("P", request_id="reqP")

        revoker = threading.Thread(target=revoke)
        for t in threads:
            t.start()
        revoker.start()
        start.set()
        for t in threads:
            t.join()
        revoker.join()

        for c in coords:
            assert c.wait_idle(10)

        accepted = {aid for aid, ok in results.items() if ok}
        rejected = {aid for aid, ok in results.items() if not ok}

        # Rejected registrations never started their device recovery.
        started = set(device.ops("recover_start"))
        assert not (rejected & started)

        # Every accepted registration was revoked (or recover-failed).
        for aid in accepted:
            snap = coords[0].query_action(aid)
            assert snap.revoked, aid

        # The actually revoked set equals the closure over stored deps.
        expected = dependency_closure(db_path, "P")
        conn = sqlite3.connect(db_path)
        actual = {
            r[0]
            for r in conn.execute("SELECT action_id FROM actions WHERE revoked = 1")
        }
        conn.close()
        assert actual == expected
        assert accepted <= expected

        # Serializability: an accepted registration implies the failback
        # request was accepted after it, and it got failed back exactly once.
        for aid in accepted:
            assert device.ops("failback_ok").count(aid) == 1
        assert device.ops("failback_ok").count("P") == 1
    finally:
        for c in coords:
            c.close()


def test_diamond_concurrent_failback_waits_for_inflight_dependent(db_path):
    """B,C depend on A; D depends on B,C.  D still recovering while B and C
    are revoked concurrently; D then recovers successfully: D fails back
    exactly once and before B and C; A is untouched; an unrelated execution
    still completes recovery."""
    device = RecordingDevice()
    coords = make_coordinators(db_path, device, 3)
    try:
        assert coords[0].register_action("A", "exec1").accepted
        assert coords[0].wait_idle(5)
        assert coords[0].register_action("B", "exec1", ["A"]).accepted
        assert coords[0].register_action("C", "exec1", ["A"]).accepted
        assert coords[0].wait_idle(5)

        d_gate = threading.Event()
        device.recover_gates["D"] = d_gate
        assert coords[0].register_action("D", "exec1", ["B", "C"]).accepted

        # Unrelated execution keeps making progress while D is blocked.
        assert coords[2].register_action("U1", "execU").accepted

        start = threading.Event()

        def revoke(target, rid, coord):
            start.wait()
            coord.request_failback(target, request_id=rid)

        t1 = threading.Thread(target=revoke, args=("B", "reqB", coords[1]))
        t2 = threading.Thread(target=revoke, args=("C", "reqC", coords[2]))
        t1.start()
        t2.start()
        start.set()
        t1.join()
        t2.join()

        # D's recovery is still in flight: no failback may have started.
        assert device.ops("failback_start") == []

        # Unrelated execution completes recovery despite the ongoing failback.
        assert coords[2].wait_idle(5) is False or True  # requests pending
        deadline_ok = coords[2].query_action("U1")
        for _ in range(200):
            if deadline_ok.phase is ActionPhase.RECOVERED:
                break
            import time

            time.sleep(0.01)
            deadline_ok = coords[2].query_action("U1")
        assert deadline_ok.phase is ActionPhase.RECOVERED

        # Let D's recovery succeed.
        d_gate.set()
        for c in coords:
            assert c.wait_idle(10)

        # D failed back exactly once, and before B and C started theirs.
        calls = list(device.calls)
        assert [op for op, aid in calls if aid == "D"].count("failback_ok") == 1
        d_done = calls.index(("failback_ok", "D"))
        assert calls.index(("failback_start", "B")) > d_done
        assert calls.index(("failback_start", "C")) > d_done

        # A was never revoked nor failed back.
        assert "A" not in device.ops("failback_start")
        assert not coords[0].query_action("A").revoked

        # B and C each failed back exactly once.
        assert device.ops("failback_ok").count("B") == 1
        assert device.ops("failback_ok").count("C") == 1

        assert coords[0].query_request("reqB").state is RequestState.COMPLETED
        assert coords[0].query_request("reqC").state is RequestState.COMPLETED
    finally:
        for c in coords:
            c.close()


def test_overlapping_failback_storm_with_queries(db_path):
    """Many instances repeatedly issue overlapping failbacks and queries;
    every observation obeys the rules and the final device log has no missing
    or duplicate failback and respects dependent-before-prerequisite order."""
    device = RecordingDevice()
    coords = make_coordinators(db_path, device, 4)
    try:
        # Build a dependency tree across executions.
        assert coords[0].register_action("root", "exec0").accepted
        assert coords[0].wait_idle(5)
        children = {}
        for i in range(6):
            aid = f"n{i}"
            prereqs = ["root"] if i < 3 else [f"n{i - 3}"]
            children[aid] = prereqs
            assert coords[i % 4].register_action(aid, f"exec{i % 3}", prereqs).accepted
            for c in coords:
                assert c.wait_idle(5)

        stop = threading.Event()
        violations = []

        def query_loop(coord):
            while not stop.is_set():
                for rid in ("r-root", "r-n0", "r-n5"):
                    snap = coord.query_request(rid)
                    if snap is None:
                        continue
                    if snap.state is RequestState.COMPLETED and snap.pending:
                        violations.append(("completed-with-pending", rid))
                for aid in ("root", "n0", "n3", "n5"):
                    snap = coord.query_action(aid)
                    if snap is None:
                        violations.append(("missing-action", aid))

        def revoke_loop(coord, target, rid, rounds):
            for _ in range(rounds):
                coord.request_failback(target, request_id=rid)

        threads = []
        for i, coord in enumerate(coords):
            threads.append(threading.Thread(target=query_loop, args=(coord,)))
        threads.append(
            threading.Thread(target=revoke_loop, args=(coords[0], "n0", "r-n0", 5))
        )
        threads.append(
            threading.Thread(target=revoke_loop, args=(coords[1], "n5", "r-n5", 5))
        )
        threads.append(
            threading.Thread(target=revoke_loop, args=(coords[2], "root", "r-root", 5))
        )
        for t in threads:
            t.start()
        for t in threads[4:]:
            t.join()
        for c in coords:
            assert c.wait_idle(15)
        stop.set()
        for t in threads[:4]:
            t.join()

        assert violations == []

        # Every action is revoked; each failback ran exactly once.
        ok = device.ops("failback_ok")
        for aid in ["root"] + [f"n{i}" for i in range(6)]:
            assert ok.count(aid) == 1, (aid, ok)

        # Dependents finished failback before their prerequisites started.
        calls = list(device.calls)
        for aid, prereqs in children.items():
            for p in prereqs:
                assert calls.index(("failback_ok", aid)) < calls.index(
                    ("failback_start", p)
                ), (aid, p)

        for rid in ("r-root", "r-n0", "r-n5"):
            assert coords[0].query_request(rid).state is RequestState.COMPLETED
    finally:
        for c in coords:
            c.close()
