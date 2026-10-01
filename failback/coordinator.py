"""对外协调入口：恢复动作登记、回切请求、状态查询。

一致性设计：
- 登记、回切受理各自在单个 BEGIN IMMEDIATE 写事务内完成校验与落库，
  多实例并发时在数据库层面串行化，可解释为遵守调用先后关系的串行顺序。
- 查询在单个只读事务内完成，WAL 快照保证不会读到部分撤销的混合视图。
- 设备动作由后台调度器在事务外执行，长动作不阻塞其它调用。
"""
from __future__ import annotations

import sqlite3
import threading
import time
import uuid
from typing import Callable, Dict, Iterable, Optional, Sequence, Tuple

from .db import begin_immediate, connect
from .errors import (
    DuplicateAction,
    FailbackError,
    RegistrationRejected,
    TooManyPrerequisites,
    UnknownAction,
    UnknownRequest,
)
from .scheduler import Scheduler
from .types import ActionStatus, ActionView, RequestStatus, RequestView

RecoverFn = Callable[[], None]
FailbackFn = Callable[[], None]

MAX_PREREQUISITES = 8

_CLOSURE_SQL = """
WITH RECURSIVE closure(aid) AS (
    SELECT action_id FROM actions WHERE action_id IN (%s)
    UNION
    SELECT d.action_id FROM action_deps d JOIN closure c ON d.prereq_id = c.aid
)
SELECT aid FROM closure
"""


class FailbackCoordinator:
    """灾备回切协调器。一个进程内可存在多个实例共享同一 SQLite 文件。"""

    def __init__(self, db_path: str, instance_id: Optional[str] = None) -> None:
        self._db_path = db_path
        self._instance_id = instance_id or uuid.uuid4().hex
        self._conn = connect(db_path)
        self._lock = threading.Lock()
        self._registry: Dict[
            str, Tuple[Optional[RecoverFn], Optional[FailbackFn]]
        ] = {}
        self._scheduler = Scheduler(db_path, self._instance_id, self._registry)
        self._scheduler.start()
        self._closed = False

    @property
    def instance_id(self) -> str:
        return self._instance_id

    # ---- 登记 ----

    def register_action(
        self,
        action_id: str,
        execution_id: str,
        prerequisites: Sequence[str] = (),
        recover: Optional[RecoverFn] = None,
        failback: Optional[FailbackFn] = None,
    ) -> None:
        """登记恢复动作。所有前提均已恢复成功且未被撤销时整体成功，
        否则整体拒绝（抛出 RegistrationRejected）且不启动任何设备动作。"""
        self._ensure_open()
        prereqs = tuple(dict.fromkeys(prerequisites))
        # 缺省回调视为立即成功的空操作。
        recover = recover or (lambda: None)
        failback = failback or (lambda: None)
        if len(prereqs) > MAX_PREREQUISITES:
            raise TooManyPrerequisites(
                "prerequisites exceed %d" % MAX_PREREQUISITES
            )
        with self._lock:
            begin_immediate(self._conn)
            try:
                row = self._conn.execute(
                    "SELECT 1 FROM actions WHERE action_id=?", (action_id,)
                ).fetchone()
                if row is not None:
                    raise DuplicateAction(action_id)
                for prereq in prereqs:
                    prow = self._conn.execute(
                        "SELECT status, failback_requested FROM actions "
                        "WHERE action_id=?",
                        (prereq,),
                    ).fetchone()
                    if prow is None:
                        raise RegistrationRejected(
                            "prerequisite %s does not exist" % prereq
                        )
                    if (
                        prow["status"] != ActionStatus.RECOVERED.value
                        or prow["failback_requested"]
                    ):
                        raise RegistrationRejected(
                            "prerequisite %s not recovered or already revoked"
                            % prereq
                        )
                # 先放回调再提交，保证调度器看到行时回调一定已就绪。
                self._registry[action_id] = (recover, failback)
                self._conn.execute(
                    "INSERT INTO actions VALUES (?,?,?,?,0,0,0,?)",
                    (
                        action_id,
                        execution_id,
                        self._instance_id,
                        ActionStatus.RECOVERING.value,
                        time.time(),
                    ),
                )
                self._conn.executemany(
                    "INSERT INTO action_deps(action_id, prereq_id) VALUES (?,?)",
                    [(action_id, p) for p in prereqs],
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                self._registry.pop(action_id, None)
                raise
        self._scheduler.wake()

    # ---- 回切请求 ----

    def request_failback(
        self,
        targets: Iterable[str],
        request_id: Optional[str] = None,
    ) -> str:
        """受理回切请求：冻结撤销范围（目标及其全部直接/间接依赖动作），
        范围内动作立即停止接纳新依赖。返回请求标识。"""
        self._ensure_open()
        target_list = list(dict.fromkeys(targets))
        if not target_list:
            raise FailbackError("targets must not be empty")
        rid = request_id or uuid.uuid4().hex
        placeholders = ",".join("?" for _ in target_list)
        with self._lock:
            begin_immediate(self._conn)
            try:
                for target in target_list:
                    row = self._conn.execute(
                        "SELECT 1 FROM actions WHERE action_id=?", (target,)
                    ).fetchone()
                    if row is None:
                        raise UnknownAction(target)
                scope = [
                    r["aid"]
                    for r in self._conn.execute(
                        _CLOSURE_SQL % placeholders, target_list
                    )
                ]
                try:
                    self._conn.execute(
                        "INSERT INTO requests VALUES (?,?,?)",
                        (rid, RequestStatus.ACCEPTED.value, time.time()),
                    )
                except sqlite3.IntegrityError:
                    raise FailbackError(
                        "duplicate request_id %s" % rid
                    ) from None
                self._conn.executemany(
                    "INSERT INTO request_scope(request_id, action_id) VALUES (?,?)",
                    [(rid, aid) for aid in scope],
                )
                scope_ph = ",".join("?" for _ in scope)
                self._conn.execute(
                    "UPDATE actions SET failback_requested=1 "
                    "WHERE action_id IN (%s)" % scope_ph,
                    scope,
                )
                self._conn.execute(
                    "UPDATE actions SET status=? WHERE action_id IN (%s) "
                    "AND status=?"
                    % (scope_ph),
                    (
                        ActionStatus.FAILBACK_QUEUED.value,
                        *scope,
                        ActionStatus.RECOVERED.value,
                    ),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        self._scheduler.wake()
        return rid

    # ---- 查询 ----

    def query_action(self, action_id: str) -> ActionView:
        """查询单个动作状态（一致性快照）。"""
        self._ensure_open()
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                row = self._conn.execute(
                    "SELECT * FROM actions WHERE action_id=?", (action_id,)
                ).fetchone()
                if row is None:
                    raise UnknownAction(action_id)
                deps = tuple(
                    r["prereq_id"]
                    for r in self._conn.execute(
                        "SELECT prereq_id FROM action_deps WHERE action_id=? "
                        "ORDER BY prereq_id",
                        (action_id,),
                    )
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return ActionView(
            action_id=row["action_id"],
            execution_id=row["execution_id"],
            status=ActionStatus(row["status"]),
            prerequisites=deps,
            failback_requested=bool(row["failback_requested"]),
        )

    def query_request(self, request_id: str) -> RequestView:
        """查询回切请求：受理（ACCEPTED）与完成（COMPLETED）严格区分。"""
        self._ensure_open()
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                row = self._conn.execute(
                    "SELECT status FROM requests WHERE request_id=?", (request_id,)
                ).fetchone()
                if row is None:
                    raise UnknownRequest(request_id)
                scope_size = self._conn.execute(
                    "SELECT COUNT(*) FROM request_scope WHERE request_id=?",
                    (request_id,),
                ).fetchone()[0]
                finished = self._conn.execute(
                    "SELECT COUNT(*) FROM request_scope s JOIN actions a "
                    "ON a.action_id=s.action_id "
                    "WHERE s.request_id=? AND a.status IN (?,?)",
                    (
                        request_id,
                        ActionStatus.RECOVER_FAILED.value,
                        ActionStatus.FAILBACK_DONE.value,
                    ),
                ).fetchone()[0]
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise
        return RequestView(
            request_id=request_id,
            status=RequestStatus(row["status"]),
            scope_size=scope_size,
            finished_size=finished,
        )

    # ---- 生命周期 ----

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._scheduler.stop()
        self._conn.close()

    def _ensure_open(self) -> None:
        if self._closed:
            raise FailbackError("coordinator is closed")

    def __enter__(self) -> "FailbackCoordinator":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
