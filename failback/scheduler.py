"""后台调度器：认领并执行恢复/回切动作，推进状态机。

- 每个实例只认领 owner 为自己的动作，设备动作（调用方回调）只存在于
  本进程内，因此不会出现跨实例执行同一动作的情况。
- 认领通过 ``UPDATE ... SET started=1 WHERE started=0`` 在写事务内完成，
  保证每个动作的恢复与回切各至多启动一次。
- 设备动作在线程池里、事务外执行，长动作不会占用数据库锁。
- 调度器内部连接由 _db_lock 保护，轮询线程与执行线程的事务不会交错。
"""
from __future__ import annotations

import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Callable, Dict, Iterator, Optional, Tuple

from .db import begin_immediate, connect
from .types import ActionStatus

_TERMINAL = (ActionStatus.RECOVER_FAILED.value, ActionStatus.FAILBACK_DONE.value)

_RECOVER_CLAIM_SQL = (
    "SELECT action_id FROM actions "
    "WHERE owner=? AND status=? AND recover_started=0 LIMIT ?"
)
_FAILBACK_CANDIDATE_SQL = (
    "SELECT action_id FROM actions "
    "WHERE owner=? AND status=? AND failback_started=0 LIMIT ?"
)
_DEPENDENTS_UNFINISHED_SQL = (
    "SELECT COUNT(*) FROM action_deps d JOIN actions a ON a.action_id=d.action_id "
    "WHERE d.prereq_id=? AND a.status NOT IN ('%s','%s')" % _TERMINAL
)
_COMPLETE_REQUESTS_SQL = (
    "UPDATE requests SET status='COMPLETED' WHERE status='ACCEPTED' AND NOT EXISTS ("
    "  SELECT 1 FROM request_scope s JOIN actions a ON a.action_id=s.action_id "
    "  WHERE s.request_id=requests.request_id "
    "  AND a.status NOT IN ('%s','%s'))" % _TERMINAL
)

RecoverFn = Callable[[], None]
FailbackFn = Callable[[], None]


class Scheduler:
    """单个协调器实例的后台执行引擎。"""

    def __init__(
        self,
        db_path: str,
        owner: str,
        registry: Dict[str, Tuple[Optional[RecoverFn], Optional[FailbackFn]]],
        poll_interval: float = 0.02,
        max_workers: int = 128,
    ) -> None:
        self._db_path = db_path
        self._owner = owner
        self._registry = registry
        self._poll_interval = poll_interval
        self._conn: Optional[sqlite3.Connection] = None
        self._db_lock = threading.Lock()
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="failback-dev"
        )
        self._poller = threading.Thread(
            target=self._poll_loop, name="failback-poller", daemon=True
        )

    def start(self) -> None:
        self._conn = connect(self._db_path)
        self._poller.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()
        self._poller.join(timeout=5)
        self._pool.shutdown(wait=True)
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def wake(self) -> None:
        self._wake.set()

    # ---- 内部实现 ----

    @contextmanager
    def _txn(self) -> Iterator[sqlite3.Connection]:
        """在连接锁内执行一个写事务，出错回滚。"""
        assert self._conn is not None
        with self._db_lock:
            begin_immediate(self._conn)
            try:
                yield self._conn
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def _txn_retry(self, body: Callable[[sqlite3.Connection], None]) -> None:
        """执行事务并在瞬时 SQLite 错误时重试，直到成功或停止。"""
        while not self._stop.is_set():
            try:
                with self._txn() as conn:
                    body(conn)
                return
            except sqlite3.Error:
                time.sleep(0.02)

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                if not self._has_pending_work():
                    progressed = False
                else:
                    progressed = self._claim_recoveries()
                    progressed |= self._claim_failbacks()
                    progressed |= self._complete_requests()
            except sqlite3.Error:
                progressed = False  # 瞬时 I/O 错误下一轮重试
            if not progressed:
                self._wake.wait(timeout=self._poll_interval)
                self._wake.clear()

    def _has_pending_work(self) -> bool:
        """只读预检：无待办时避免空写事务与业务写争用。"""
        assert self._conn is not None
        with self._db_lock:
            row = self._conn.execute(
                "SELECT "
                "(SELECT COUNT(*) FROM actions WHERE owner=? "
                " AND status=? AND recover_started=0) + "
                "(SELECT COUNT(*) FROM actions WHERE owner=? "
                " AND status=? AND failback_started=0) + "
                "(SELECT COUNT(*) FROM requests WHERE status='ACCEPTED')",
                (
                    self._owner,
                    ActionStatus.RECOVERING.value,
                    self._owner,
                    ActionStatus.FAILBACK_QUEUED.value,
                ),
            ).fetchone()
        return row[0] > 0

    def _claim_recoveries(self) -> bool:
        claimed = []

        def work(conn: sqlite3.Connection) -> None:
            rows = conn.execute(
                _RECOVER_CLAIM_SQL,
                (self._owner, ActionStatus.RECOVERING.value, 64),
            ).fetchall()
            for row in rows:
                cur = conn.execute(
                    "UPDATE actions SET recover_started=1 "
                    "WHERE action_id=? AND recover_started=0",
                    (row["action_id"],),
                )
                if cur.rowcount == 1:
                    claimed.append(row["action_id"])

        with self._txn() as conn:
            work(conn)
        for action_id in claimed:
            entry = self._registry.get(action_id)
            if entry is not None and entry[0] is not None:
                self._pool.submit(self._run_recover, action_id, entry[0])
        return bool(claimed)

    def _run_recover(self, action_id: str, fn: RecoverFn) -> None:
        ok = True
        try:
            fn()
        except Exception:
            ok = False  # 恢复失败保证无设备状态残留

        def work(conn: sqlite3.Connection) -> None:
            if ok:
                row = conn.execute(
                    "SELECT failback_requested FROM actions WHERE action_id=?",
                    (action_id,),
                ).fetchone()
                new_status = (
                    ActionStatus.FAILBACK_QUEUED.value
                    if row["failback_requested"]
                    else ActionStatus.RECOVERED.value
                )
            else:
                new_status = ActionStatus.RECOVER_FAILED.value
            conn.execute(
                "UPDATE actions SET status=? WHERE action_id=?",
                (new_status, action_id),
            )

        self._txn_retry(work)
        self._wake.set()

    def _claim_failbacks(self) -> bool:
        claimed = []

        def work(conn: sqlite3.Connection) -> None:
            rows = conn.execute(
                _FAILBACK_CANDIDATE_SQL,
                (self._owner, ActionStatus.FAILBACK_QUEUED.value, 64),
            ).fetchall()
            for row in rows:
                action_id = row["action_id"]
                unfinished = conn.execute(
                    _DEPENDENTS_UNFINISHED_SQL, (action_id,)
                ).fetchone()[0]
                if unfinished:
                    continue
                cur = conn.execute(
                    "UPDATE actions SET failback_started=1, status=? "
                    "WHERE action_id=? AND failback_started=0",
                    (ActionStatus.FAILBACK_RUNNING.value, action_id),
                )
                if cur.rowcount == 1:
                    claimed.append(action_id)

        with self._txn() as conn:
            work(conn)
        for action_id in claimed:
            entry = self._registry.get(action_id)
            if entry is not None and entry[1] is not None:
                self._pool.submit(self._run_failback, action_id, entry[1])
        return bool(claimed)

    def _run_failback(self, action_id: str, fn: FailbackFn) -> None:
        # 回切动作保证成功；若调用方实现抛出异常则重试直到成功。
        while not self._stop.is_set():
            try:
                fn()
                break
            except Exception:
                time.sleep(0.05)

        def work(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE actions SET status=? WHERE action_id=?",
                (ActionStatus.FAILBACK_DONE.value, action_id),
            )

        self._txn_retry(work)
        self._wake.set()

    def _complete_requests(self) -> bool:
        with self._txn() as conn:
            cur = conn.execute(_COMPLETE_REQUESTS_SQL)
            return cur.rowcount > 0
