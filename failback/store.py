"""SQLite persistence layer.

All mutations happen inside ``BEGIN IMMEDIATE`` transactions so that the
sequence of committed transactions is the serial order every call can be
explained by.  Reads run in their own transaction and therefore always
observe one consistent snapshot.
"""

from __future__ import annotations

import functools
import sqlite3
import threading
from typing import Optional

from .models import (
    ActionSnapshot,
    FailbackState,
    RecoverState,
    RegisterResult,
    RequestState,
    RequestSnapshot,
    derive_phase,
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS actions (
    action_id      TEXT PRIMARY KEY,
    execution_id   TEXT NOT NULL,
    recover_state  TEXT NOT NULL,
    revoked        INTEGER NOT NULL DEFAULT 0,
    failback_state TEXT NOT NULL DEFAULT 'NONE'
);
CREATE TABLE IF NOT EXISTS deps (
    action_id   TEXT NOT NULL REFERENCES actions(action_id),
    prereq_id   TEXT NOT NULL,
    PRIMARY KEY (action_id, prereq_id)
);
CREATE INDEX IF NOT EXISTS idx_deps_prereq ON deps(prereq_id);
CREATE INDEX IF NOT EXISTS idx_actions_failback
    ON actions(revoked, recover_state, failback_state);
CREATE TABLE IF NOT EXISTS requests (
    request_id       TEXT PRIMARY KEY,
    target_action_id TEXT NOT NULL,
    state            TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS request_scope (
    request_id TEXT NOT NULL REFERENCES requests(request_id),
    action_id  TEXT NOT NULL,
    PRIMARY KEY (request_id, action_id)
);
"""


def _locked(fn):
    """Serialize access to the shared connection per store instance."""

    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return fn(self, *args, **kwargs)

    return wrapper


class Store:
    """Thread-safe gateway to one SQLite database file."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.RLock()
        self._conn: Optional[sqlite3.Connection] = None
        self._init_schema()

    # -- internal helpers -------------------------------------------------

    def _get(self) -> sqlite3.Connection:
        if self._conn is None:
            conn = sqlite3.connect(
                self._db_path, timeout=30.0, check_same_thread=False
            )
            conn.execute("PRAGMA busy_timeout = 30000")
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = NORMAL")
            conn.execute("PRAGMA foreign_keys = ON")
            self._conn = conn
        return self._conn

    def _init_schema(self) -> None:
        with self._lock:
            conn = self._get()
            with conn:
                conn.executescript(SCHEMA)

    # -- registration -----------------------------------------------------

    @_locked
    def try_register(
        self,
        action_id: str,
        execution_id: str,
        prerequisites: tuple[str, ...],
    ) -> RegisterResult:
        """Atomically accept or reject a registration."""
        conn = self._get()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT 1 FROM actions WHERE action_id = ?", (action_id,)
            ).fetchone()
            if row is not None:
                conn.execute("ROLLBACK")
                return RegisterResult(False, action_id, "duplicate action_id")
            for prereq in prerequisites:
                row = conn.execute(
                    "SELECT recover_state, revoked FROM actions WHERE action_id = ?",
                    (prereq,),
                ).fetchone()
                if row is None:
                    conn.execute("ROLLBACK")
                    return RegisterResult(
                        False, action_id, f"prerequisite {prereq} not registered"
                    )
                if row[0] != RecoverState.SUCCEEDED.value:
                    conn.execute("ROLLBACK")
                    return RegisterResult(
                        False, action_id, f"prerequisite {prereq} not recovered"
                    )
                if row[1]:
                    conn.execute("ROLLBACK")
                    return RegisterResult(
                        False, action_id, f"prerequisite {prereq} revoked"
                    )
            conn.execute(
                "INSERT INTO actions (action_id, execution_id, recover_state,"
                " revoked, failback_state) VALUES (?, ?, ?, 0, ?)",
                (action_id, execution_id, RecoverState.RUNNING.value,
                 FailbackState.NONE.value),
            )
            conn.executemany(
                "INSERT INTO deps (action_id, prereq_id) VALUES (?, ?)",
                [(action_id, p) for p in prerequisites],
            )
            conn.execute("COMMIT")
            return RegisterResult(True, action_id)
        except Exception:
            conn.execute("ROLLBACK")
            raise

    # -- recover lifecycle --------------------------------------------------

    @_locked
    def finish_recover(self, action_id: str, succeeded: bool) -> None:
        state = RecoverState.SUCCEEDED.value if succeeded else RecoverState.FAILED.value
        conn = self._get()
        with conn:
            conn.execute(
                "UPDATE actions SET recover_state = ?"
                " WHERE action_id = ? AND recover_state = ?",
                (state, action_id, RecoverState.RUNNING.value),
            )

    # -- failback lifecycle -------------------------------------------------

    @_locked
    def accept_failback(self, request_id: str, target_action_id: str) -> tuple[str, ...]:
        """Mark target plus all transitive dependents as revoked; return scope."""
        conn = self._get()
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT state FROM requests WHERE request_id = ?", (request_id,)
            ).fetchone()
            if row is not None:
                scope = tuple(
                    r[0]
                    for r in conn.execute(
                        "SELECT action_id FROM request_scope WHERE request_id = ?",
                        (request_id,),
                    )
                )
                conn.execute("COMMIT")
                return scope
            target = conn.execute(
                "SELECT 1 FROM actions WHERE action_id = ?", (target_action_id,)
            ).fetchone()
            if target is None:
                conn.execute("ROLLBACK")
                raise KeyError(f"unknown action {target_action_id}")
            scope = tuple(
                r[0]
                for r in conn.execute(
                    """
                    WITH RECURSIVE closure(id) AS (
                        SELECT ?
                        UNION
                        SELECT d.action_id FROM deps d
                        JOIN closure c ON d.prereq_id = c.id
                    )
                    SELECT id FROM closure
                    """,
                    (target_action_id,),
                )
            )
            conn.executemany(
                "UPDATE actions SET revoked = 1 WHERE action_id = ?",
                [(a,) for a in scope],
            )
            conn.execute(
                "INSERT INTO requests (request_id, target_action_id, state)"
                " VALUES (?, ?, ?)",
                (request_id, target_action_id, RequestState.ACCEPTED.value),
            )
            conn.executemany(
                "INSERT INTO request_scope (request_id, action_id) VALUES (?, ?)",
                [(request_id, a) for a in scope],
            )
            conn.execute("COMMIT")
            return scope
        except Exception:
            conn.execute("ROLLBACK")
            raise

    @_locked
    def claim_failback_ready(self, limit: int = 16) -> list[str]:
        """Atomically claim revoked actions whose dependents are all terminal."""
        conn = self._get()
        candidates = [
            r[0]
            for r in conn.execute(
                """
                SELECT a.action_id FROM actions a
                WHERE a.revoked = 1
                  AND a.recover_state = ?
                  AND a.failback_state = ?
                  AND NOT EXISTS (
                      SELECT 1 FROM deps d
                      JOIN actions dep ON dep.action_id = d.action_id
                      WHERE d.prereq_id = a.action_id
                        AND NOT (dep.recover_state = ?
                                 OR dep.failback_state = ?)
                  )
                LIMIT ?
                """,
                (
                    RecoverState.SUCCEEDED.value,
                    FailbackState.NONE.value,
                    RecoverState.FAILED.value,
                    FailbackState.DONE.value,
                    limit,
                ),
            )
        ]
        if not candidates:
            return []
        claimed = []
        with conn:
            for action_id in candidates:
                cur = conn.execute(
                    "UPDATE actions SET failback_state = ?"
                    " WHERE action_id = ? AND failback_state = ?",
                    (FailbackState.RUNNING.value, action_id, FailbackState.NONE.value),
                )
                if cur.rowcount == 1:
                    claimed.append(action_id)
        return claimed

    @_locked
    def finish_failback(self, action_id: str, succeeded: bool) -> None:
        conn = self._get()
        with conn:
            if succeeded:
                conn.execute(
                    "UPDATE actions SET failback_state = ?"
                    " WHERE action_id = ? AND failback_state = ?",
                    (FailbackState.DONE.value, action_id, FailbackState.RUNNING.value),
                )
            else:
                conn.execute(
                    "UPDATE actions SET failback_state = ?"
                    " WHERE action_id = ? AND failback_state = ?",
                    (FailbackState.NONE.value, action_id, FailbackState.RUNNING.value),
                )

    @_locked
    def complete_requests(self, limit: int = 32) -> int:
        """Mark requests whose whole scope is terminal as COMPLETED."""
        conn = self._get()
        with conn:
            cur = conn.execute(
                """
                UPDATE requests SET state = ?
                WHERE state = ? AND NOT EXISTS (
                    SELECT 1 FROM request_scope s
                    JOIN actions a ON a.action_id = s.action_id
                    WHERE s.request_id = requests.request_id
                      AND NOT (a.recover_state = ? OR a.failback_state = ?)
                )
                """,
                (
                    RequestState.COMPLETED.value,
                    RequestState.ACCEPTED.value,
                    RecoverState.FAILED.value,
                    FailbackState.DONE.value,
                ),
            )
            return cur.rowcount

    # -- queries ------------------------------------------------------------

    @_locked
    def get_action(self, action_id: str) -> Optional[ActionSnapshot]:
        conn = self._get()
        with conn:
            row = conn.execute(
                "SELECT execution_id, recover_state, revoked, failback_state"
                " FROM actions WHERE action_id = ?",
                (action_id,),
            ).fetchone()
            if row is None:
                return None
            prereqs = tuple(
                r[0]
                for r in conn.execute(
                    "SELECT prereq_id FROM deps WHERE action_id = ? ORDER BY prereq_id",
                    (action_id,),
                )
            )
        recover_state = RecoverState(row[1])
        revoked = bool(row[2])
        failback_state = FailbackState(row[3])
        return ActionSnapshot(
            action_id=action_id,
            execution_id=row[0],
            phase=derive_phase(recover_state, revoked, failback_state),
            recover_state=recover_state,
            revoked=revoked,
            failback_state=failback_state,
            prerequisites=prereqs,
        )

    @_locked
    def get_request(self, request_id: str) -> Optional[RequestSnapshot]:
        conn = self._get()
        with conn:
            row = conn.execute(
                "SELECT target_action_id, state FROM requests WHERE request_id = ?",
                (request_id,),
            ).fetchone()
            if row is None:
                return None
            scope = tuple(
                r[0]
                for r in conn.execute(
                    "SELECT action_id FROM request_scope WHERE request_id = ?"
                    " ORDER BY action_id",
                    (request_id,),
                )
            )
            pending = tuple(
                r[0]
                for r in conn.execute(
                    """
                    SELECT s.action_id FROM request_scope s
                    JOIN actions a ON a.action_id = s.action_id
                    WHERE s.request_id = ?
                      AND NOT (a.recover_state = ? OR a.failback_state = ?)
                    ORDER BY s.action_id
                    """,
                    (
                        request_id,
                        RecoverState.FAILED.value,
                        FailbackState.DONE.value,
                    ),
                )
            )
        return RequestSnapshot(
            request_id=request_id,
            target_action_id=row[0],
            state=RequestState(row[1]),
            scope=scope,
            pending=pending,
        )

    @_locked
    def count_unfinished(self) -> int:
        """Number of in-flight device actions plus incomplete requests."""
        conn = self._get()
        with conn:
            row = conn.execute(
                "SELECT (SELECT COUNT(*) FROM actions WHERE recover_state = ?"
                " OR failback_state = ?)"
                " + (SELECT COUNT(*) FROM requests WHERE state = ?)",
                (
                    RecoverState.RUNNING.value,
                    FailbackState.RUNNING.value,
                    RequestState.ACCEPTED.value,
                ),
            ).fetchone()
        return int(row[0])

    @_locked
    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None
