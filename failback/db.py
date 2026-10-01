"""SQLite 存储层：连接管理与建表。

设计要点：
- WAL 模式 + busy_timeout，支持 8 个库实例并发读写同一文件。
- 所有跨行一致性判断都放在单个事务里完成，保证可串行化解释。
- 设备动作（恢复/回切）由动作所属实例的调度器在事务外执行，
  长动作不会阻塞其它实例的登记、受理与查询。
"""
from __future__ import annotations

import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS actions (
    action_id          TEXT PRIMARY KEY,
    execution_id       TEXT NOT NULL,
    owner              TEXT NOT NULL,          -- 登记实例，负责执行其设备动作
    status             TEXT NOT NULL,          -- ActionStatus 取值
    failback_requested INTEGER NOT NULL DEFAULT 0,  -- 已被某受理请求纳入撤销范围
    recover_started    INTEGER NOT NULL DEFAULT 0,
    failback_started   INTEGER NOT NULL DEFAULT 0,
    created_at         REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_actions_owner_status
    ON actions(owner, status);
CREATE INDEX IF NOT EXISTS idx_actions_status
    ON actions(status);

CREATE TABLE IF NOT EXISTS action_deps (
    action_id TEXT NOT NULL REFERENCES actions(action_id),
    prereq_id TEXT NOT NULL,
    PRIMARY KEY (action_id, prereq_id)
);
CREATE INDEX IF NOT EXISTS idx_deps_prereq
    ON action_deps(prereq_id);

CREATE TABLE IF NOT EXISTS requests (
    request_id TEXT PRIMARY KEY,
    status     TEXT NOT NULL,                  -- ACCEPTED / COMPLETED
    created_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS request_scope (
    request_id TEXT NOT NULL REFERENCES requests(request_id),
    action_id  TEXT NOT NULL,
    PRIMARY KEY (request_id, action_id)
);
CREATE INDEX IF NOT EXISTS idx_scope_action
    ON request_scope(action_id);
"""

_BUSY_TIMEOUT_MS = 5000


def connect(db_path: str) -> sqlite3.Connection:
    """打开（必要时创建）数据库并应用性能/并发参数。"""
    conn = sqlite3.connect(
        db_path,
        timeout=_BUSY_TIMEOUT_MS / 1000.0,
        isolation_level=None,  # 显式控制事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=%d" % _BUSY_TIMEOUT_MS)
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    return conn


def begin_immediate(conn: sqlite3.Connection) -> None:
    """开启写事务；并发写者在此串行化。"""
    conn.execute("BEGIN IMMEDIATE")
