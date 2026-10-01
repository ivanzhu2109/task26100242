"""类型定义：状态枚举与查询视图。"""
from __future__ import annotations

import enum
from dataclasses import dataclass
from typing import Optional, Tuple


class ActionStatus(enum.Enum):
    """动作状态机。

    RECOVERING: 已登记，恢复待启动或进行中
    RECOVERED: 恢复成功
    RECOVER_FAILED: 恢复失败（终态，无设备残留）
    FAILBACK_QUEUED: 已纳入撤销范围且恢复成功，等待依赖它的动作收尾
    FAILBACK_RUNNING: 回切进行中
    FAILBACK_DONE: 回切完成（终态）
    """

    RECOVERING = "RECOVERING"
    RECOVERED = "RECOVERED"
    RECOVER_FAILED = "RECOVER_FAILED"
    FAILBACK_QUEUED = "FAILBACK_QUEUED"
    FAILBACK_RUNNING = "FAILBACK_RUNNING"
    FAILBACK_DONE = "FAILBACK_DONE"


TERMINAL_STATUSES = frozenset(
    {ActionStatus.RECOVER_FAILED, ActionStatus.FAILBACK_DONE}
)


class RequestStatus(enum.Enum):
    """回切请求状态：受理不等于完成。"""

    ACCEPTED = "ACCEPTED"
    COMPLETED = "COMPLETED"


@dataclass(frozen=True)
class ActionView:
    """单个动作的查询视图。"""

    action_id: str
    execution_id: str
    status: ActionStatus
    prerequisites: Tuple[str, ...]
    failback_requested: bool


@dataclass(frozen=True)
class RequestView:
    """回切请求的查询视图。"""

    request_id: str
    status: RequestStatus
    scope_size: int
    finished_size: int
