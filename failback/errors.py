"""库对外暴露的异常类型。"""


class FailbackError(Exception):
    """所有库异常的基类。"""


class RegistrationRejected(FailbackError):
    """登记被整体拒绝（前提未恢复成功或已被纳入撤销范围等）。"""


class DuplicateAction(FailbackError):
    """动作标识已存在。"""


class UnknownAction(FailbackError):
    """查询或回切目标的动作不存在。"""


class UnknownRequest(FailbackError):
    """查询的回切请求不存在。"""


class TooManyPrerequisites(FailbackError):
    """前提数量超过 8 个。"""
