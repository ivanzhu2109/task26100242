"""灾备回切协调库。"""
from .coordinator import FailbackCoordinator
from .errors import (
    DuplicateAction,
    FailbackError,
    RegistrationRejected,
    TooManyPrerequisites,
    UnknownAction,
    UnknownRequest,
)
from .types import ActionStatus, ActionView, RequestStatus, RequestView

__all__ = [
    "FailbackCoordinator",
    "ActionStatus",
    "ActionView",
    "RequestStatus",
    "RequestView",
    "FailbackError",
    "RegistrationRejected",
    "DuplicateAction",
    "UnknownAction",
    "UnknownRequest",
    "TooManyPrerequisites",
]
