"""Type definitions for the failback coordination library."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Callable, Optional, Protocol


class RecoverState(str, enum.Enum):
    """Lifecycle of the caller-provided recover action."""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class FailbackState(str, enum.Enum):
    """Lifecycle of the caller-provided failback action."""

    NONE = "NONE"
    RUNNING = "RUNNING"
    DONE = "DONE"


class ActionPhase(str, enum.Enum):
    """Derived, user-facing phase of a registered action."""

    RECOVERING = "RECOVERING"
    RECOVERED = "RECOVERED"
    RECOVER_FAILED = "RECOVER_FAILED"
    REVOKING = "REVOKING"
    FAILING_BACK = "FAILING_BACK"
    REVOKED = "REVOKED"


class RequestState(str, enum.Enum):
    """State of an accepted failback request."""

    ACCEPTED = "ACCEPTED"
    COMPLETED = "COMPLETED"


def derive_phase(
    recover_state: RecoverState, revoked: bool, failback_state: FailbackState
) -> ActionPhase:
    """Derive the user-facing phase from the raw persisted columns."""
    if failback_state is FailbackState.DONE:
        return ActionPhase.REVOKED
    if failback_state is FailbackState.RUNNING:
        return ActionPhase.FAILING_BACK
    if revoked:
        return ActionPhase.REVOKING
    if recover_state is RecoverState.RUNNING:
        return ActionPhase.RECOVERING
    if recover_state is RecoverState.SUCCEEDED:
        return ActionPhase.RECOVERED
    return ActionPhase.RECOVER_FAILED


@dataclass(frozen=True)
class ActionSnapshot:
    """Consistent point-in-time view of one action."""

    action_id: str
    execution_id: str
    phase: ActionPhase
    recover_state: RecoverState
    revoked: bool
    failback_state: FailbackState
    prerequisites: tuple[str, ...] = ()


@dataclass(frozen=True)
class RequestSnapshot:
    """Consistent point-in-time view of one failback request."""

    request_id: str
    target_action_id: str
    state: RequestState
    scope: tuple[str, ...] = ()
    pending: tuple[str, ...] = ()


@dataclass(frozen=True)
class RegisterResult:
    """Outcome of a registration attempt."""

    accepted: bool
    action_id: str
    reason: Optional[str] = None


class DeviceActions(Protocol):
    """Caller-provided, in-process device operations.

    ``recover`` runs the device recovery; raising marks the recovery as
    failed (the caller guarantees no device state is left behind on
    failure).  ``failback`` undoes the recovery and is guaranteed to
    succeed; it is retried if it raises.
    """

    def recover(self, action_id: str) -> None: ...

    def failback(self, action_id: str) -> None: ...
