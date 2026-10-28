"""Task state machine.

    CREATED ─► PENDING ─► CLAIMED ─► RUNNING ─► SUCCEEDED
                  ▲          │          ├─────► FAILED      (non-retryable error)
                  │          │          ├─────► DEAD        (attempts exhausted)
              RETRY_WAIT ◄───┴──────────┘                   (retryable error)
       any non-terminal ─► CANCELLED

Every status change in the store goes through `check_transition`, so an
illegal write (e.g. SUCCEEDED -> RUNNING) is rejected in one place.
"""

from enum import StrEnum


class Status(StrEnum):
    CREATED = "CREATED"
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    DEAD = "DEAD"
    CANCELLED = "CANCELLED"


TERMINAL = frozenset({Status.SUCCEEDED, Status.FAILED, Status.DEAD, Status.CANCELLED})  # final
CLAIMABLE = frozenset({Status.PENDING, Status.RETRY_WAIT})  # waiting in the queue
ACTIVE = frozenset({Status.CLAIMED, Status.RUNNING})        # owned by a worker, holds a lease

TRANSITIONS: dict[Status, frozenset[Status]] = {
    Status.CREATED: frozenset({Status.PENDING, Status.CANCELLED}),
    Status.PENDING: frozenset({Status.CLAIMED, Status.CANCELLED}),
    Status.RETRY_WAIT: frozenset({Status.CLAIMED, Status.CANCELLED}),
    # CLAIMED/RUNNING -> PENDING is a graceful release (worker shutting down);
    # -> RETRY_WAIT / DEAD also happens when a lease expires.
    Status.CLAIMED: frozenset(
        {Status.RUNNING, Status.PENDING, Status.RETRY_WAIT, Status.DEAD, Status.CANCELLED}
    ),
    Status.RUNNING: frozenset(
        {
            Status.SUCCEEDED,
            Status.FAILED,
            Status.RETRY_WAIT,
            Status.DEAD,
            Status.CANCELLED,
            Status.PENDING,
        }
    ),
}


class IllegalTransition(Exception):
    pass


def check_transition(frm: str, to: str) -> None:
    if Status(to) not in TRANSITIONS.get(Status(frm), frozenset()):
        raise IllegalTransition(f"illegal transition {frm} -> {to}")
