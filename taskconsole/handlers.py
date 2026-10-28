"""Task type registry + built-in demo task types.

A handler is `fn(ctx, payload) -> result`. It should call `ctx.sleep()` /
`ctx.progress()` regularly: those are the points where cancellation and the
timeout are noticed.

Each type declares a pydantic `Payload` model. The API validates against it at submit
time (422, never enqueued), and the worker validates again before running, turning a
bad payload into a non-retryable failure: bad input is not something a retry can fix (R3).
The handler receives the validated payload as a dict, with defaults filled in.
"""

import os
import time
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .store import LeaseLost  # noqa: F401  (re-exported for handlers)


class NonRetryableError(Exception):
    """Raise for errors that retrying cannot fix (bad input, ...)."""


class TaskCancelled(Exception):
    pass


class TaskTimeout(Exception):
    pass


class TaskReleased(Exception):
    """The worker is draining and asked the handler to stop so the task can be handed back."""


def crash():
    """Simulate the worker process dying mid-task (kill -9 equivalent). Patched in tests."""
    os._exit(137)


REGISTRY: dict[str, dict] = {}


class Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")  # a misspelt field is an error, not silently ignored


def handler(name, description, example=None, pool="default", payload=Payload):
    def deco(fn):
        REGISTRY[name] = {"fn": fn, "description": description, "example": example or {}, "pool": pool,
                          "payload": payload}
        return fn
    return deco


def validate_payload(spec, payload: dict) -> dict:
    """Validated payload with defaults filled in. Raises pydantic.ValidationError."""
    model = spec.get("payload")
    return payload if model is None else model.model_validate(payload).model_dump()


def payload_errors(e: ValidationError) -> list[dict]:
    """JSON-safe error list (pydantic's `ctx` may hold exception objects)."""
    return [{k: v for k, v in err.items() if k in ("type", "loc", "msg", "input")} for err in e.errors()]


class TaskContext:
    """Handed to a task handler. The flags are set by the worker's heartbeat loop
    (another thread); `check()` turns them into exceptions inside the handler."""

    def __init__(self, store, task, worker_id):
        self.store = store
        self.task = task
        self.task_id = task["id"]
        self.fence = task["fence"]
        self.attempt = task["attempts"]
        self.worker_id = worker_id
        self.checkpoint = task.get("checkpoint")
        self.started = time.monotonic()
        self.timeout_s = task["timeout_s"]
        self.cancel_requested = False
        self.lease_lost = False
        self.release_requested = False

    def check(self):
        if self.lease_lost:
            raise LeaseLost("lease lost; stopping without writing")
        if self.cancel_requested:
            raise TaskCancelled()
        if self.release_requested:
            raise TaskReleased()
        if time.monotonic() - self.started > self.timeout_s:
            raise TaskTimeout(f"exceeded timeout of {self.timeout_s:g}s")

    def progress(self, fraction: float, checkpoint=None):
        self.check()
        if self.store.report_progress(self.task_id, self.fence, self.worker_id, fraction, checkpoint):
            self.cancel_requested = True
        if checkpoint is not None:
            self.checkpoint = checkpoint
        self.check()

    def sleep(self, seconds: float):
        end = time.monotonic() + seconds
        while (left := end - time.monotonic()) > 0:
            self.check()
            time.sleep(min(0.1, left))
        self.check()

    def effect(self, step, kind, detail, crash_before_record=False):
        """Idempotent external side effect (see TaskStore.perform_effect)."""
        self.check()
        return self.store.perform_effect(self.task_id, self.fence, self.worker_id, step, kind, detail,
                                         crash_before_record=crash if crash_before_record else None)


class SleepPayload(Payload):
    seconds: float = Field(5, ge=0, le=3600)


@handler("sleep", "Sleeps for `seconds`, reporting progress 10 times.", {"seconds": 5}, payload=SleepPayload)
def sleep_task(ctx, payload):
    seconds = float(payload.get("seconds", 5))
    for i in range(10):
        ctx.sleep(seconds / 10)
        ctx.progress((i + 1) / 10)
    return {"slept": seconds}


class CountPrimesPayload(Payload):
    n: int = Field(300000, ge=0, le=50_000_000)
    chunks: int = Field(20, ge=1, le=1000)


@handler("count_primes", "Counts primes below `n` in chunks; checkpoints so a retry resumes.",
         {"n": 300000, "chunks": 20}, pool="cpu", payload=CountPrimesPayload)
def count_primes(ctx, payload):
    n, chunks = int(payload.get("n", 300000)), int(payload.get("chunks", 20))
    state = ctx.checkpoint or {"chunk": 0, "count": 0}
    size = max(1, n // chunks)
    for i in range(state["chunk"], chunks):
        lo, hi = i * size, n if i == chunks - 1 else (i + 1) * size
        state = {"chunk": i + 1, "count": state["count"] + sum(1 for k in range(lo, hi) if _is_prime(k))}
        ctx.progress((i + 1) / chunks, checkpoint=state)
    return {"n": n, "primes": state["count"], "resumed_from_chunk": (ctx.task.get("checkpoint") or {}).get("chunk", 0)}


def _is_prime(k):
    if k < 2:
        return False
    if k % 2 == 0:
        return k == 2
    f = 3
    while f * f <= k:
        if k % f == 0:
            return False
        f += 2
    return True


class FlakyPayload(Payload):
    fail_times: int = Field(2, ge=0)
    seconds: float = Field(1, ge=0, le=3600)


@handler("flaky", "Fails with a transient error on the first `fail_times` attempts, then succeeds.",
         {"fail_times": 2, "seconds": 1}, payload=FlakyPayload)
def flaky(ctx, payload):
    ctx.sleep(float(payload.get("seconds", 1)))
    if ctx.attempt <= int(payload.get("fail_times", 2)):
        raise ConnectionError(f"transient upstream error on attempt {ctx.attempt}")
    ctx.progress(1.0)
    return {"ok": True, "succeeded_on_attempt": ctx.attempt}


class FailPermanentPayload(Payload):
    reason: str = "invalid input"


@handler("fail_permanent", "Always fails with a non-retryable error (bad input).", {"reason": "invalid account id"},
         payload=FailPermanentPayload)
def fail_permanent(ctx, payload):
    ctx.sleep(0.5)
    raise NonRetryableError(payload.get("reason", "invalid input"))


class CrashPayload(Payload):
    seconds: float = Field(4, ge=0, le=3600)
    crash_at: float = Field(0.5, ge=0, le=1)
    crash_on_attempts: list[int] = [1]


@handler("crash", "Dies (kill -9 style) at `crash_at` progress on the listed attempts. "
         "Recovered when its lease expires.", {"seconds": 4, "crash_at": 0.5, "crash_on_attempts": [1]},
         payload=CrashPayload)
def crash_task(ctx, payload):
    seconds = float(payload.get("seconds", 4))
    crash_at = float(payload.get("crash_at", 0.5))
    for i in range(10):
        if (i + 1) / 10 > crash_at and ctx.attempt in payload.get("crash_on_attempts", [1]):
            crash()
        ctx.sleep(seconds / 10)
        ctx.progress((i + 1) / 10)
    return {"survived_on_attempt": ctx.attempt}


class PoisonPayload(Payload):
    seconds: float = Field(1, ge=0, le=3600)


@handler("poison", "Crashes its worker on every attempt: a poison pill that must end up DEAD.", {"seconds": 1},
         payload=PoisonPayload)
def poison(ctx, payload):
    ctx.sleep(float(payload.get("seconds", 1)))
    crash()


class HangPayload(Payload):
    seconds: float = Field(3600, ge=0)


@handler("hang", "Ignores the context and never returns. The worker enforces the hard timeout by "
         "exiting; the supervisor restarts it.", {"seconds": 3600}, payload=HangPayload)
def hang(ctx, payload):
    time.sleep(float(payload.get("seconds", 3600)))
    return {"woke": True}


class ChargeAndEmailPayload(Payload):
    customer: str
    amount_cents: int = Field(gt=0)
    email: str
    crash: Literal["after_charge", "before_record"] | None = None
    seconds: float = Field(1, ge=0, le=3600)


@handler("charge_and_email", "Charges a card then emails a receipt, both as idempotent side effects. "
         "`crash`: 'after_charge' kills the worker after the charge is recorded; 'before_record' kills it "
         "after the provider charged but before our ledger saw it.",
         {"customer": "cus_42", "amount_cents": 1999, "email": "ada@example.com", "crash": "after_charge"},
         payload=ChargeAndEmailPayload)
def charge_and_email(ctx, payload):
    first = ctx.attempt == 1
    mode = payload.get("crash")
    charge = ctx.effect("charge", "charge",
                        {"customer": payload.get("customer"), "amount_cents": payload.get("amount_cents")},
                        crash_before_record=first and mode == "before_record")
    ctx.progress(0.5)
    ctx.sleep(float(payload.get("seconds", 1)))
    if first and mode == "after_charge":
        crash()
    email = ctx.effect("email", "email", {"to": payload.get("email"), "subject": "Your receipt"})
    ctx.progress(1.0)
    return {"charge": charge["id"], "email": email["id"]}
