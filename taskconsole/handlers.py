"""Task type registry + built-in demo task types.

A handler is `fn(ctx, payload) -> result`. It should call `ctx.sleep()` /
`ctx.progress()` regularly: those are the points where cancellation and the
timeout are noticed.
"""

import time


class NonRetryableError(Exception):
    """Raise for errors that retrying cannot fix (bad input, ...)."""


class TaskCancelled(Exception):
    pass


class TaskTimeout(Exception):
    pass


REGISTRY: dict[str, dict] = {}


def handler(name, description, example=None, pool="default"):
    def deco(fn):
        REGISTRY[name] = {"fn": fn, "description": description, "example": example or {}, "pool": pool}
        return fn
    return deco


class TaskContext:
    def __init__(self, store, task, worker_id):
        self.store = store
        self.task = task
        self.task_id = task["id"]
        self.attempt = task["attempts"]
        self.worker_id = worker_id
        self.checkpoint = task.get("checkpoint")
        self.started = time.monotonic()
        self.timeout_s = task["timeout_s"]
        self.cancel_requested = False

    def check(self):
        if self.cancel_requested:
            raise TaskCancelled()
        if time.monotonic() - self.started > self.timeout_s:
            raise TaskTimeout(f"exceeded timeout of {self.timeout_s:g}s")

    def progress(self, fraction: float, checkpoint=None):
        self.check()
        if self.store.report_progress(self.task_id, self.worker_id, fraction, checkpoint):
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


@handler("sleep", "Sleeps for `seconds`, reporting progress 10 times.", {"seconds": 5})
def sleep_task(ctx, payload):
    seconds = float(payload.get("seconds", 5))
    for i in range(10):
        ctx.sleep(seconds / 10)
        ctx.progress((i + 1) / 10)
    return {"slept": seconds}


@handler("count_primes", "Counts primes below `n` in chunks; checkpoints so a retry resumes.",
         {"n": 300000, "chunks": 20}, pool="cpu")
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


@handler("flaky", "Fails with a transient error on the first `fail_times` attempts, then succeeds.",
         {"fail_times": 2, "seconds": 1})
def flaky(ctx, payload):
    ctx.sleep(float(payload.get("seconds", 1)))
    if ctx.attempt <= int(payload.get("fail_times", 2)):
        raise ConnectionError(f"transient upstream error on attempt {ctx.attempt}")
    ctx.progress(1.0)
    return {"ok": True, "succeeded_on_attempt": ctx.attempt}


@handler("fail_permanent", "Always fails with a non-retryable error (bad input).", {"reason": "invalid account id"})
def fail_permanent(ctx, payload):
    ctx.sleep(0.5)
    raise NonRetryableError(payload.get("reason", "invalid input"))
