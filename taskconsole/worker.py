"""Worker: claim a task, run its handler, heartbeat, record the outcome.

The handler runs in a thread; the worker's main thread is the heartbeat loop:
  * renews the lease every `heartbeat_s` (fenced: a stale worker learns it lost the task)
  * relays cancel requests into the handler's context
  * enforces the hard timeout: if the handler ignores it, the worker records the
    failure and exits, and the supervisor starts a replacement (the only reliable way
    to stop stuck Python code)
  * on SIGTERM (drain) stops claiming, gives the running task `drain_grace_s` to
    finish, then asks it to stop and hands it back (release -> PENDING)

Run as a process:  python -m taskconsole.worker --db tasks.db --pool default --id default-1
"""

import argparse
import logging
import os
import signal
import threading
import time
import traceback
import uuid

from . import handlers
from .handlers import NonRetryableError, TaskCancelled, TaskContext, TaskReleased
from .store import LeaseLost, TaskStore

log = logging.getLogger("taskconsole.worker")

EXIT_HARD_TIMEOUT = 70
EXIT_LEASE_LOST = 71


class Worker:
    TIMEOUT_GRACE_S = 1.0

    def __init__(self, store, pool="default", worker_id=None, poll_interval=0.5, lease_s=30.0,
                 heartbeat_s=None, drain_grace_s=10.0, hard_exit=None):
        self.store = store
        self.pool = pool
        self.id = worker_id or f"{pool}-{uuid.uuid4().hex[:6]}"
        self.poll_interval = poll_interval
        self.lease_s = lease_s
        self.heartbeat_s = heartbeat_s or lease_s / 3
        self.drain_grace_s = drain_grace_s
        self.draining = threading.Event()
        self.hard_exit = hard_exit or (lambda code: os._exit(code))

    def run_one(self) -> bool:
        """Claim and execute one task. Returns False if nothing was ready."""
        task = self.store.claim(self.pool, self.id, self.lease_s)
        if task is None:
            return False
        self.execute(task)
        return True

    def execute(self, task):
        tid, fence = task["id"], task["fence"]
        try:
            self.store.start(tid, fence, self.id)
        except LeaseLost:
            return
        ctx = TaskContext(self.store, task, self.id)
        outcome = {}
        th = threading.Thread(target=self._run_handler, args=(ctx, task, outcome), daemon=True,
                              name=f"task-{tid}")
        th.start()
        drain_started = None
        while th.is_alive():
            th.join(self.heartbeat_s)
            if not th.is_alive():
                break
            try:
                if self.store.renew_lease(tid, fence, self.id, self.lease_s):
                    ctx.cancel_requested = True
            except LeaseLost:
                # Someone else owns the task now. Stop the handler; never write again.
                log.warning("%s lost lease on %s (fence %s)", self.id, tid, fence)
                ctx.lease_lost = True
                th.join(2.0)
                if th.is_alive():
                    self.hard_exit(EXIT_LEASE_LOST)
                return
            except Exception:
                # Transient DB error (e.g. locked): keep heartbeating. If it persists the lease
                # expires and fencing stops us from writing anything stale.
                log.exception("%s: lease renewal failed for %s", self.id, tid)
            # Cooperative handlers notice the timeout themselves; give them a grace period
            # (while still heartbeating) before treating the handler as stuck.
            if time.monotonic() - ctx.started > task["timeout_s"] + self.TIMEOUT_GRACE_S:
                self._safe(self.store.fail, tid, fence, self.id,
                           f"hard timeout after {task['timeout_s']:g}s: handler unresponsive, "
                           f"worker {self.id} restarting", True)
                self.hard_exit(EXIT_HARD_TIMEOUT)
                return
            if self.draining.is_set():
                if drain_started is None:
                    drain_started = time.monotonic()
                    self._safe(self.store.worker_heartbeat, self.id, "draining", tid)
                if time.monotonic() - drain_started > self.drain_grace_s:
                    ctx.release_requested = True
        self._record(tid, fence, outcome)

    def _run_handler(self, ctx, task, outcome):
        spec = handlers.REGISTRY.get(task["type"])
        try:
            if spec is None:
                raise NonRetryableError(f"unknown task type {task['type']!r}")
            outcome["result"] = spec["fn"](ctx, task["payload"])
        except BaseException as e:  # noqa: BLE001 - everything is reported via `outcome`
            outcome["exc"] = e
            outcome["tb"] = traceback.format_exc()

    def _record(self, tid, fence, outcome):
        e = outcome.get("exc")
        if e is None:
            self._safe(self.store.complete, tid, fence, self.id, outcome.get("result"))
        elif isinstance(e, LeaseLost):
            pass  # someone else owns it; writing anything would be a zombie write
        elif isinstance(e, TaskCancelled):
            self._safe(self.store.mark_cancelled, tid, fence, self.id)
        elif isinstance(e, TaskReleased):
            self._safe(self.store.release, tid, fence, self.id, "worker draining")
        elif isinstance(e, NonRetryableError):
            self._safe(self.store.fail, tid, fence, self.id, str(e), False)
        else:
            log.debug("task %s failed: %s", tid, outcome.get("tb"))
            self._safe(self.store.fail, tid, fence, self.id, f"{type(e).__name__}: {e}", True)

    def _safe(self, fn, *args):
        try:
            fn(*args)
        except LeaseLost as e:
            log.warning("%s: write fenced off: %s", self.id, e)
        except Exception:
            # Outcome not recorded; the lease will expire and the task is retried.
            log.exception("%s: failed to record outcome", self.id)

    def run_forever(self, stop: threading.Event | None = None):
        stop = stop or threading.Event()
        while not stop.is_set() and not self.draining.is_set():
            try:
                self.store.worker_heartbeat(self.id, "idle")
                if not self.run_one():
                    stop.wait(self.poll_interval)
            except Exception:
                log.exception("worker %s loop error", self.id)
                stop.wait(self.poll_interval)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True)
    p.add_argument("--pool", default="default")
    p.add_argument("--id", required=True)
    p.add_argument("--lease", type=float, default=5.0)
    p.add_argument("--heartbeat", type=float, default=1.0)
    p.add_argument("--drain-grace", type=float, default=10.0)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{args.id}] %(levelname)s %(message)s")

    store = TaskStore(args.db)
    worker = Worker(store, pool=args.pool, worker_id=args.id, lease_s=args.lease,
                    heartbeat_s=args.heartbeat, drain_grace_s=args.drain_grace)
    parent = os.getppid()

    def on_term(signum, frame):
        # Only set a flag: a signal handler can run in the middle of one of the main
        # thread's DB transactions, so it must not touch the DB itself.
        worker.draining.set()

    signal.signal(signal.SIGTERM, on_term)

    def orphan_watch():  # exit if the supervisor died
        while os.getppid() == parent:
            time.sleep(1)
        os._exit(0)

    threading.Thread(target=orphan_watch, daemon=True).start()
    store.register_worker(worker.id, worker.pool, os.getpid(), "idle")
    worker.run_forever()
    log.info("drained, exiting")


if __name__ == "__main__":
    main()
