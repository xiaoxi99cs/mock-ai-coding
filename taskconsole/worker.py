"""Worker: claim a task, run its handler, heartbeat, record the outcome.

One worker process runs one task at a time, using two threads:

  * the handler thread runs the task's code (e.g. `count_primes`);
  * the main thread is the "babysitter" (heartbeat loop). Every `heartbeat_s` it:
      1. renews the lease, so nobody else takes the task while we are alive.
         The renewal is fenced: if it fails with LeaseLost, another worker owns the task
         now and we stop without writing anything.
      2. passes a cancel request (set by the API) on to the handler.
      3. enforces the hard timeout: if the handler ignores its timeout, the worker records
         the failure and exits; the supervisor starts a replacement. Exiting the process is
         the only reliable way to stop stuck Python code (threads can't be killed).
      4. handles drain (SIGTERM): stop claiming, give the running task `drain_grace_s` to
         finish, then ask it to stop and hand it back (release -> PENDING).

The handler thread never writes the final status itself: it puts its result or exception
into `outcome`, and the main thread writes it to the DB afterwards (`_record`).

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

# Exit codes the supervisor sees when a worker kills itself on purpose.
EXIT_HARD_TIMEOUT = 70  # handler ignored its timeout
EXIT_LEASE_LOST = 71    # lost the lease and the handler would not stop


class Worker:
    # Extra time a handler gets after its timeout to notice it and stop by itself.
    TIMEOUT_GRACE_S = 1.0

    def __init__(self, store, pool="default", worker_id=None, poll_interval=0.5, lease_s=30.0,
                 heartbeat_s=None, drain_grace_s=10.0, hard_exit=None):
        self.store = store
        self.pool = pool
        self.id = worker_id or f"{pool}-{uuid.uuid4().hex[:6]}"
        self.poll_interval = poll_interval   # how long to sleep when the queue is empty
        self.lease_s = lease_s               # how long a claim is valid without a heartbeat
        self.heartbeat_s = heartbeat_s or lease_s / 3  # renew well before the lease expires
        self.drain_grace_s = drain_grace_s
        self.draining = threading.Event()    # set by SIGTERM: finish up and stop claiming
        # Tests pass a fake here, so "exit the process" can be observed instead of happening.
        self.hard_exit = hard_exit or (lambda code: os._exit(code))

    def run_forever(self, stop: threading.Event | None = None):
        """Main loop: claim and run tasks until stopped or draining."""
        stop = stop or threading.Event()
        while not stop.is_set() and not self.draining.is_set():
            try:
                self.store.worker_heartbeat(self.id, "idle")
                if not self.run_one():
                    stop.wait(self.poll_interval)  # queue empty: wait a bit, then poll again
            except Exception:
                # Never let one bad iteration (e.g. DB locked) kill the worker process.
                log.exception("worker %s loop error", self.id)
                stop.wait(self.poll_interval)

    def run_one(self) -> bool:
        """Claim and execute one task. Returns False if nothing was ready."""
        task = self.store.claim(self.pool, self.id, self.lease_s)
        if task is None:
            return False
        self.execute(task)
        return True

    def execute(self, task):
        """Run one claimed task to the end: CLAIMED -> RUNNING -> final status.

        `fence` is the fencing token we got when claiming. Every write we make carries it,
        and the store rejects the write (raises LeaseLost) if the task has since been
        claimed again by someone else.
        """
        task_id, fence = task["id"], task["fence"]
        try:
            self.store.start(task_id, fence, self.id)  # CLAIMED -> RUNNING
        except LeaseLost:
            return  # the lease already expired and someone else took the task

        # The handler runs in its own thread so this thread is free to heartbeat.
        # `ctx` is shared: we set flags on it (cancel/release/lease lost), and the
        # handler checks them via ctx.check(). `outcome` receives its result or exception.
        ctx = TaskContext(self.store, task, self.id)
        outcome = {}
        handler_thread = threading.Thread(target=self._run_handler, args=(ctx, task, outcome),
                                          daemon=True, name=f"task-{task_id}")
        handler_thread.start()
        drain_started = None

        while handler_thread.is_alive():
            # Wait up to one heartbeat interval for the handler to finish.
            handler_thread.join(self.heartbeat_s)
            if not handler_thread.is_alive():
                break  # finished: go record the outcome

            # 1. Heartbeat: extend the lease. It returns True if a cancel was requested.
            try:
                if self.store.renew_lease(task_id, fence, self.id, self.lease_s):
                    ctx.cancel_requested = True  # handler raises TaskCancelled at its next check
            except LeaseLost:
                # Someone else owns the task now. Stop the handler; never write again.
                log.warning("%s lost lease on %s (fence %s)", self.id, task_id, fence)
                ctx.lease_lost = True
                handler_thread.join(2.0)
                if handler_thread.is_alive():
                    self.hard_exit(EXIT_LEASE_LOST)  # handler won't stop: kill the process
                return
            except Exception:
                # Transient DB error (e.g. locked): keep heartbeating. If it persists the lease
                # expires and fencing stops us from writing anything stale.
                log.exception("%s: lease renewal failed for %s", self.id, task_id)

            # 2. Hard timeout. Cooperative handlers notice the timeout themselves (ctx.check()
            # raises TaskTimeout); give them a grace period, still heartbeating, before
            # treating the handler as stuck.
            running_for = time.monotonic() - ctx.started
            if running_for > task["timeout_s"] + self.TIMEOUT_GRACE_S:
                self._try_write(self.store.fail, task_id, fence, self.id,
                                f"hard timeout after {task['timeout_s']:g}s: handler unresponsive, "
                                f"worker {self.id} restarting", True)
                self.hard_exit(EXIT_HARD_TIMEOUT)  # supervisor will start a fresh worker
                return

            # 3. Drain (SIGTERM): let the task run for `drain_grace_s`, then ask it to stop.
            # The handler raises TaskReleased and _record() hands the task back to the queue.
            if self.draining.is_set():
                if drain_started is None:
                    drain_started = time.monotonic()
                    self._try_write(self.store.worker_heartbeat, self.id, "draining", task_id)
                if time.monotonic() - drain_started > self.drain_grace_s:
                    ctx.release_requested = True

        self._record(task_id, fence, outcome)

    def _run_handler(self, ctx, task, outcome):
        """Runs in the handler thread. Never touches task status: it only fills `outcome`."""
        spec = handlers.REGISTRY.get(task["type"])
        try:
            if spec is None:
                raise NonRetryableError(f"unknown task type {task['type']!r}")
            outcome["result"] = spec["fn"](ctx, task["payload"])
        except BaseException as e:  # noqa: BLE001 - everything is reported via `outcome`
            outcome["exc"] = e
            outcome["tb"] = traceback.format_exc()

    def _record(self, task_id, fence, outcome):
        """Turn the handler's result or exception into the task's next status."""
        e = outcome.get("exc")
        if e is None:
            # RUNNING -> SUCCEEDED
            self._try_write(self.store.complete, task_id, fence, self.id, outcome.get("result"))
        elif isinstance(e, LeaseLost):
            pass  # someone else owns it; writing anything would be a zombie write
        elif isinstance(e, TaskCancelled):
            # RUNNING -> CANCELLED
            self._try_write(self.store.mark_cancelled, task_id, fence, self.id)
        elif isinstance(e, TaskReleased):
            # RUNNING -> PENDING (drain hand-back; the attempt doesn't count)
            self._try_write(self.store.release, task_id, fence, self.id, "worker draining")
        elif isinstance(e, NonRetryableError):
            # RUNNING -> FAILED
            self._try_write(self.store.fail, task_id, fence, self.id, str(e), False)
        else:
            # Any other exception (incl. TaskTimeout) is retryable:
            # RUNNING -> RETRY_WAIT, or DEAD if attempts are used up.
            log.debug("task %s failed: %s", task_id, outcome.get("tb"))
            self._try_write(self.store.fail, task_id, fence, self.id, f"{type(e).__name__}: {e}", True)

    def _try_write(self, store_method, *args):
        """Call a store write, e.g. `store.complete(...)`; log errors instead of raising.

        Used for the worker's final writes. If one fails, crashing the worker would not help,
        and the task is still safe:
          * LeaseLost: the fencing token is stale, i.e. another worker took over the task.
            The store correctly rejected our write, so there is nothing to do.
          * any other error (e.g. DB locked): the outcome was not saved. The task stays
            RUNNING until its lease expires; the next claim then recovers it and retries it.
        """
        try:
            store_method(*args)
        except LeaseLost as e:
            log.warning("%s: write fenced off: %s", self.id, e)
        except Exception:
            log.exception("%s: failed to record outcome", self.id)


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

    def orphan_watch():
        # If the supervisor dies we get re-parented; exit instead of running unsupervised.
        while os.getppid() == parent:
            time.sleep(1)
        os._exit(0)

    threading.Thread(target=orphan_watch, daemon=True).start()
    store.register_worker(worker.id, worker.pool, os.getpid(), "idle")
    worker.run_forever()
    log.info("drained, exiting")


if __name__ == "__main__":
    main()
