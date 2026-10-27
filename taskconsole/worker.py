"""Worker: claim a task, run its handler, record the outcome."""

import logging
import threading
import traceback
import uuid

from . import handlers
from .handlers import NonRetryableError, TaskCancelled, TaskContext

log = logging.getLogger("taskconsole.worker")


class Worker:
    def __init__(self, store, pool="default", worker_id=None, poll_interval=0.5):
        self.store = store
        self.pool = pool
        self.id = worker_id or f"{pool}-{uuid.uuid4().hex[:6]}"
        self.poll_interval = poll_interval

    def run_one(self) -> bool:
        """Claim and execute one task. Returns False if nothing was ready."""
        task = self.store.claim(self.pool, self.id)
        if task is None:
            return False
        self.execute(task)
        return True

    def execute(self, task):
        tid = task["id"]
        self.store.start(tid, self.id)
        ctx = TaskContext(self.store, task, self.id)
        spec = handlers.REGISTRY.get(task["type"])
        try:
            if spec is None:
                raise NonRetryableError(f"unknown task type {task['type']!r}")
            result = spec["fn"](ctx, task["payload"])
        except TaskCancelled:
            self.store.mark_cancelled(tid, self.id)
        except NonRetryableError as e:
            self.store.fail(tid, self.id, str(e), retryable=False)
        except Exception as e:
            log.debug("task %s failed: %s", tid, traceback.format_exc())
            self.store.fail(tid, self.id, f"{type(e).__name__}: {e}", retryable=True)
        else:
            self.store.complete(tid, self.id, result)

    def run_forever(self, stop: threading.Event):
        while not stop.is_set():
            try:
                if not self.run_one():
                    stop.wait(self.poll_interval)
            except Exception:
                log.exception("worker %s loop error", self.id)
                stop.wait(self.poll_interval)
