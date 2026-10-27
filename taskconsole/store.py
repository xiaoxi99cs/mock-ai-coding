"""TaskStore: all reads/writes of task state. Workers and the API only talk to this."""

import json
import threading
import time
import uuid
from contextlib import contextmanager

from . import db
from .states import ACTIVE, CLAIMABLE, TERMINAL, IllegalTransition, Status, check_transition


class TaskNotFound(Exception):
    pass


class NotOwner(Exception):
    """A worker tried to write to a task it does not (or no longer) own."""


JSON_FIELDS = ("payload", "checkpoint", "result")


def _row_to_task(row) -> dict:
    t = dict(row)
    for f in JSON_FIELDS:
        if t.get(f) is not None:
            t[f] = json.loads(t[f])
    t["cancel_requested"] = bool(t["cancel_requested"])
    return t


class TaskStore:
    def __init__(self, path: str, clock=time.time, backoff_base: float = 1.0, backoff_cap: float = 30.0):
        self.path = path
        self.clock = clock
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self._local = threading.local()
        db.init(path)

    # -- plumbing ---------------------------------------------------------

    def conn(self):
        c = getattr(self._local, "conn", None)
        if c is None:
            c = db.connect(self.path)
            self._local.conn = c
        return c

    @contextmanager
    def tx(self):
        c = self.conn()
        c.execute("BEGIN IMMEDIATE")
        try:
            yield c
        except BaseException:
            c.execute("ROLLBACK")
            raise
        else:
            c.execute("COMMIT")

    def _event(self, c, task_id, frm, to, attempt, worker_id, message):
        c.execute(
            "INSERT INTO task_events(task_id, ts, from_status, to_status, attempt, worker_id, message)"
            " VALUES (?,?,?,?,?,?,?)",
            (task_id, self.clock(), frm, to, attempt, worker_id, message),
        )

    def _transition(self, c, task, to, message=None, **fields):
        """Move `task` (a row read inside this tx) to status `to`, guarded by the state machine
        and by an optimistic `AND status=<old>` check."""
        frm = task["status"]
        check_transition(frm, to)
        now = self.clock()
        fields.update(status=str(to), updated_at=now)
        if to in TERMINAL:
            fields.setdefault("finished_at", now)
        cols = ", ".join(f"{k}=?" for k in fields)
        cur = c.execute(
            f"UPDATE tasks SET {cols} WHERE id=? AND status=?",
            (*fields.values(), task["id"], frm),
        )
        if cur.rowcount != 1:
            raise IllegalTransition(f"task {task['id']} changed concurrently")
        self._event(
            c,
            task["id"],
            frm,
            str(to),
            fields.get("attempts", task["attempts"]),
            fields.get("worker_id", task["worker_id"]),
            message,
        )

    def _load(self, c, task_id):
        row = c.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise TaskNotFound(task_id)
        return row

    def _owned(self, c, task_id, worker_id):
        row = self._load(c, task_id)
        if row["worker_id"] != worker_id or row["status"] not in ACTIVE:
            raise NotOwner(f"{worker_id} does not own task {task_id} (status {row['status']})")
        return row

    def backoff(self, attempt: int) -> float:
        return min(self.backoff_cap, self.backoff_base * 2 ** (attempt - 1))

    # -- client API -------------------------------------------------------

    def submit(self, type, payload=None, pool="default", priority=0, max_attempts=3, timeout_s=60.0) -> dict:
        task_id = uuid.uuid4().hex[:12]
        now = self.clock()
        with self.tx() as c:
            c.execute(
                "INSERT INTO tasks(id, type, payload, pool, priority, status, max_attempts, timeout_s,"
                " run_after, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, type, json.dumps(payload or {}), pool, priority, Status.CREATED,
                 max_attempts, timeout_s, now, now, now),
            )
            self._event(c, task_id, None, Status.CREATED, 0, None, "submitted")
            self._transition(c, self._load(c, task_id), Status.PENDING, f"enqueued to pool '{pool}'")
        return self.get(task_id)

    def get(self, task_id) -> dict:
        return _row_to_task(self._load(self.conn(), task_id))

    def list_tasks(self, status=None, pool=None, limit=100, offset=0) -> list[dict]:
        sql, args = "SELECT * FROM tasks WHERE 1=1", []
        if status:
            sql += " AND status=?"
            args.append(status)
        if pool:
            sql += " AND pool=?"
            args.append(pool)
        sql += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        args += [limit, offset]
        return [_row_to_task(r) for r in self.conn().execute(sql, args)]

    def events(self, task_id) -> list[dict]:
        rows = self.conn().execute("SELECT * FROM task_events WHERE task_id=? ORDER BY id", (task_id,))
        return [dict(r) for r in rows]

    def cancel(self, task_id) -> dict:
        with self.tx() as c:
            task = self._load(c, task_id)
            status = Status(task["status"])
            if status in TERMINAL:
                raise IllegalTransition(f"task already {status}")
            if status in ACTIVE:
                # Running code can't be stopped from outside: flag it, the worker sees it on its next check.
                c.execute("UPDATE tasks SET cancel_requested=1, updated_at=? WHERE id=?", (self.clock(), task_id))
                self._event(c, task_id, status, status, task["attempts"], task["worker_id"], "cancel requested")
            else:
                self._transition(c, task, Status.CANCELLED, "cancelled by user", cancel_requested=1)
        return self.get(task_id)

    # -- worker API -------------------------------------------------------

    def claim(self, pool: str, worker_id: str) -> dict | None:
        """Phase 1: single worker, so select-then-update is fine."""
        now = self.clock()
        with self.tx() as c:
            row = c.execute(
                "SELECT * FROM tasks WHERE pool=? AND status IN (?,?) AND run_after<=?"
                " ORDER BY priority DESC, run_after, created_at LIMIT 1",
                (pool, *CLAIMABLE, now),
            ).fetchone()
            if row is None:
                return None
            self._transition(
                c, row, Status.CLAIMED, f"claimed by {worker_id}",
                worker_id=worker_id, attempts=row["attempts"] + 1,
            )
        return self.get(row["id"])

    def start(self, task_id, worker_id) -> None:
        with self.tx() as c:
            task = self._owned(c, task_id, worker_id)
            self._transition(c, task, Status.RUNNING, f"attempt {task['attempts']} started",
                             started_at=self.clock())

    def report_progress(self, task_id, worker_id, progress: float, checkpoint=None) -> bool:
        """Returns True if a cancel was requested."""
        with self.tx() as c:
            self._owned(c, task_id, worker_id)
            row = c.execute(
                "UPDATE tasks SET progress=?, checkpoint=COALESCE(?, checkpoint), updated_at=?"
                " WHERE id=? RETURNING cancel_requested",
                (progress, None if checkpoint is None else json.dumps(checkpoint), self.clock(), task_id),
            ).fetchone()
        return bool(row["cancel_requested"])

    def complete(self, task_id, worker_id, result=None) -> None:
        with self.tx() as c:
            task = self._owned(c, task_id, worker_id)
            self._transition(c, task, Status.SUCCEEDED, "succeeded", progress=1.0,
                             result=json.dumps(result), error=None)

    def fail(self, task_id, worker_id, error: str, retryable: bool = True) -> str:
        """Record a failed attempt. Returns the new status (RETRY_WAIT / FAILED / DEAD)."""
        with self.tx() as c:
            task = self._owned(c, task_id, worker_id)
            return self._fail(c, task, error, retryable)

    def _fail(self, c, task, error, retryable):
        attempts = task["attempts"]
        if not retryable:
            to, msg, extra = Status.FAILED, f"non-retryable: {error}", {}
        elif attempts >= task["max_attempts"]:
            to, msg, extra = Status.DEAD, f"attempts exhausted ({attempts}/{task['max_attempts']}): {error}", {}
        else:
            delay = self.backoff(attempts)
            to, msg = Status.RETRY_WAIT, f"retry in {delay:.1f}s: {error}"
            extra = {"run_after": self.clock() + delay}
        self._transition(c, task, to, msg, error=error, **extra)
        return str(to)

    def mark_cancelled(self, task_id, worker_id) -> None:
        with self.tx() as c:
            task = self._owned(c, task_id, worker_id)
            self._transition(c, task, Status.CANCELLED, "cancelled while running")
