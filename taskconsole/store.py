"""TaskStore: all reads/writes of task state. Workers, the supervisor and the API only talk to this.

Concurrency model
-----------------
* Claiming is a compare-and-swap: pick a candidate, then
  `UPDATE … WHERE id=? AND status=<seen> RETURNING *`. Each claim bumps the
  task's fencing token (`fence`) and sets a lease.
* Every worker-side write carries (task_id, fence) and only applies if the task is
  still CLAIMED/RUNNING with that same fence. A worker whose lease expired (it
  crashed, hung, or was paused) can therefore never overwrite the state written
  by the worker that took over. Rejected writes are counted and logged.
* Expired leases are recovered inside the claim path (and by the supervisor
  tick): the task goes to RETRY_WAIT with backoff, or DEAD if attempts are exhausted.
"""

import hashlib
import json
import random
import threading
import time
import uuid
from contextlib import contextmanager

from . import db
from .states import ACTIVE, CLAIMABLE, TERMINAL, IllegalTransition, Status, check_transition


class TaskNotFound(Exception):
    pass


class LeaseLost(Exception):
    """The caller's fencing token is stale: the task was recovered or claimed by someone else."""


class IdempotencyConflict(Exception):
    """Same idempotency key re-used with a different request body."""


class TaskFinished(IllegalTransition):
    """The task already reached a terminal state other than the one requested."""


JSON_FIELDS = ("payload", "checkpoint", "result")


def _row_to_task(row) -> dict:
    t = dict(row)
    for f in JSON_FIELDS:
        if t.get(f) is not None:
            t[f] = json.loads(t[f])
    t["cancel_requested"] = bool(t["cancel_requested"])
    t.pop("request_hash", None)
    return t


class TaskStore:
    def __init__(self, path: str, clock=time.time, backoff_base: float = 1.0, backoff_cap: float = 30.0,
                 rng=random.random):
        self.path = path
        self.clock = clock
        self.backoff_base = backoff_base
        self.backoff_cap = backoff_cap
        self.rng = rng
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

    def _event(self, c, task_id, frm, to, attempt, worker_id, message, fence=None, kind="transition"):
        c.execute(
            "INSERT INTO task_events(task_id, ts, from_status, to_status, attempt, worker_id, fence, kind, message)"
            " VALUES (?,?,?,?,?,?,?,?,?)",
            (task_id, self.clock(), frm, to, attempt, worker_id, fence, kind, message),
        )

    def _incr(self, c, name, n=1):
        c.execute(
            "INSERT INTO counters(name, value) VALUES (?, ?)"
            " ON CONFLICT(name) DO UPDATE SET value = value + excluded.value",
            (name, n),
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
        if to not in ACTIVE:
            fields.setdefault("lease_expires_at", None)
        cols = ", ".join(f"{k}=?" for k in fields)
        cur = c.execute(
            f"UPDATE tasks SET {cols} WHERE id=? AND status=?",
            (*fields.values(), task["id"], frm),
        )
        if cur.rowcount != 1:
            raise IllegalTransition(f"task {task['id']} changed concurrently")
        self._event(
            c, task["id"], frm, str(to),
            fields.get("attempts", task["attempts"]),
            fields.get("worker_id", task["worker_id"]),
            message,
            fence=fields.get("fence", task["fence"]),
        )
        if to in TERMINAL:
            self._incr(c, f"tasks_{str(to).lower()}")

    def _load(self, c, task_id):
        row = c.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
        if row is None:
            raise TaskNotFound(task_id)
        return row

    def _fenced(self, task_id, fence, worker_id, action, fn):
        """Run fn(c, row) in a tx iff `fence` is still the live token for an active task.
        Otherwise record the rejected (stale) write and raise LeaseLost."""
        with self.tx() as c:
            row = self._load(c, task_id)
            if row["fence"] == fence and row["status"] in ACTIVE:
                return fn(c, row)
            msg = (f"REJECTED stale {action} from {worker_id} (fence {fence}; task is now "
                   f"{row['status']} with fence {row['fence']})")
            self._event(c, task_id, row["status"], row["status"], row["attempts"], worker_id, msg,
                        fence=fence, kind="rejected")
            self._incr(c, "fenced_writes_rejected")
        raise LeaseLost(msg)

    def backoff(self, attempt: int) -> float:
        """Exponential backoff with full jitter: uniform(0, min(cap, base * 2^(attempt-1)))."""
        # Cap the exponent too: 2 ** 1024 overflows a float, and 2 ** 63 already exceeds any cap.
        return min(self.backoff_cap, self.backoff_base * 2 ** min(attempt - 1, 63)) * self.rng()

    # -- client API -------------------------------------------------------

    def submit(self, type, payload=None, pool="default", priority=0, max_attempts=3, timeout_s=60.0,
               idempotency_key=None) -> tuple[dict, bool]:
        """Returns (task, created). With an idempotency key, a repeat submission returns the
        original task (created=False); a different body under the same key raises."""
        payload = payload or {}
        idempotency_key = idempotency_key or None  # "" is not a key; never store it in the UNIQUE column
        req_hash = hashlib.sha256(
            json.dumps([type, payload, pool, priority, max_attempts, timeout_s], sort_keys=True).encode()
        ).hexdigest()
        task_id = uuid.uuid4().hex[:12]
        now = self.clock()
        with self.tx() as c:
            if idempotency_key:
                row = c.execute("SELECT * FROM tasks WHERE idempotency_key=?", (idempotency_key,)).fetchone()
                if row is not None:
                    if row["request_hash"] != req_hash:
                        raise IdempotencyConflict(
                            f"idempotency key {idempotency_key!r} was used for a different request")
                    self._incr(c, "idempotent_submits_deduplicated")
                    self._event(c, row["id"], row["status"], row["status"], row["attempts"], None,
                                f"duplicate submit (key {idempotency_key!r}) returned existing task", kind="info")
                    return _row_to_task(row), False
            c.execute(
                "INSERT INTO tasks(id, type, payload, pool, priority, status, max_attempts, timeout_s,"
                " run_after, created_at, updated_at, idempotency_key, request_hash)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (task_id, type, json.dumps(payload), pool, priority, Status.CREATED,
                 max_attempts, timeout_s, now, now, now, idempotency_key, req_hash),
            )
            self._event(c, task_id, None, Status.CREATED, 0, None, "submitted")
            self._transition(c, self._load(c, task_id), Status.PENDING, f"enqueued to pool '{pool}'")
            self._incr(c, "tasks_submitted")
        return self.get(task_id), True

    def get(self, task_id) -> dict:
        return _row_to_task(self._load(self.conn(), task_id))

    def list_tasks(self, status=None, pool=None, limit=100, after=None) -> list[dict]:
        """Newest first. Keyset pagination: `after` is the (created_at, id) of the last task of the
        previous page, so rows inserted meanwhile never shift a page (no skips, no repeats)."""
        sql, args = "SELECT * FROM tasks WHERE 1=1", []
        if status:
            sql += " AND status=?"
            args.append(status)
        if pool:
            sql += " AND pool=?"
            args.append(pool)
        if after:
            sql += " AND (created_at < ? OR (created_at = ? AND id < ?))"
            args += [after[0], after[0], after[1]]
        sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
        args.append(limit)
        return [_row_to_task(r) for r in self.conn().execute(sql, args)]

    def events(self, task_id) -> list[dict]:
        rows = self.conn().execute("SELECT * FROM task_events WHERE task_id=? ORDER BY id", (task_id,))
        return [dict(r) for r in rows]

    def cancel(self, task_id) -> dict:
        """Idempotent: cancelling a CANCELLED task, or re-requesting a pending cancel, is a no-op."""
        with self.tx() as c:
            task = self._load(c, task_id)
            status = Status(task["status"])
            if status == Status.CANCELLED or (status in ACTIVE and task["cancel_requested"]):
                return _row_to_task(task)
            if status in TERMINAL:
                raise TaskFinished(f"task {task_id} already {status}")
            if status in ACTIVE:
                # Running code can't be stopped from outside: flag it, the worker sees it on its next heartbeat.
                c.execute("UPDATE tasks SET cancel_requested=1, updated_at=? WHERE id=?", (self.clock(), task_id))
                self._event(c, task_id, status, status, task["attempts"], task["worker_id"], "cancel requested",
                            fence=task["fence"], kind="info")
            else:
                self._transition(c, task, Status.CANCELLED, "cancelled by user", cancel_requested=1)
        return self.get(task_id)

    # -- worker API (everything below a claim is fenced) -------------------

    def claim(self, pool: str, worker_id: str, lease_s: float = 30.0) -> dict | None:
        """Atomically claim the best ready task in `pool`: bump attempts + fence, set a lease."""
        with self.tx() as c:
            now = self.clock()
            self._recover_expired(c, now)
            while True:
                cand = c.execute(
                    "SELECT id, status FROM tasks WHERE pool=? AND status IN (?,?) AND run_after<=?"
                    " ORDER BY priority DESC, run_after, created_at LIMIT 1",
                    (pool, *CLAIMABLE, now),
                ).fetchone()
                if cand is None:
                    return None
                row = c.execute(
                    "UPDATE tasks SET status=?, fence=fence+1, attempts=attempts+1, worker_id=?,"
                    " lease_expires_at=?, updated_at=? WHERE id=? AND status=? RETURNING *",
                    (Status.CLAIMED, worker_id, now + lease_s, now, cand["id"], cand["status"]),
                ).fetchone()
                if row is not None:  # CAS won (always, under SQLite's write lock; loop is for portability)
                    break
            self._event(c, row["id"], cand["status"], Status.CLAIMED, row["attempts"], worker_id,
                        f"claimed by {worker_id} (lease {lease_s:g}s)", fence=row["fence"])
            c.execute("UPDATE workers SET status='busy', current_task_id=?, last_heartbeat=? WHERE id=?",
                      (row["id"], now, worker_id))
            if row["attempts"] > 1:
                self._incr(c, "retries")
        return _row_to_task(row)

    def start(self, task_id, fence, worker_id) -> None:
        def go(c, task):
            self._transition(c, task, Status.RUNNING, f"attempt {task['attempts']} started",
                             started_at=self.clock())
        self._fenced(task_id, fence, worker_id, "start", go)

    def renew_lease(self, task_id, fence, worker_id, lease_s: float) -> bool:
        """Heartbeat for a task: extend the lease. Returns True if a cancel was requested."""
        def go(c, task):
            now = self.clock()
            c.execute("UPDATE tasks SET lease_expires_at=? WHERE id=?", (now + lease_s, task_id))
            c.execute("UPDATE workers SET last_heartbeat=? WHERE id=?", (now, worker_id))
            return bool(task["cancel_requested"])
        return self._fenced(task_id, fence, worker_id, "lease renewal", go)

    def report_progress(self, task_id, fence, worker_id, progress: float, checkpoint=None) -> bool:
        """Returns True if a cancel was requested."""
        def go(c, task):
            c.execute(
                "UPDATE tasks SET progress=?, checkpoint=COALESCE(?, checkpoint), updated_at=? WHERE id=?",
                (progress, None if checkpoint is None else json.dumps(checkpoint), self.clock(), task_id),
            )
            return bool(task["cancel_requested"])
        return self._fenced(task_id, fence, worker_id, "progress update", go)

    def complete(self, task_id, fence, worker_id, result=None) -> None:
        def go(c, task):
            self._transition(c, task, Status.SUCCEEDED, "succeeded", progress=1.0,
                             result=json.dumps(result), error=None)
            c.execute("UPDATE workers SET tasks_succeeded=tasks_succeeded+1 WHERE id=?", (worker_id,))
        self._fenced(task_id, fence, worker_id, "completion", go)

    def fail(self, task_id, fence, worker_id, error: str, retryable: bool = True) -> str:
        """Record a failed attempt. Returns the new status (RETRY_WAIT / FAILED / DEAD)."""
        def go(c, task):
            c.execute("UPDATE workers SET tasks_failed=tasks_failed+1 WHERE id=?", (worker_id,))
            return self._fail(c, task, error, retryable)
        return self._fenced(task_id, fence, worker_id, "failure", go)

    def _fail(self, c, task, error, retryable, prefix=""):
        attempts = task["attempts"]
        if task["cancel_requested"]:
            to, msg, extra = Status.CANCELLED, f"{prefix}cancelled (attempt failed: {error})", {}
        elif not retryable:
            to, msg, extra = Status.FAILED, f"{prefix}non-retryable: {error}", {}
        elif attempts >= task["max_attempts"]:
            to, msg, extra = Status.DEAD, f"{prefix}attempts exhausted ({attempts}/{task['max_attempts']}): {error}", {}
        else:
            delay = self.backoff(attempts)
            to, msg = Status.RETRY_WAIT, f"{prefix}retry in {delay:.2f}s (full jitter): {error}"
            extra = {"run_after": self.clock() + delay}
        self._transition(c, task, to, msg, error=error, **extra)
        return str(to)

    def mark_cancelled(self, task_id, fence, worker_id) -> None:
        def go(c, task):
            self._transition(c, task, Status.CANCELLED, "cancelled while running")
        self._fenced(task_id, fence, worker_id, "cancel", go)

    def release(self, task_id, fence, worker_id, reason: str) -> None:
        """Graceful hand-back (worker draining): back to PENDING, and the attempt doesn't count."""
        def go(c, task):
            if task["cancel_requested"]:
                self._transition(c, task, Status.CANCELLED, f"cancelled (released by {worker_id} while cancel requested)")
                return
            self._transition(c, task, Status.PENDING, f"released by {worker_id}: {reason}",
                             attempts=task["attempts"] - 1, run_after=self.clock(), worker_id=None)
        self._fenced(task_id, fence, worker_id, "release", go)

    def recover_expired(self) -> int:
        with self.tx() as c:
            return self._recover_expired(c, self.clock())

    def _recover_expired(self, c, now) -> int:
        rows = c.execute(
            "SELECT * FROM tasks WHERE status IN (?,?) AND lease_expires_at < ?", (*ACTIVE, now)
        ).fetchall()
        for task in rows:
            # A crash counts as an attempt, so a task that keeps killing workers ends up DEAD (poison pill).
            self._fail(c, task, f"lease expired: worker {task['worker_id']} stopped heartbeating", True,
                       prefix="recovered: ")
            self._incr(c, "lease_expirations")
        return len(rows)

    # -- idempotent side effects -----------------------------------------

    def perform_effect(self, task_id, fence, worker_id, step, kind, detail, crash_before_record=None):
        """Do an external side effect at most once per (task, step).

        Layer 1: our ledger (`effects`): if this step already completed on an earlier
                 attempt, skip it and return the recorded result.
        Layer 2: the provider gets `task_id:step` as its Idempotency-Key, which covers a
                 crash *between* the provider call and our ledger write.
        """
        key = f"{task_id}:{step}"

        def check_ledger(c, task):  # fenced: a zombie neither starts effects nor reads the ledger
            row = c.execute("SELECT result FROM effects WHERE key=?", (key,)).fetchone()
            if row is None:
                return None
            self._incr(c, "effects_skipped_already_done")
            self._event(c, task_id, task["status"], task["status"], task["attempts"], worker_id,
                        f"effect '{step}' already done on an earlier attempt: skipped", fence=fence, kind="info")
            return json.loads(row["result"])
        done = self._fenced(task_id, fence, worker_id, f"effect '{step}'", check_ledger)
        if done is not None:
            return done
        result = self.provider_call(key, kind, detail)
        if crash_before_record:
            crash_before_record()
        def go(c, task):
            c.execute("INSERT OR IGNORE INTO effects(key, task_id, kind, fence, result, created_at)"
                      " VALUES (?,?,?,?,?,?)", (key, task_id, kind, fence, json.dumps(result), self.clock()))
            self._event(c, task_id, task["status"], task["status"], task["attempts"], worker_id,
                        f"effect '{step}' done: {result['id']}", fence=fence, kind="info")
        self._fenced(task_id, fence, worker_id, f"effect '{step}' record", go)
        return result

    def provider_call(self, idempotency_key, kind, detail) -> dict:
        """Fake external provider (payments / email API) that dedups on Idempotency-Key."""
        with self.tx() as c:
            row = c.execute("SELECT * FROM external_effects WHERE idempotency_key=?", (idempotency_key,)).fetchone()
            if row is not None:
                c.execute("UPDATE external_effects SET duplicate_calls=duplicate_calls+1 WHERE idempotency_key=?",
                          (idempotency_key,))
                self._incr(c, "provider_duplicates_blocked")
                return {"id": f"{kind}_{idempotency_key}", "deduplicated_by_provider": True}
            c.execute("INSERT INTO external_effects(idempotency_key, kind, detail, created_at) VALUES (?,?,?,?)",
                      (idempotency_key, kind, json.dumps(detail), self.clock()))
            self._incr(c, f"external_{kind}s")
        return {"id": f"{kind}_{idempotency_key}", "deduplicated_by_provider": False}

    def external_effects(self, limit=50) -> list[dict]:
        rows = self.conn().execute("SELECT * FROM external_effects ORDER BY created_at DESC LIMIT ?", (limit,))
        return [{**dict(r), "detail": json.loads(r["detail"])} for r in rows]

    # -- workers ----------------------------------------------------------

    def register_worker(self, worker_id, pool, pid, status="starting"):
        with self.tx() as c:
            c.execute(
                "INSERT INTO workers(id, pool, pid, status, started_at, last_heartbeat) VALUES (?,?,?,?,?,?)"
                # Ids restart at <pool>-1 when the app restarts: a reused id is a new process, so
                # reset everything; a stale stopped_at would hide it from list_workers().
                " ON CONFLICT(id) DO UPDATE SET pool=excluded.pool, pid=excluded.pid, status=excluded.status,"
                " started_at=excluded.started_at, last_heartbeat=excluded.last_heartbeat, current_task_id=NULL,"
                " tasks_succeeded=0, tasks_failed=0, exit_code=NULL, stopped_at=NULL",
                (worker_id, pool, pid, status, self.clock(), self.clock()),
            )

    def worker_heartbeat(self, worker_id, status, current_task_id=None):
        with self.tx() as c:
            c.execute("UPDATE workers SET last_heartbeat=?, status=?, current_task_id=? WHERE id=?"
                      " AND status NOT IN ('stopped','dead')",
                      (self.clock(), status, current_task_id, worker_id))

    def worker_exited(self, worker_id, status, exit_code):
        with self.tx() as c:
            c.execute("UPDATE workers SET status=?, exit_code=?, stopped_at=?, current_task_id=NULL WHERE id=?",
                      (status, exit_code, self.clock(), worker_id))
            if status == "dead":
                self._incr(c, "worker_crashes")

    def incr(self, name, n=1):
        with self.tx() as c:
            self._incr(c, name, n)

    def list_workers(self, include_exited_s: float = 60.0) -> list[dict]:
        rows = self.conn().execute(
            "SELECT * FROM workers WHERE stopped_at IS NULL OR stopped_at > ? ORDER BY pool, started_at",
            (self.clock() - include_exited_s,),
        )
        return [dict(r) for r in rows]

    def counters(self) -> dict[str, int]:
        return {r["name"]: r["value"] for r in self.conn().execute("SELECT * FROM counters")}
