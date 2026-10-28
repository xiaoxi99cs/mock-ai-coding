"""One-click demo scenarios for the console.

Each scenario drives the real system (store + supervisor + worker processes) from a
background thread: it submits tasks, injects a fault (kill / pause / crash / hang ...),
waits for the system to react and logs every step, so the console can show what
happened next to the live task table and event timelines.

Waits poll the store in real time and are always bounded: a scenario that does not see
the expected outcome in time ends with status "error" (and says what it was waiting for).
"""

import itertools
import threading
import time
import traceback
import uuid


from . import handlers
from .errors import ApiError
from .states import TERMINAL, Status
from .store import IdempotencyConflict

MAX_RUNS = 20
POLL_S = 0.1

SCENARIOS: dict[str, dict] = {}


def scenario(name, title, description, watch):
    def deco(fn):
        SCENARIOS[name] = {"name": name, "title": title, "description": description, "watch": watch, "fn": fn}
        return fn
    return deco


class DemoError(Exception):
    pass


class Run:
    """One execution of a scenario: its log, the tasks it created, and helpers to wait on the store."""

    def __init__(self, name, store, sup, lock):
        self.run_id = uuid.uuid4().hex[:8]
        self.name = name
        self.store = store
        self.sup = sup
        self._lock = lock
        self.status = "running"
        self.started_at = store.clock()
        self.finished_at = None
        self.log_entries: list[dict] = []
        self.task_ids: list[str] = []
        self._followed: dict[str, int] = {}  # task_id -> last event id logged

    def to_dict(self):
        with self._lock:
            return {"run_id": self.run_id, "name": self.name, "status": self.status,
                    "started_at": self.started_at, "finished_at": self.finished_at,
                    "log": list(self.log_entries), "task_ids": list(self.task_ids)}

    def log(self, msg):
        with self._lock:
            self.log_entries.append({"t": self.store.clock(), "msg": msg})

    # -- tasks ------------------------------------------------------------

    def need_pool(self, pool, workers=1):
        have = self.sup.desired.get(pool)
        if have is None:
            raise DemoError(f"pool '{pool}' does not exist")
        if have < workers:
            raise DemoError(f"pool '{pool}' needs at least {workers} worker(s), has {have}: scale it up first")

    def submit(self, type, payload=None, follow=True, quiet=False, **kw):
        kw.setdefault("pool", handlers.REGISTRY[type]["pool"])
        task, created = self.store.submit(type, payload or {}, **kw)
        with self._lock:
            if task["id"] not in self.task_ids:
                self.task_ids.append(task["id"])
        if follow:
            self._followed.setdefault(task["id"], 0)
        if not quiet:
            self.log(f"submitted {type} task {task['id']} to pool '{task['pool']}'"
                     + ("" if created else " (deduplicated: existing task returned)"))
        return task, created

    def follow_events(self):
        """Log new events of followed tasks (the same entries the timeline shows)."""
        for tid, last in self._followed.items():
            rows = self.store.conn().execute(
                "SELECT * FROM task_events WHERE task_id=? AND id>? ORDER BY id", (tid, last)).fetchall()
            for e in rows:
                if e["kind"] == "transition" and e["from_status"] is not None:
                    self.log(f"task {tid}: {e['from_status']} -> {e['to_status']}: {e['message']}")
                elif e["kind"] != "transition":
                    self.log(f"task {tid}: {e['message']}")
                self._followed[tid] = e["id"]

    def wait(self, pred, timeout, what):
        """Poll `pred()` until it returns something truthy (returned) or `timeout` real seconds pass."""
        deadline = time.monotonic() + timeout
        while True:
            self.follow_events()
            value = pred()
            if value:
                self.follow_events()
                return value
            if time.monotonic() > deadline:
                raise DemoError(f"timed out after {timeout:g}s waiting for {what}")
            time.sleep(POLL_S)

    def wait_status(self, tid, statuses, timeout):
        statuses = {str(s) for s in ([statuses] if isinstance(statuses, (str, Status)) else statuses)}
        return self.wait(lambda: (t := self.store.get(tid))["status"] in statuses and t, timeout,
                         f"task {tid} to reach {'/'.join(sorted(statuses))}")

    def wait_terminal(self, tid, timeout):
        return self.wait_status(tid, TERMINAL, timeout)

    def expect(self, cond, msg):
        if not cond:
            raise DemoError(f"unexpected outcome: {msg}")

    def counter(self, name):
        return self.store.counters().get(name, 0)

    @property
    def lease_s(self):
        return self.sup.lease_s


def _fmt(s):
    return "n/a" if s is None else f"{s:.2f}s"


def _first_claim_ts(store, tid):
    row = store.conn().execute("SELECT MIN(ts) AS ts FROM task_events WHERE task_id=? AND to_status=?"
                               " AND kind='transition'", (tid, Status.CLAIMED)).fetchone()
    return row["ts"]


def _wait_running(run, tid, timeout):
    """Wait until `tid` is RUNNING with some progress; returns the task row."""
    t = run.wait(lambda: (t := run.store.get(tid))["status"] == Status.RUNNING and t["progress"] >= 0.2 and t,
                 timeout, f"task {tid} to be RUNNING")
    return t


# -- scenarios ----------------------------------------------------------------


@scenario("happy_path", "Happy path",
          "Submit a sleep task and a count_primes task; each is claimed, run and succeeds on its first attempt.",
          "Task table: PENDING -> CLAIMED -> RUNNING -> SUCCEEDED with live progress; the drawer timeline "
          "shows one attempt; the wait/run latency tiles and throughput chart pick them up.")
def happy_path(run):
    run.need_pool("default")
    run.need_pool("cpu")
    a, _ = run.submit("sleep", {"seconds": 2})
    b, _ = run.submit("count_primes", {"n": 200000, "chunks": 10})
    for t in (a, b):
        done = run.wait_terminal(t["id"], 30 + 2 * run.lease_s)
        run.expect(done["status"] == Status.SUCCEEDED, f"task {t['id']} ended {done['status']}")
        wait = _first_claim_ts(run.store, t["id"]) - done["created_at"]
        run.log(f"task {t['id']} SUCCEEDED on attempt {done['attempts']} by {done['worker_id']}: "
                f"waited {_fmt(wait)}, ran {_fmt(done['finished_at'] - done['started_at'])}, "
                f"result {done['result']}")


@scenario("retry_backoff", "Retry with backoff",
          "A flaky task fails twice with a transient error and succeeds on attempt 3; a fail_permanent task "
          "raises a non-retryable error and goes straight to FAILED.",
          "The flaky task alternates RUNNING -> RETRY_WAIT with a 'retry in Xs' countdown (exponential backoff, "
          "full jitter); the retries counter rises by 2. The other task ends FAILED after one attempt.")
def retry_backoff(run):
    run.need_pool("default")
    retries = run.counter("retries")
    flaky, _ = run.submit("flaky", {"fail_times": 2, "seconds": 0.5}, max_attempts=4)
    bad, _ = run.submit("fail_permanent", {"reason": "invalid account id"}, max_attempts=4)
    done = run.wait_terminal(flaky["id"], 30 + 2 * run.store.backoff_cap)
    run.expect(done["status"] == Status.SUCCEEDED and done["attempts"] == 3,
               f"flaky task ended {done['status']} after {done['attempts']} attempts")
    delays = [e["message"] for e in run.store.events(flaky["id"]) if e["to_status"] == Status.RETRY_WAIT
              and e["kind"] == "transition"]
    run.log(f"flaky task succeeded on attempt 3 after {len(delays)} backoffs "
            f"(cap of the jitter window doubles each attempt: {run.store.backoff_base:g}s, "
            f"{2 * run.store.backoff_base:g}s, ...)")
    failed = run.wait_terminal(bad["id"], 30)
    run.expect(failed["status"] == Status.FAILED and failed["attempts"] == 1,
               f"fail_permanent ended {failed['status']} after {failed['attempts']} attempts")
    run.log(f"fail_permanent task FAILED after 1 attempt (non-retryable: {failed['error']})")
    run.log(f"retries counter +{run.counter('retries') - retries}")


@scenario("crash_recovery", "Crash -> lease recovery",
          "Start a sleep task, then kill -9 the worker running it. Its lease expires, the task is recovered "
          "into RETRY_WAIT and re-run (from the start) by another worker; the supervisor replaces the dead worker.",
          "Workers panel: the killed worker turns dead and a replacement appears (worker_restarts +1). Task: the "
          "lease countdown runs out, then 'recovered: lease expired' -> RETRY_WAIT -> attempt 2 on another worker.")
def crash_recovery(run):
    run.need_pool("default", 2)
    restarts, expirations = run.counter("worker_restarts"), run.counter("lease_expirations")
    t, _ = run.submit("sleep", {"seconds": 4})
    t = _wait_running(run, t["id"], 30)
    victim = t["worker_id"]
    run.log(f"kill -9 worker {victim} while it runs task {t['id']} (progress {t['progress']:.0%}); "
            f"its lease expires within {run.lease_s:g}s")
    try:
        run.sup.kill(victim)
    except KeyError:
        raise DemoError(f"worker {victim} is not managed by the supervisor")
    done = run.wait_terminal(t["id"], 4 * run.lease_s + 30)
    run.expect(done["status"] == Status.SUCCEEDED and done["attempts"] == 2,
               f"task ended {done['status']} after {done['attempts']} attempts")
    run.wait(lambda: run.counter("worker_restarts") > restarts, 10, "the supervisor to restart the worker")
    run.log(f"task SUCCEEDED on attempt 2 by {done['worker_id']} (killed worker was {victim}); "
            f"lease_expirations +{run.counter('lease_expirations') - expirations}, "
            f"worker_restarts +{run.counter('worker_restarts') - restarts}")


@scenario("poison_pill", "Poison pill -> DEAD",
          "A task that kills its worker on every attempt. Each crash counts as an attempt (recovered via the "
          "lease), so after max_attempts=3 it is parked as DEAD instead of crashing workers forever.",
          "Workers panel: three workers die and are restarted. Task timeline: three 'recovered: lease expired' "
          "entries, the last one 'attempts exhausted (3/3)' -> DEAD.")
def poison_pill(run):
    run.need_pool("default")
    crashes = run.counter("worker_crashes")
    t, _ = run.submit("poison", {"seconds": 0.5}, max_attempts=3)
    done = run.wait_terminal(t["id"], 3 * (run.lease_s + 5) + 2 * run.store.backoff_cap)
    run.expect(done["status"] == Status.DEAD and done["attempts"] == 3,
               f"task ended {done['status']} after {done['attempts']} attempts")
    run.wait(lambda: run.counter("worker_crashes") >= crashes + 3, 10, "three worker crashes to be recorded")
    run.log(f"poison pill is DEAD after {done['attempts']} attempts; worker_crashes "
            f"+{run.counter('worker_crashes') - crashes}: {done['error']}")


@scenario("hang_timeout", "Hang -> hard timeout",
          "A handler that ignores its context and never returns (timeout_s=2, max_attempts=2). The worker's "
          "heartbeat loop enforces the hard timeout by recording the failure and exiting; the supervisor "
          "restarts it. The retry hangs too, so the task ends DEAD.",
          "Task: 'hard timeout after 2s' -> RETRY_WAIT, attempt 2 times out -> DEAD. Workers panel: two workers "
          "exit and are replaced.")
def hang_timeout(run):
    run.need_pool("default")
    restarts = run.counter("worker_restarts")
    t, _ = run.submit("hang", {"seconds": 3600}, timeout_s=2, max_attempts=2)
    done = run.wait_terminal(t["id"], 2 * (2 + 5) + run.store.backoff_cap + 2 * run.lease_s)
    run.expect(done["status"] == Status.DEAD and "hard timeout" in (done["error"] or ""),
               f"task ended {done['status']}: {done['error']}")
    run.wait(lambda: run.counter("worker_restarts") >= restarts + 2, 10, "the hung workers to be restarted")
    run.log(f"task DEAD after {done['attempts']} hard timeouts; worker_restarts "
            f"+{run.counter('worker_restarts') - restarts}")


@scenario("zombie_fencing", "Zombie worker -> fencing",
          "Freeze (SIGSTOP) the worker running a sleep task. Its lease expires and another worker claims the "
          "task with a higher fencing token. Then unfreeze the zombie: it still thinks it owns the task, but "
          "every write it attempts carries the old fence and is rejected; it notices and stops.",
          "Workers panel: the paused worker goes stale. Task timeline: a new claim with fence +1, then red "
          "'REJECTED stale ...' entries from the old worker; the fenced_writes_rejected counter rises.")
def zombie_fencing(run):
    run.need_pool("default", 2)
    rejected = run.counter("fenced_writes_rejected")
    t, _ = run.submit("sleep", {"seconds": 4})
    t = _wait_running(run, t["id"], 30)
    zombie, fence = t["worker_id"], t["fence"]
    run.log(f"pause (SIGSTOP) worker {zombie}: it holds task {t['id']} with fence {fence}")
    try:
        run.sup.pause(zombie)
    except KeyError:
        raise DemoError(f"worker {zombie} is not managed by the supervisor")
    try:
        t2 = run.wait(lambda: (x := run.store.get(t["id"]))["status"] in (Status.CLAIMED, Status.RUNNING)
                      and x["fence"] > fence and x,
                      3 * run.lease_s + run.store.backoff_cap + 10, "another worker to reclaim the task")
        run.log(f"task reclaimed by {t2['worker_id']} with fence {t2['fence']}; resuming zombie {zombie}")
    finally:
        try:
            run.sup.resume(zombie)
        except KeyError:
            pass
    run.wait(lambda: run.counter("fenced_writes_rejected") > rejected, 10 + 2 * run.lease_s,
             "the zombie's stale write to be rejected")
    done = run.wait_terminal(t["id"], 30 + 2 * run.lease_s)
    run.expect(done["status"] == Status.SUCCEEDED and done["worker_id"] != zombie,
               f"task ended {done['status']} owned by {done['worker_id']}")
    run.log(f"task SUCCEEDED by {done['worker_id']} (fence {done['fence']}); the zombie's writes were rejected: "
            f"fenced_writes_rejected +{run.counter('fenced_writes_rejected') - rejected}")


@scenario("idempotent_submit", "Idempotent submit",
          "Submit a task with an idempotency key, submit the identical request again (the existing task is "
          "returned, nothing new is queued), then re-use the key with a different body (rejected, HTTP 409).",
          "Task table: only one task for the key; its timeline has a 'duplicate submit' info entry. The "
          "dedup-hits counter (idempotent_submits_deduplicated) rises by 1.")
def idempotent_submit(run):
    run.need_pool("default")
    key = f"demo-{run.run_id}"
    dedup = run.counter("idempotent_submits_deduplicated")
    a, created = run.submit("sleep", {"seconds": 1}, idempotency_key=key)
    run.expect(created, "first submit was deduplicated")
    run.log(f"submit #2 with the same key {key!r} and the same body")
    b, created = run.submit("sleep", {"seconds": 1}, idempotency_key=key)
    run.expect(not created and b["id"] == a["id"], "duplicate submit created a new task")
    run.log(f"same task {b['id']} returned; nothing new queued")
    run.log(f"submit #3 with key {key!r} but a different body (seconds=2)")
    try:
        run.submit("sleep", {"seconds": 2}, idempotency_key=key)
    except IdempotencyConflict as e:
        run.log(f"rejected (HTTP 409 via the API): {e}")
    else:
        raise DemoError("a different body under the same key was accepted")
    done = run.wait_terminal(a["id"], 30 + 2 * run.lease_s)
    run.expect(done["status"] == Status.SUCCEEDED, f"task ended {done['status']}")
    n = run.store.conn().execute("SELECT COUNT(*) AS n FROM tasks WHERE idempotency_key=?", (key,)).fetchone()["n"]
    run.log(f"task SUCCEEDED; {n} task(s) exist for the key; idempotent_submits_deduplicated "
            f"+{run.counter('idempotent_submits_deduplicated') - dedup}")


@scenario("no_double_charge", "Charge + crash -> no double charge",
          "Two charge_and_email tasks whose worker dies on attempt 1: one right after the charge was recorded in "
          "our ledger, one after the provider charged but before our ledger saw it. Both are retried and "
          "succeed, and each customer is charged exactly once.",
          "Side-effects ledger: one charge + one email per task; the 'before_record' charge shows "
          "duplicate_calls=1 (blocked by the provider's idempotency key). Timelines: 'already done ... skipped'.")
def no_double_charge(run):
    run.need_pool("default")
    skipped, blocked = run.counter("effects_skipped_already_done"), run.counter("provider_duplicates_blocked")
    tasks = []
    for mode, customer in (("after_charge", "cus_ada"), ("before_record", "cus_bob")):
        t, _ = run.submit("charge_and_email", {"customer": customer, "amount_cents": 1999,
                                               "email": f"{customer}@example.com", "crash": mode, "seconds": 0.5})
        tasks.append((mode, t))
    for mode, t in tasks:
        done = run.wait_terminal(t["id"], 2 * (run.lease_s + 5) + run.store.backoff_cap)
        run.expect(done["status"] == Status.SUCCEEDED and done["attempts"] == 2,
                   f"{mode} task ended {done['status']} after {done['attempts']} attempts")
    for mode, t in tasks:
        rows = run.store.conn().execute(
            "SELECT * FROM external_effects WHERE idempotency_key LIKE ? ORDER BY created_at",
            (f"{t['id']}:%",)).fetchall()
        charges = [r for r in rows if r["kind"] == "charge"]
        run.expect(len(charges) == 1, f"{mode} task has {len(charges)} charges")
        for r in rows:
            run.log(f"provider ledger [{mode}]: {r['kind']} key={r['idempotency_key']} "
                    f"duplicate_calls={r['duplicate_calls']}")
    run.log(f"exactly one charge per task; effects_skipped_already_done "
            f"+{run.counter('effects_skipped_already_done') - skipped}, provider_duplicates_blocked "
            f"+{run.counter('provider_duplicates_blocked') - blocked}")


@scenario("cancel_running", "Cancel a running task",
          "Start a 10s sleep task and cancel it mid-run. Running code can't be stopped from outside, so the "
          "cancel sets a flag; the worker relays it on its next heartbeat and the handler stops at its next "
          "checkpoint.",
          "Task: 'cancel requested' info entry while RUNNING, then RUNNING -> CANCELLED within about a "
          "heartbeat; the worker goes back to idle.")
def cancel_running(run):
    run.need_pool("default")
    t, _ = run.submit("sleep", {"seconds": 10})
    t = _wait_running(run, t["id"], 30)
    run.log(f"cancel task {t['id']} at progress {t['progress']:.0%} (worker {t['worker_id']})")
    asked = time.monotonic()
    run.store.cancel(t["id"])
    done = run.wait_terminal(t["id"], 10 + 2 * run.lease_s)
    run.expect(done["status"] == Status.CANCELLED, f"task ended {done['status']}")
    run.log(f"task CANCELLED {time.monotonic() - asked:.2f}s after the request, at progress "
            f"{done['progress']:.0%}")


@scenario("multi_pool_burst", "Multi-pool burst",
          "Burst 20 sleep tasks into the default pool and 8 count_primes tasks into the cpu pool. Each pool's "
          "workers drain their own queue independently; atomic claiming means no task runs twice.",
          "Queue-depth tiles per pool fill up and drain; throughput chart spikes; every worker in each pool "
          "is busy. Try scaling a pool +/- while it runs.")
def multi_pool_burst(run):
    run.need_pool("default")
    run.need_pool("cpu")
    ids = []
    for i in range(20):
        t, _ = run.submit("sleep", {"seconds": 1 + (i % 3) * 0.5}, follow=False, quiet=True)
        ids.append(t["id"])
    for i in range(8):
        t, _ = run.submit("count_primes", {"n": 150000 + 10000 * i, "chunks": 10}, follow=False, quiet=True)
        ids.append(t["id"])
    run.log(f"submitted 20 sleep tasks (pool 'default', {run.sup.desired['default']} workers) and 8 count_primes "
            f"tasks (pool 'cpu', {run.sup.desired['cpu']} workers)")
    marks = ",".join("?" * len(ids))
    last = [0.0]

    def all_done():
        rows = run.store.conn().execute(
            f"SELECT pool, status, COUNT(*) AS n FROM tasks WHERE id IN ({marks}) GROUP BY pool, status",
            ids).fetchall()
        if time.monotonic() - last[0] >= 3:
            last[0] = time.monotonic()
            by_pool = {}
            for r in rows:
                by_pool.setdefault(r["pool"], []).append(f"{r['status']}={r['n']}")
            run.log("progress: " + "; ".join(f"{p}: {', '.join(v)}" for p, v in sorted(by_pool.items())))
        return all(r["status"] in TERMINAL for r in rows)

    run.wait(all_done, 120 + 4 * run.lease_s, "the burst to finish")
    rows = run.store.conn().execute(
        f"SELECT pool, worker_id, COUNT(*) AS n, MIN(created_at) AS t0, MAX(finished_at) AS t1,"
        f" SUM(attempts) AS attempts, SUM(status='SUCCEEDED') AS ok FROM tasks WHERE id IN ({marks})"
        f" GROUP BY pool, worker_id ORDER BY pool, worker_id", ids).fetchall()
    pools = {}
    for r in rows:
        p = pools.setdefault(r["pool"], {"n": 0, "ok": 0, "t0": r["t0"], "t1": r["t1"], "workers": []})
        p["n"] += r["n"]
        p["ok"] += r["ok"]
        p["t0"], p["t1"] = min(p["t0"], r["t0"]), max(p["t1"], r["t1"])
        p["workers"].append(f"{r['worker_id']}={r['n']}")
    for pool, p in sorted(pools.items()):
        span = max(p["t1"] - p["t0"], 1e-9)
        run.log(f"pool '{pool}': {p['ok']}/{p['n']} succeeded in {span:.1f}s ({p['n'] / span:.2f} tasks/s); "
                f"per worker: {', '.join(p['workers'])}")
    run.expect(sum(p["ok"] for p in pools.values()) == len(ids), "not every burst task succeeded")


# -- runner + API ---------------------------------------------------------------


class DemoRunner:
    def __init__(self, store, sup):
        self.store = store
        self.sup = sup
        self._lock = threading.RLock()
        self._runs: list[Run] = []
        self._threads = itertools.count(1)

    def start(self, name) -> dict:
        """Start scenario `name` in a background thread; returns the run as it was at start."""
        spec = SCENARIOS[name]
        run = Run(name, self.store, self.sup, self._lock)
        with self._lock:
            self._runs.insert(0, run)
            del self._runs[MAX_RUNS:]
        started = run.to_dict()
        threading.Thread(target=self._execute, args=(spec, run), daemon=True,
                         name=f"demo-{name}-{next(self._threads)}").start()
        return started

    def _execute(self, spec, run):
        run.log(f"demo '{spec['title']}' started")
        try:
            spec["fn"](run)
        except DemoError as e:
            run.log(f"error: {e}")
            status = "error"
        except Exception as e:  # noqa: BLE001 - report it in the run log, don't die silently
            run.log(f"error: {type(e).__name__}: {e}")
            run.log(traceback.format_exc(limit=3))
            status = "error"
        else:
            run.log("done")
            status = "done"
        with self._lock:
            run.status = status
            run.finished_at = self.store.clock()

    def runs(self) -> list[dict]:
        with self._lock:
            runs = list(self._runs)
        return [r.to_dict() for r in runs]


def register(app, store, sup):
    runner = DemoRunner(store, sup)
    app.state.demos = runner

    @app.get("/api/demos")
    def list_demos():
        return [{k: v for k, v in spec.items() if k != "fn"} for spec in SCENARIOS.values()]

    @app.get("/api/demos/runs")
    def demo_runs():
        return runner.runs()

    @app.post("/api/demos/{name}", status_code=202)
    def start_demo(name: str):
        if name not in SCENARIOS:
            raise ApiError(404, "demo_not_found", f"demo {name} not found", {"demos": sorted(SCENARIOS)})
        return runner.start(name)
