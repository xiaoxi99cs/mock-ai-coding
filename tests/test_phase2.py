import threading
import time

import pytest
from fastapi.testclient import TestClient

from taskconsole import handlers, supervisor
from taskconsole.api import create_app
from taskconsole.handlers import TaskContext
from taskconsole.store import IdempotencyConflict, LeaseLost, TaskStore
from taskconsole.supervisor import Supervisor
from taskconsole.worker import Worker


class SimulatedCrash(BaseException):
    pass


@pytest.fixture
def crashy(monkeypatch):
    def boom():
        raise SimulatedCrash()
    monkeypatch.setattr(handlers, "crash", boom)


def run_until_crash(store, task, worker_id):
    """Run a handler inline on a claimed task until it 'crashes' (no outcome is recorded, like kill -9)."""
    store.start(task["id"], task["fence"], worker_id)
    ctx = TaskContext(store, task, worker_id)
    with pytest.raises(SimulatedCrash):
        handlers.REGISTRY[task["type"]]["fn"](ctx, task["payload"])


# -- 1. atomic claiming ----------------------------------------------------

def test_concurrent_claims_never_double_claim(tmp_path):
    store = TaskStore(str(tmp_path / "c.db"))
    ids = {store.submit("sleep", {"seconds": 0})[0]["id"] for _ in range(60)}
    claimed, lock = [], threading.Lock()

    def grab(n):
        s = TaskStore(store.path)
        while (t := s.claim("default", f"w{n}", lease_s=60)) is not None:
            with lock:
                claimed.append(t["id"])

    threads = [threading.Thread(target=grab, args=(i,)) for i in range(8)]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert sorted(claimed) == sorted(ids)  # every task exactly once


# -- 2. backoff with full jitter -------------------------------------------

def test_full_jitter_backoff_bounds(tmp_path):
    store = TaskStore(str(tmp_path / "j.db"), backoff_base=1, backoff_cap=8)
    for attempt, ceiling in [(1, 1), (2, 2), (3, 4), (4, 8), (10, 8)]:
        delays = [store.backoff(attempt) for _ in range(200)]
        assert all(0 <= d <= ceiling for d in delays)
        assert max(delays) > ceiling * 0.8 and min(delays) < ceiling * 0.2  # actually spread out


# -- 3. leases: recovery from a crashed worker -------------------------------

def test_expired_lease_is_recovered_and_reclaimed(store, clock):
    t = store.submit_task("sleep", {"seconds": 0})
    c1 = store.claim("default", "w1", lease_s=5)
    store.start(t["id"], c1["fence"], "w1")
    # w1 dies: no heartbeats. Before expiry nothing happens.
    clock.advance(4)
    assert store.claim("default", "w2", lease_s=5) is None
    clock.advance(2)
    store.recover_expired()
    assert store.get(t["id"])["status"] == "RETRY_WAIT"
    clock.advance(10)
    c2 = store.claim("default", "w2", lease_s=5)
    assert c2["id"] == t["id"] and c2["attempts"] == 2 and c2["fence"] == c1["fence"] + 1
    assert any("lease expired" in (e["message"] or "") for e in store.events(t["id"]))
    assert store.counters()["lease_expirations"] == 1


def test_heartbeat_keeps_lease_alive(store, clock):
    t = store.submit_task("sleep", {})
    c1 = store.claim("default", "w1", lease_s=5)
    for _ in range(5):
        clock.advance(3)
        store.renew_lease(t["id"], c1["fence"], "w1", 5)
        assert store.recover_expired() == 0
    assert store.get(t["id"])["status"] == "CLAIMED"


def test_poison_pill_ends_dead(store, clock):
    t = store.submit_task("poison", {}, max_attempts=3)
    for i in range(3):
        c = store.claim("default", f"w{i}", lease_s=5)
        assert c is not None and c["attempts"] == i + 1
        clock.advance(6)  # worker crashed, lease expires
        store.recover_expired()
        clock.advance(100)  # past the retry backoff
    t = store.get(t["id"])
    assert t["status"] == "DEAD" and t["attempts"] == 3


def test_cancel_with_dead_worker_resolves_on_recovery(store, clock):
    t = store.submit_task("sleep", {})
    store.claim("default", "w1", lease_s=5)
    store.cancel(t["id"])
    clock.advance(6)
    store.recover_expired()
    assert store.get(t["id"])["status"] == "CANCELLED"


# -- 4. fencing ----------------------------------------------------------------

def test_zombie_worker_writes_are_rejected(store, clock):
    t = store.submit_task("sleep", {})
    old = store.claim("default", "w1", lease_s=5)
    store.start(t["id"], old["fence"], "w1")
    clock.advance(6)  # w1 paused (GC pause / SIGSTOP / partition)
    store.recover_expired()
    clock.advance(60)
    new = store.claim("default", "w2", lease_s=5)
    store.start(t["id"], new["fence"], "w2")
    # w1 wakes up and tries to finish / heartbeat with its old token
    for write in (lambda: store.complete(t["id"], old["fence"], "w1", {"from": "zombie"}),
                  lambda: store.renew_lease(t["id"], old["fence"], "w1", 5),
                  lambda: store.report_progress(t["id"], old["fence"], "w1", 0.9)):
        with pytest.raises(LeaseLost):
            write()
    store.complete(t["id"], new["fence"], "w2", {"from": "w2"})
    t = store.get(t["id"])
    assert t["status"] == "SUCCEEDED" and t["result"] == {"from": "w2"}
    assert store.counters()["fenced_writes_rejected"] == 3
    assert sum(e["kind"] == "rejected" for e in store.events(t["id"])) == 3


def test_worker_stops_when_lease_lost(store, clock):
    t = store.submit_task("sleep", {"seconds": 0})
    c = store.claim("default", "w1", lease_s=5)
    store.start(t["id"], c["fence"], "w1")
    clock.advance(6)
    store.recover_expired()
    ctx = TaskContext(store, c, "w1")
    with pytest.raises(LeaseLost):
        ctx.progress(0.5)


# -- 5. idempotency ------------------------------------------------------------

def test_idempotent_submit(store):
    a, created_a = store.submit("sleep", {"seconds": 1}, idempotency_key="order-1")
    b, created_b = store.submit("sleep", {"seconds": 1}, idempotency_key="order-1")
    assert created_a and not created_b and a["id"] == b["id"]
    with pytest.raises(IdempotencyConflict):
        store.submit("sleep", {"seconds": 2}, idempotency_key="order-1")
    assert store.counters()["idempotent_submits_deduplicated"] == 1


@pytest.mark.parametrize("mode, provider_dupes", [("after_charge", 0), ("before_record", 1)])
def test_no_double_charge_after_crash(store, clock, crashy, mode, provider_dupes):
    t = store.submit_task("charge_and_email", {"customer": "c1", "amount_cents": 500, "email": "a@b.c",
                                               "crash": mode, "seconds": 0})
    run_until_crash(store, store.claim("default", "w1", lease_s=5), "w1")
    clock.advance(6)
    store.recover_expired()
    clock.advance(60)
    Worker(store, worker_id="w2", lease_s=5).run_one()
    t = store.get(t["id"])
    assert t["status"] == "SUCCEEDED" and t["attempts"] == 2
    charges = [e for e in store.external_effects() if e["kind"] == "charge"]
    emails = [e for e in store.external_effects() if e["kind"] == "email"]
    assert len(charges) == 1 and len(emails) == 1
    assert charges[0]["duplicate_calls"] == provider_dupes


def test_graceful_release_does_not_count_attempt(store):
    t = store.submit_task("sleep", {})
    c = store.claim("default", "w1", lease_s=5)
    store.start(t["id"], c["fence"], "w1")
    store.release(t["id"], c["fence"], "w1", "worker draining")
    t = store.get(t["id"])
    assert t["status"] == "PENDING" and t["attempts"] == 0


def test_api_idempotency_and_pools(tmp_path):
    app = create_app(str(tmp_path / "api.db"), start_workers=False)
    with TestClient(app) as c:
        body = {"type": "sleep", "payload": {"seconds": 1}, "idempotency_key": "k1"}
        r1, r2 = c.post("/api/tasks", json=body), c.post("/api/tasks", json=body)
        assert (r1.status_code, r2.status_code) == (201, 200)
        assert r1.json()["id"] == r2.json()["id"] and r2.json()["deduplicated"]
        assert c.post("/api/tasks", json={**body, "payload": {"seconds": 9}}).status_code == 409
        assert c.post("/api/tasks", json={"type": "sleep", "pool": "nope"}).status_code == 422
        assert c.put("/api/pools/default", json={"size": 5}).json()["default"] == 5


# -- 6. worker registry across supervisor restarts ------------------------------

class FakePopen:
    pids = iter(range(1000, 2000))

    def __init__(self, cmd, cwd=None):
        self.pid = next(self.pids)

    def poll(self):
        return None


def test_restarted_supervisor_never_reuses_worker_ids(tmp_path, clock, monkeypatch):
    """Regression: ids restarted at `<pool>-1`, so a new process inherited the old row (its
    stopped_at, exit code and counters) and vanished from the worker list 60s later."""
    monkeypatch.setattr(supervisor.subprocess, "Popen", FakePopen)
    path = str(tmp_path / "w.db")

    first = Supervisor(TaskStore(path, clock=clock), path, {"default": 2})
    first.tick()
    old_ids = set(first.procs)
    first.store.worker_exited("default-1", "stopped", 0)  # one exited cleanly; the other is orphaned

    clock.advance(10)
    store = TaskStore(path, clock=clock)  # app restart: fresh store, fresh supervisor
    second = Supervisor(store, path, {"default": 2})
    second.store.mark_orphaned_workers()  # what start() does before its first tick
    second.tick()
    assert old_ids.isdisjoint(second.procs)

    clock.advance(61)  # past list_workers' 60s window for exited workers
    rows = {w["id"]: w for w in store.list_workers()}
    assert set(rows) == set(second.procs)  # old rows aged out, new workers still listed
    assert all(w["stopped_at"] is None and w["tasks_succeeded"] == 0 for w in rows.values())


def test_orphaned_workers_are_marked_dead_at_startup(store):
    store.register_worker("default-1", "default", 1, "busy")
    store.register_worker("default-2", "default", 2, "idle")
    store.worker_exited("default-2", "stopped", 0)
    assert store.mark_orphaned_workers() == 1
    assert {w["id"]: w["status"] for w in store.list_workers()} == {"default-1": "dead", "default-2": "stopped"}


# -- integration: real worker processes + supervisor -------------------------

def wait_for(pred, timeout=20, interval=0.2):
    end = time.time() + timeout
    while time.time() < end:
        if (v := pred()):
            return v
        time.sleep(interval)
    raise AssertionError("condition not met in time")


def test_processes_crash_recovery_fencing_and_timeout(tmp_path):
    db = str(tmp_path / "it.db")
    app = create_app(db, pools={"default": 2}, lease_s=1.0, heartbeat_s=0.25)
    store, sup = app.state.store, app.state.supervisor
    with TestClient(app) as c:
        wait_for(lambda: len([w for w in c.get("/api/workers").json() if w["state"] == "idle"]) == 2)

        # crash mid-task -> worker restarted, task recovered via lease and finished on attempt 2
        crash_id = c.post("/api/tasks", json={"type": "crash", "payload": {"seconds": 1, "crash_on_attempts": [1]}}).json()["id"]
        t = wait_for(lambda: (t := store.get(crash_id))["status"] == "SUCCEEDED" and t)
        assert t["attempts"] == 2
        wait_for(lambda: store.counters().get("worker_restarts", 0) >= 1)

        # hung handler -> hard timeout -> worker exits and is replaced; max_attempts=1 -> DEAD
        hang_id = c.post("/api/tasks", json={"type": "hang", "max_attempts": 1, "timeout_s": 1}).json()["id"]
        t = wait_for(lambda: (t := store.get(hang_id))["status"] == "DEAD" and t)
        assert "hard timeout" in t["error"]

        # pause the worker running a task -> lease expires -> another worker takes over;
        # resume -> the zombie's writes are fenced off
        sid = c.post("/api/tasks", json={"type": "sleep", "payload": {"seconds": 3}}).json()["id"]
        owner = wait_for(lambda: (t := store.get(sid))["status"] == "RUNNING" and t["worker_id"])
        sup.pause(owner)
        wait_for(lambda: (t := store.get(sid))["status"] == "RUNNING" and t["worker_id"] != owner)
        sup.resume(owner)
        assert wait_for(lambda: store.get(sid)["status"] == "SUCCEEDED")
        wait_for(lambda: store.counters().get("fenced_writes_rejected", 0) >= 1)
        assert store.get(sid)["worker_id"] != owner

        # drain: graceful exit, replaced to keep the pool at size
        victim = next(w["id"] for w in c.get("/api/workers").json() if w["state"] == "idle")
        c.post(f"/api/workers/{victim}/drain")
        wait_for(lambda: any(w["id"] == victim and w["status"] == "stopped" for w in c.get("/api/workers").json()))
        wait_for(lambda: len(sup.snapshot()) == 2)


def test_worker_survives_transient_renew_error(tmp_path, monkeypatch):
    import sqlite3
    store = TaskStore(str(tmp_path / "r.db"))
    t, _ = store.submit("sleep", {"seconds": 0.6})
    real, calls = store.renew_lease, []

    def flaky_renew(*a):
        calls.append(1)
        if len(calls) == 1:
            raise sqlite3.OperationalError("database is locked")
        return real(*a)

    monkeypatch.setattr(store, "renew_lease", flaky_renew)
    Worker(store, lease_s=3, heartbeat_s=0.1).run_one()
    assert store.get(t["id"])["status"] == "SUCCEEDED" and len(calls) > 1
