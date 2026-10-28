"""Deterministic fault-injection tests (fake clock, no real processes).

Each test injects a fault the earlier phase tests don't combine: crashes at every
checkpoint, successive lease expiries, late writes racing recovery, zombie writes of
every kind, lost ledger writes, drains mid-task, and the worker's own grace periods.
"""

import itertools
import threading

import pytest

from taskconsole import handlers
from taskconsole.handlers import TaskContext, _is_prime
from taskconsole.states import TRANSITIONS, IllegalTransition, Status, check_transition
from taskconsole.store import LeaseLost
from taskconsole.worker import EXIT_HARD_TIMEOUT, EXIT_LEASE_LOST, Worker


class SimulatedCrash(BaseException):
    """kill -9 stand-in: nothing after it runs, no outcome is recorded."""


def events(store, tid, kind=None):
    return [e for e in store.events(tid) if kind is None or e["kind"] == kind]


def counter(store, name):
    return store.counters().get(name, 0)


def expire_and_recover(store, clock, lease_s=5):
    clock.advance(lease_s + 1)
    return store.recover_expired()


@pytest.fixture
def register(monkeypatch):
    """Register an ad-hoc handler for one test."""
    def add(name, fn):
        monkeypatch.setitem(handlers.REGISTRY, name, {"fn": fn, "description": name, "example": {},
                                                     "pool": "default"})
    return add


# -- 1. crash at every progress point, resume from the checkpoint -------------

class CrashingContext(TaskContext):
    """Dies right before its `crash_at`-th progress report (0 = before any checkpoint)."""

    def __init__(self, *a, crash_at, **kw):
        super().__init__(*a, **kw)
        self.crash_at, self.reports = crash_at, 0

    def progress(self, fraction, checkpoint=None):
        if self.reports == self.crash_at:
            raise SimulatedCrash()
        super().progress(fraction, checkpoint)
        self.reports += 1


def crash_attempt(store, worker_id, crash_at, chunks):
    task = store.claim("cpu", worker_id, lease_s=5)
    store.start(task["id"], task["fence"], worker_id)
    ctx = CrashingContext(store, task, worker_id, crash_at=crash_at)
    with pytest.raises(SimulatedCrash):
        handlers.count_primes(ctx, task["payload"])
        raise SimulatedCrash()  # crash_at == chunks: died after the last checkpoint, before complete()
    return task


N, CHUNKS = 2000, 5
PRIMES_BELOW_N = sum(map(_is_prime, range(N)))


@pytest.mark.parametrize("crash_at", range(CHUNKS + 1))
def test_crash_at_every_progress_point_resumes_from_checkpoint(store, clock, crash_at):
    t = store.submit_task("count_primes", {"n": N, "chunks": CHUNKS}, pool="cpu", max_attempts=2)
    crash_attempt(store, "w1", crash_at, CHUNKS)
    done_below = N if crash_at == CHUNKS else crash_at * (N // CHUNKS)
    expected_cp = {"chunk": crash_at, "count": sum(map(_is_prime, range(done_below)))} if crash_at else None
    assert store.get(t["id"])["checkpoint"] == expected_cp
    assert expire_and_recover(store, clock) == 1
    clock.advance(60)
    Worker(store, pool="cpu", worker_id="w2").run_one()
    t = store.get(t["id"])
    assert t["status"] == "SUCCEEDED" and t["attempts"] == 2
    assert t["result"] == {"n": N, "primes": PRIMES_BELOW_N, "resumed_from_chunk": crash_at}


@pytest.mark.parametrize("first, second", [(0, 1), (1, 3), (2, 2), (3, 5)])
def test_repeated_crashes_resume_from_latest_checkpoint(store, clock, first, second):
    t = store.submit_task("count_primes", {"n": N, "chunks": CHUNKS}, pool="cpu", max_attempts=3)
    crash_attempt(store, "w1", first, CHUNKS)
    expire_and_recover(store, clock)
    clock.advance(60)
    # attempt 2 resumes at `first`; its own report counter restarts at 0
    crash_attempt(store, "w2", second - first, CHUNKS)
    expire_and_recover(store, clock)
    clock.advance(60)
    Worker(store, pool="cpu", worker_id="w3").run_one()
    t = store.get(t["id"])
    assert t["status"] == "SUCCEEDED" and t["attempts"] == 3
    assert t["result"]["primes"] == PRIMES_BELOW_N and t["result"]["resumed_from_chunk"] == second
    assert counter(store, "lease_expirations") == 2 and counter(store, "retries") == 2


# -- 2. backoff grows, then caps, across successive lease expiries -----------

def test_backoff_grows_and_caps_across_lease_expiries(tmp_path, clock):
    from conftest import Store
    store = Store(str(tmp_path / "b.db"), clock=clock, backoff_base=1.0, backoff_cap=4.0, rng=lambda: 1.0)
    t = store.submit_task("poison", {}, max_attempts=6)
    seen = []
    for attempt in range(1, 6):
        c = store.claim("default", f"w{attempt}", lease_s=5)
        assert c["attempts"] == attempt and c["fence"] == attempt
        clock.advance(6)  # worker died; the lease expires...
        # ...and is recovered in the claim path itself (no reaper): nothing ready yet
        assert store.claim("default", "probe", lease_s=5) is None
        row = store.get(t["id"])
        assert row["status"] == "RETRY_WAIT"
        delay = row["run_after"] - clock()
        seen.append(delay)
        clock.advance(delay - 0.01)
        assert store.claim("default", "probe", lease_s=5) is None, "claimable before run_after"
        clock.advance(0.01)
    assert seen == [1, 2, 4, 4, 4]
    c = store.claim("default", "w6", lease_s=5)
    clock.advance(6)
    store.recover_expired()
    t = store.get(t["id"])
    assert c["attempts"] == 6 and t["status"] == "DEAD"
    assert "attempts exhausted (6/6)" in events(store, t["id"])[-1]["message"]
    assert counter(store, "lease_expirations") == 6 and counter(store, "tasks_dead") == 1


def test_jittered_backoff_uses_rng_within_ceiling(tmp_path, clock):
    from conftest import Store
    draws = itertools.cycle([0.0, 0.25, 0.5, 0.999])
    store = Store(str(tmp_path / "j.db"), clock=clock, backoff_base=2.0, backoff_cap=10.0, rng=lambda: next(draws))
    t = store.submit_task("poison", {}, max_attempts=5)
    delays = []
    for _ in range(4):
        store.claim("default", "w", lease_s=5)
        clock.advance(6)
        store.recover_expired()
        delays.append(store.get(t["id"])["run_after"] - clock())
        clock.advance(100)
    # ceilings 2, 4, 8, 10(cap) scaled by the rng draws
    assert delays == pytest.approx([0.0, 1.0, 4.0, 9.99])


# -- 3. lease expiry racing a late completion ----------------------------------

def test_late_completion_after_recovery_is_rejected_and_counted(store, clock):
    t = store.submit_task("sleep", {"seconds": 0}, max_attempts=3)
    c = store.claim("default", "w1", lease_s=5)
    store.start(t["id"], c["fence"], "w1")
    expire_and_recover(store, clock)  # w1 was paused past its lease
    before = store.get(t["id"])
    with pytest.raises(LeaseLost):
        store.complete(t["id"], c["fence"], "w1", {"late": True})
    after = store.get(t["id"])
    assert after == before and after["status"] == "RETRY_WAIT" and after["result"] is None
    assert counter(store, "fenced_writes_rejected") == 1 and counter(store, "tasks_succeeded") == 0
    rej = events(store, t["id"], "rejected")
    assert len(rej) == 1 and "completion" in rej[0]["message"] and rej[0]["fence"] == c["fence"]
    clock.advance(60)
    Worker(store, worker_id="w2").run_one()  # the retry still runs normally
    assert store.get(t["id"])["status"] == "SUCCEEDED" and store.get(t["id"])["attempts"] == 2


def test_completion_after_expiry_but_before_recovery_wins(store, clock):
    """Nobody recovered the task yet, so the token is still the live one: the write is safe."""
    t = store.submit_task("sleep", {"seconds": 0})
    c = store.claim("default", "w1", lease_s=5)
    store.start(t["id"], c["fence"], "w1")
    clock.advance(6)
    store.complete(t["id"], c["fence"], "w1", {"ok": 1})
    assert store.recover_expired() == 0
    assert store.get(t["id"])["status"] == "SUCCEEDED" and counter(store, "lease_expirations") == 0


def test_worker_swallows_fenced_completion(store, clock, register):
    """The handler 'pauses' past its lease (recovered + reclaimed meanwhile), then returns:
    the Worker's completion is fenced off, nothing raises, the new owner is untouched."""
    holder = {}

    def paused(ctx, payload):
        clock.advance(31)
        store.recover_expired()
        clock.advance(60)
        holder["new"] = store.claim("default", "w2", lease_s=30)
        return {"from": "zombie"}

    register("paused", paused)
    t = store.submit_task("paused", {})
    Worker(store, worker_id="w1").run_one()
    row = store.get(t["id"])
    assert row["status"] == "CLAIMED" and row["worker_id"] == "w2" and row["fence"] == holder["new"]["fence"]
    assert row["result"] is None and counter(store, "fenced_writes_rejected") == 1


# -- 4. cancel requested while the lease expires -------------------------------

def test_cancel_requested_then_lease_expires_on_last_attempt(store, clock):
    t = store.submit_task("sleep", {}, max_attempts=1)
    c = store.claim("default", "w1", lease_s=5)
    store.start(t["id"], c["fence"], "w1")
    clock.advance(6)  # lease already expired, not yet recovered: cancel can only flag it
    assert store.cancel(t["id"])["status"] == "RUNNING"
    store.recover_expired()
    row = store.get(t["id"])
    assert row["status"] == "CANCELLED", "cancel beats DEAD even with attempts exhausted"
    assert counter(store, "tasks_cancelled") == 1 and counter(store, "tasks_dead") == 0
    # the zombie noticing the cancel late can't write either
    for write in (lambda: store.mark_cancelled(t["id"], c["fence"], "w1"),
                  lambda: store.renew_lease(t["id"], c["fence"], "w1", 5)):
        with pytest.raises(LeaseLost):
            write()
    assert store.get(t["id"])["status"] == "CANCELLED" and counter(store, "fenced_writes_rejected") == 2


def test_cancel_after_recovery_to_retry_wait_is_immediate(store, clock):
    t = store.submit_task("sleep", {}, max_attempts=3)
    c = store.claim("default", "w1", lease_s=5)
    expire_and_recover(store, clock)
    assert store.cancel(t["id"])["status"] == "CANCELLED"
    clock.advance(100)
    assert store.claim("default", "w2") is None
    with pytest.raises(LeaseLost):
        store.start(t["id"], c["fence"], "w1")


def test_release_honours_pending_cancel(store):
    t = store.submit_task("sleep", {})
    c = store.claim("default", "w1", lease_s=5)
    store.start(t["id"], c["fence"], "w1")
    store.cancel(t["id"])  # flagged while running...
    store.release(t["id"], c["fence"], "w1", "worker draining")  # ...then the worker drains
    assert store.get(t["id"])["status"] == "CANCELLED"


# -- 5. drain during a task -----------------------------------------------------

def test_drain_releases_task_without_consuming_attempts(store, clock):
    t = store.submit_task("sleep", {"seconds": 5}, max_attempts=1)
    for i in range(3):  # drained three times: max_attempts=1 is never used up
        w = Worker(store, worker_id=f"w{i}", lease_s=5, heartbeat_s=0.02, drain_grace_s=0)
        store.register_worker(w.id, w.pool, 0, "idle")
        w.draining.set()
        assert w.run_one()
        row = store.get(t["id"])
        assert row["status"] == "PENDING" and row["attempts"] == 0 and row["worker_id"] is None
        assert row["lease_expires_at"] is None and row["fence"] == i + 1
        assert counter(store, "retries") == 0
        # the drained worker's token is dead now
        with pytest.raises(LeaseLost):
            store.report_progress(t["id"], row["fence"], w.id, 0.9)
    released = [e for e in events(store, t["id"], "transition") if e["to_status"] == "PENDING"]
    assert len(released) == 4 and all("worker draining" in e["message"] for e in released[1:])
    assert any(w["status"] == "draining" for w in store.list_workers())
    store.cancel(t["id"])


def test_drain_lets_task_finish_within_grace_and_keeps_heartbeating(store, monkeypatch):
    t = store.submit_task("sleep", {"seconds": 0.3})
    renewals = []
    real = store.renew_lease
    monkeypatch.setattr(store, "renew_lease", lambda *a: renewals.append(a) or real(*a))
    w = Worker(store, worker_id="w1", lease_s=5, heartbeat_s=0.03, drain_grace_s=10)
    w.draining.set()
    w.run_one()
    row = store.get(t["id"])
    assert row["status"] == "SUCCEEDED" and row["attempts"] == 1
    assert len(renewals) >= 3, "lease must keep being renewed during the drain grace period"
    stop = threading.Event()
    w.run_forever(stop)  # draining: returns immediately without claiming
    assert store.claim("default", "x") is None


# -- 6. zombie writes of every kind ---------------------------------------------

def zombie_writes(store, tid, fence, wid):
    return {
        "start": lambda: store.start(tid, fence, wid),
        "lease renewal": lambda: store.renew_lease(tid, fence, wid, 5),
        "progress update": lambda: store.report_progress(tid, fence, wid, 0.99, {"chunk": 99, "count": -1}),
        "completion": lambda: store.complete(tid, fence, wid, {"from": "zombie"}),
        "failure": lambda: store.fail(tid, fence, wid, "zombie failure"),
        "failure (non-retryable)": lambda: store.fail(tid, fence, wid, "zombie failure", False),
        "cancel": lambda: store.mark_cancelled(tid, fence, wid),
        "release": lambda: store.release(tid, fence, wid, "zombie drain"),
        "effect": lambda: store.perform_effect(tid, fence, wid, "charge", "charge", {"amount_cents": 1}),
    }


@pytest.mark.parametrize("new_owner_state", ["RETRY_WAIT", "RUNNING", "SUCCEEDED"])
def test_every_kind_of_zombie_write_is_rejected_repeatedly(store, clock, new_owner_state):
    t = store.submit_task("count_primes", {"n": 100}, pool="cpu", max_attempts=3)
    tid = t["id"]
    old = store.claim("cpu", "w1", lease_s=5)
    store.start(tid, old["fence"], "w1")
    store.report_progress(tid, old["fence"], "w1", 0.2, {"chunk": 1, "count": 3})
    expire_and_recover(store, clock)
    if new_owner_state != "RETRY_WAIT":
        clock.advance(60)
        new = store.claim("cpu", "w2", lease_s=5)
        store.start(tid, new["fence"], "w2")
        store.report_progress(tid, new["fence"], "w2", 0.5, {"chunk": 2, "count": 7})
        if new_owner_state == "SUCCEEDED":
            store.complete(tid, new["fence"], "w2", {"from": "w2"})
    snapshot, n_events = store.get(tid), len(store.events(tid))
    writes = zombie_writes(store, tid, old["fence"], "w1")
    for _ in range(3):
        for name, write in writes.items():
            with pytest.raises(LeaseLost):
                write()
    assert store.get(tid) == snapshot, "no zombie write may change the task row"
    assert counter(store, "fenced_writes_rejected") == 3 * len(writes)
    rejected = store.events(tid)[n_events:]
    assert len(rejected) == 3 * len(writes) and all(e["kind"] == "rejected" for e in rejected)
    assert all(e["fence"] == old["fence"] and e["worker_id"] == "w1" for e in rejected)
    assert {e["to_status"] for e in rejected} == {new_owner_state}  # rejected events don't move state
    assert store.external_effects() == [], "a zombie must not reach the provider"
    assert store.conn().execute("SELECT COUNT(*) FROM effects").fetchone()[0] == 0


def test_zombie_effect_on_already_recorded_step_is_rejected(store, clock):
    t = store.submit_task("charge_and_email", {})
    old = store.claim("default", "w1", lease_s=5)
    store.start(t["id"], old["fence"], "w1")
    store.perform_effect(t["id"], old["fence"], "w1", "charge", "charge", {"amount_cents": 1})
    expire_and_recover(store, clock)
    clock.advance(60)
    store.claim("default", "w2", lease_s=5)
    with pytest.raises(LeaseLost):
        store.perform_effect(t["id"], old["fence"], "w1", "charge", "charge", {"amount_cents": 1})
    assert counter(store, "effects_skipped_already_done") == 0


def test_illegal_store_write_rolls_back_atomically(store):
    """A buggy worker completing without start(): CLAIMED -> SUCCEEDED is illegal; nothing is half-written."""
    t = store.submit_task("sleep", {})
    c = store.claim("default", "w1", lease_s=5)
    n_events, counters = len(store.events(t["id"])), store.counters()
    with pytest.raises(IllegalTransition):
        store.complete(t["id"], c["fence"], "w1", {"x": 1})
    row = store.get(t["id"])
    assert row["status"] == "CLAIMED" and row["result"] is None and row["progress"] == 0
    assert len(store.events(t["id"])) == n_events and store.counters() == counters
    w = store.conn().execute("SELECT tasks_succeeded FROM workers WHERE id='w1'").fetchone()
    assert w is None or w[0] == 0


# -- 7. provider dedup when our ledger write is lost ----------------------------

def test_provider_dedups_when_ledger_record_is_fenced_off(store, clock):
    """The worker is paused between the provider call and the ledger write; meanwhile its lease
    is recovered and the task reclaimed. The ledger write is rejected, so the next attempt calls
    the provider again with the same Idempotency-Key and is deduplicated: exactly one charge."""
    t = store.submit_task("charge_and_email", {"customer": "c1", "amount_cents": 700, "email": "a@b.c",
                                               "seconds": 0})
    tid = t["id"]
    old = store.claim("default", "w1", lease_s=5)
    store.start(tid, old["fence"], "w1")

    def paused_before_record():
        expire_and_recover(store, clock)

    with pytest.raises(LeaseLost):
        store.perform_effect(tid, old["fence"], "w1", "charge", "charge", {"amount_cents": 700},
                             crash_before_record=paused_before_record)
    assert store.conn().execute("SELECT COUNT(*) FROM effects").fetchone()[0] == 0  # ledger write lost
    assert counter(store, "external_charges") == 1
    clock.advance(60)
    Worker(store, worker_id="w2", lease_s=5).run_one()
    row = store.get(tid)
    assert row["status"] == "SUCCEEDED" and row["attempts"] == 2
    assert row["result"]["charge"] == f"charge_{tid}:charge"
    charges = [e for e in store.external_effects() if e["kind"] == "charge"]
    assert len(charges) == 1 and charges[0]["duplicate_calls"] == 1
    assert counter(store, "external_charges") == 1 and counter(store, "provider_duplicates_blocked") == 1
    ledger = store.conn().execute("SELECT key, fence FROM effects ORDER BY key").fetchall()
    assert [tuple(r) for r in ledger] == [(f"{tid}:charge", old["fence"] + 1), (f"{tid}:email", old["fence"] + 1)]
    assert counter(store, "fenced_writes_rejected") == 1


def test_provider_dedup_survives_repeated_lost_records(store, clock):
    """Ledger write lost on two consecutive attempts: still one external effect, two blocked dupes."""
    t = store.submit_task("charge_and_email", {"customer": "c1", "amount_cents": 700, "email": "a@b.c", "seconds": 0},
                          max_attempts=3)
    tid = t["id"]
    for wid in ("w1", "w2"):
        c = store.claim("default", wid, lease_s=5)
        store.start(tid, c["fence"], wid)
        with pytest.raises(LeaseLost):
            store.perform_effect(tid, c["fence"], wid, "charge", "charge", {},
                                 crash_before_record=lambda: expire_and_recover(store, clock))
        clock.advance(60)
    Worker(store, worker_id="w3").run_one()
    assert store.get(tid)["status"] == "SUCCEEDED"
    charges = [e for e in store.external_effects() if e["kind"] == "charge"]
    assert len(charges) == 1 and charges[0]["duplicate_calls"] == 2
    assert counter(store, "external_charges") == 1 and counter(store, "external_emails") == 1


# -- 8. the worker's own grace periods (hard timeout, lost lease) ---------------

def test_hard_timeout_keeps_heartbeating_then_exits(store, monkeypatch, register):
    release = threading.Event()
    register("stuck", lambda ctx, payload: release.wait(5))  # ignores the context entirely
    monkeypatch.setattr(Worker, "TIMEOUT_GRACE_S", 0.15)
    renewals, exits = [], []
    real = store.renew_lease
    monkeypatch.setattr(store, "renew_lease", lambda *a: renewals.append(a) or real(*a))
    t = store.submit_task("stuck", {}, timeout_s=0.05, max_attempts=2)
    try:
        Worker(store, worker_id="w1", lease_s=5, heartbeat_s=0.03, hard_exit=exits.append).run_one()
    finally:
        release.set()
    row = store.get(t["id"])
    assert exits == [EXIT_HARD_TIMEOUT]
    assert row["status"] == "RETRY_WAIT" and "hard timeout" in row["error"] and row["attempts"] == 1
    assert len(renewals) >= 3, "heartbeats must continue through the timeout grace period"


def test_worker_that_loses_its_lease_stops_without_writing(store, clock, register):
    def victim(ctx, payload):
        clock.advance(6)  # paused past the lease; another worker takes over
        store.recover_expired()
        clock.advance(60)
        store.claim("default", "w2", lease_s=5)
        ctx.sleep(5)  # cooperative: stops at the next check once the heartbeat sees LeaseLost
        return {"from": "zombie"}

    register("victim", victim)
    t = store.submit_task("victim", {})
    exits = []
    Worker(store, worker_id="w1", lease_s=5, heartbeat_s=0.02, hard_exit=exits.append).run_one()
    row = store.get(t["id"])
    assert exits == [] and row["worker_id"] == "w2" and row["status"] == "CLAIMED" and row["result"] is None
    assert counter(store, "fenced_writes_rejected") == 1  # just the heartbeat; no later write attempted


def test_worker_with_uncooperative_handler_exits_after_losing_lease(store, clock, register, monkeypatch):
    release = threading.Event()

    def stubborn(ctx, payload):
        clock.advance(6)
        store.recover_expired()
        release.wait(5)

    register("stubborn", stubborn)
    real_join = threading.Thread.join
    monkeypatch.setattr(threading.Thread, "join",
                        lambda self, timeout=None: real_join(self, None if timeout is None else min(timeout, 0.1)))
    t = store.submit_task("stubborn", {})
    exits = []
    try:
        Worker(store, worker_id="w1", lease_s=5, heartbeat_s=0.02, hard_exit=exits.append).run_one()
    finally:
        release.set()
    assert exits == [EXIT_LEASE_LOST] and store.get(t["id"])["status"] == "RETRY_WAIT"


# -- 9. transition table: exhaustive -------------------------------------------

S = Status
EXPECTED = {  # independently transcribed from the CLAUDE.md state machine
    S.CREATED: {S.PENDING, S.CANCELLED},
    S.PENDING: {S.CLAIMED, S.CANCELLED},
    S.RETRY_WAIT: {S.CLAIMED, S.CANCELLED},
    S.CLAIMED: {S.RUNNING, S.PENDING, S.RETRY_WAIT, S.DEAD, S.CANCELLED},
    S.RUNNING: {S.SUCCEEDED, S.FAILED, S.RETRY_WAIT, S.DEAD, S.CANCELLED, S.PENDING},
    S.SUCCEEDED: set(), S.FAILED: set(), S.DEAD: set(), S.CANCELLED: set(),
}


def test_transition_table_matches_documented_state_machine():
    assert {k: set(v) for k, v in TRANSITIONS.items() if v} == {k: v for k, v in EXPECTED.items() if v}


@pytest.mark.parametrize("frm, to", list(itertools.product(Status, Status)), ids=lambda s: str(s))
def test_every_status_pair(frm, to):
    if to in EXPECTED[frm]:
        check_transition(frm, to)
        check_transition(str(frm), str(to))  # plain strings (as stored in SQLite) too
    else:
        with pytest.raises(IllegalTransition):
            check_transition(frm, to)


def test_unknown_status_is_rejected():
    with pytest.raises(ValueError):
        check_transition("PENDING", "PAUSED")
