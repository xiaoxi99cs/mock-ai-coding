import pytest
from fastapi.testclient import TestClient

from taskconsole import handlers
from taskconsole.api import create_app
from taskconsole.handlers import TaskCancelled, TaskContext
from taskconsole.states import IllegalTransition, Status, check_transition
from taskconsole.store import LeaseLost
from taskconsole.worker import Worker


def statuses(store, tid):
    return [e["to_status"] for e in store.events(tid) if e["kind"] == "transition"]


def test_transition_table():
    check_transition("PENDING", "CLAIMED")
    check_transition("RUNNING", "RETRY_WAIT")
    for frm, to in [("SUCCEEDED", "RUNNING"), ("PENDING", "SUCCEEDED"), ("DEAD", "PENDING"), ("CREATED", "RUNNING")]:
        with pytest.raises(IllegalTransition):
            check_transition(frm, to)


def test_happy_path(store):
    t = store.submit_task("sleep", {"seconds": 0})
    assert t["status"] == "PENDING"
    Worker(store, worker_id="w1").run_one()
    t = store.get(t["id"])
    assert t["status"] == "SUCCEEDED" and t["result"] == {"slept": 0.0} and t["attempts"] == 1
    assert statuses(store, t["id"]) == ["CREATED", "PENDING", "CLAIMED", "RUNNING", "SUCCEEDED"]


def test_retry_then_succeed_with_backoff(store, clock):
    t = store.submit_task("flaky", {"fail_times": 2, "seconds": 0}, max_attempts=3)
    w = Worker(store, worker_id="w1")
    w.run_one()
    t1 = store.get(t["id"])
    assert t1["status"] == "RETRY_WAIT" and t1["run_after"] == clock() + 1  # base * 2^0
    assert not w.run_one(), "not claimable before run_after"
    clock.advance(1)
    w.run_one()
    assert store.get(t["id"])["run_after"] == clock() + 2  # base * 2^1
    clock.advance(2)
    w.run_one()
    t3 = store.get(t["id"])
    assert t3["status"] == "SUCCEEDED" and t3["attempts"] == 3


def test_attempts_exhausted_goes_dead(store, clock):
    t = store.submit_task("flaky", {"fail_times": 5, "seconds": 0}, max_attempts=2)
    w = Worker(store, worker_id="w1")
    w.run_one()
    clock.advance(10)
    w.run_one()
    t = store.get(t["id"])
    assert t["status"] == "DEAD" and "exhausted" in store.events(t["id"])[-1]["message"]


def test_non_retryable_fails_immediately(store):
    t = store.submit_task("fail_permanent", {}, max_attempts=5)
    Worker(store, worker_id="w1").run_one()
    t = store.get(t["id"])
    assert t["status"] == "FAILED" and t["attempts"] == 1


def test_unknown_type_is_non_retryable(store):
    t = store.submit_task("nope", {})
    Worker(store).run_one()
    assert store.get(t["id"])["status"] == "FAILED"


def test_invalid_payload_fails_without_retrying(store):
    """A task that bypassed API validation fails on its first attempt: a retry can't fix bad input."""
    t = store.submit_task("count_primes", {"n": "abc"}, pool="cpu")
    Worker(store, pool="cpu").run_one()
    t = store.get(t["id"])
    assert t["status"] == "FAILED" and t["attempts"] == 1 and "invalid payload" in t["error"]


def test_validated_payload_fills_defaults_and_coerces():
    spec = handlers.REGISTRY["count_primes"]
    assert handlers.validate_payload(spec, {"n": "10"}) == {"n": 10, "chunks": 20}


def test_cancel_pending_and_terminal(store):
    t = store.submit_task("sleep", {})
    assert store.cancel(t["id"])["status"] == "CANCELLED"
    assert store.cancel(t["id"])["status"] == "CANCELLED"  # idempotent: no error, no second event
    assert [e["to_status"] for e in store.events(t["id"])].count("CANCELLED") == 1

    done = store.submit_task("sleep", {"seconds": 0})
    c = store.claim("default", "w1")
    store.start(done["id"], c["fence"], "w1")
    store.complete(done["id"], c["fence"], "w1")
    with pytest.raises(IllegalTransition):  # TaskFinished: a finished task can't be cancelled
        store.cancel(done["id"])


def test_cancel_running_is_cooperative(store):
    t = store.submit_task("sleep", {"seconds": 0})
    claimed = store.claim("default", "w1")
    store.start(t["id"], claimed["fence"], "w1")
    assert store.cancel(t["id"])["status"] == "RUNNING"  # only flagged
    ctx = TaskContext(store, claimed, "w1")
    with pytest.raises(TaskCancelled):
        ctx.progress(0.1)  # the handler notices at its next progress call
    store.mark_cancelled(t["id"], claimed["fence"], "w1")
    assert store.get(t["id"])["status"] == "CANCELLED"


def test_only_current_fence_can_write(store):
    t = store.submit_task("sleep", {})
    claimed = store.claim("default", "w1")
    with pytest.raises(LeaseLost):
        store.start(t["id"], claimed["fence"] - 1, "w2")


def test_timeout_is_retryable(store):
    t = store.submit_task("sleep", {"seconds": 2}, timeout_s=0.2)
    Worker(store).run_one()
    t = store.get(t["id"])
    assert t["status"] == "RETRY_WAIT" and "TaskTimeout" in t["error"]


def test_checkpoint_resume(store, clock):
    t = store.submit_task("count_primes", {"n": 1000, "chunks": 4}, pool="cpu")
    claimed = store.claim("cpu", "w1")
    f = claimed["fence"]
    store.start(t["id"], f, "w1")
    store.report_progress(t["id"], f, "w1", 0.5, {"chunk": 2, "count": 95})
    store.fail(t["id"], f, "w1", "boom")
    clock.advance(5)
    Worker(store, pool="cpu", worker_id="w2").run_one()
    t = store.get(t["id"])
    assert t["status"] == "SUCCEEDED" and t["result"]["resumed_from_chunk"] == 2
    assert t["result"]["primes"] == 168  # primes < 1000, using the checkpointed count for chunks 0-1
    assert claimed["attempts"] == 1


def test_api_roundtrip(tmp_path):
    app = create_app(str(tmp_path / "api.db"), start_workers=False)
    with TestClient(app) as c:
        assert "sleep" in c.get("/api/task-types").json()
        assert c.post("/api/tasks", json={"type": "bogus"}).status_code == 422
        r = c.post("/api/tasks", json={"type": "count_primes", "payload": {"n": 10}})
        assert r.status_code == 201 and r.json()["pool"] == "cpu"
        tid = r.json()["id"]
        assert c.get(f"/api/tasks/{tid}").json()["events"][0]["to_status"] == "CREATED"
        assert c.post(f"/api/tasks/{tid}/cancel").json()["status"] == "CANCELLED"
        assert c.post(f"/api/tasks/{tid}/cancel").status_code == 200  # idempotent
        assert c.get("/api/tasks/missing").status_code == 404
        assert [t["id"] for t in c.get("/api/tasks?status=CANCELLED").json()["items"]] == [tid]


def test_terminal_states_have_no_exits():
    for frm in ("SUCCEEDED", "FAILED", "DEAD", "CANCELLED"):
        for to in Status:
            with pytest.raises(IllegalTransition):
                check_transition(frm, to)


def test_cancel_retry_wait(store):
    t = store.submit_task("flaky", {"fail_times": 5, "seconds": 0})
    Worker(store).run_one()
    assert store.get(t["id"])["status"] == "RETRY_WAIT"
    assert store.cancel(t["id"])["status"] == "CANCELLED"


def test_pool_isolation_and_priority(store):
    low = store.submit_task("sleep", {}, priority=0)
    high = store.submit_task("sleep", {}, priority=5)
    cpu = store.submit_task("count_primes", {}, pool="cpu")
    assert store.claim("default", "w1")["id"] == high["id"]
    assert store.claim("default", "w1")["id"] == low["id"]
    assert store.claim("default", "w1") is None  # never takes another pool's task
    assert store.claim("cpu", "w2")["id"] == cpu["id"]
