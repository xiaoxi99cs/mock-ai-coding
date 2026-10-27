import pytest
from fastapi.testclient import TestClient

from taskconsole import metrics
from taskconsole.api import create_app
from taskconsole.worker import Worker


def snap(store, pools=("default", "cpu"), heartbeat_s=1.0):
    return metrics.snapshot(store, pools, heartbeat_s)


def test_percentile():
    assert metrics.percentile([], 0.5) is None
    assert metrics.percentile([3], 0.95) == 3
    assert metrics.percentile([1, 2, 3, 4], 0.5) == 2.5
    assert metrics.percentile(list(range(101)), 0.95) == pytest.approx(95)


def test_empty_store(store, clock):
    m = snap(store)
    assert m["now"] == clock()
    assert set(m["status_counts"]) == {"CREATED", "PENDING", "CLAIMED", "RUNNING", "RETRY_WAIT",
                                       "SUCCEEDED", "FAILED", "DEAD", "CANCELLED"}
    assert not any(m["status_counts"].values())
    assert m["queue_depth"] == {p: {"ready": 0, "delayed": 0, "running": 0} for p in ("default", "cpu")}
    assert all(m["counters"][k] == 0 for k in metrics.KEY_COUNTERS)
    assert m["latency"] == {"wait": {"p50": None, "p95": None, "n": 0}, "run": {"p50": None, "p95": None, "n": 0}}
    series = m["throughput"]["series"]
    assert m["throughput"]["bucket_s"] == 5 and len(series) == 60
    assert series[-1]["t"] <= clock() < series[-1]["t"] + 5
    assert all(b["t"] - a["t"] == 5 for a, b in zip(series, series[1:]))
    assert m["workers"] == {"alive": 0, "stale": 0, "dead": 0}


def test_status_counts_queue_depth_and_counters(store, clock):
    store.submit_task("sleep", {"seconds": 0})
    store.submit_task("sleep", {"seconds": 0}, pool="cpu")
    store.submit_task("flaky", {"fail_times": 5, "seconds": 0}, idempotency_key="k")
    store.submit("flaky", {"fail_times": 5, "seconds": 0}, idempotency_key="k")  # dedup hit
    running = store.claim("default", "w1")
    assert running["type"] == "sleep"
    Worker(store, worker_id="w2").run_one()  # flaky fails -> RETRY_WAIT (delayed)

    m = snap(store)
    assert m["status_counts"]["PENDING"] == 1
    assert m["status_counts"]["CLAIMED"] == 1
    assert m["status_counts"]["RETRY_WAIT"] == 1
    assert m["queue_depth"]["default"] == {"ready": 0, "delayed": 1, "running": 1}
    assert m["queue_depth"]["cpu"] == {"ready": 1, "delayed": 0, "running": 0}
    assert m["counters"]["idempotent_submits_deduplicated"] == 1
    assert m["counters"]["tasks_submitted"] == 3

    clock.advance(1)  # backoff elapsed: the retry becomes ready
    assert snap(store)["queue_depth"]["default"] == {"ready": 1, "delayed": 0, "running": 1}


def test_unknown_pool_still_reported(store):
    store.submit_task("sleep", {"seconds": 0}, pool="adhoc")
    assert snap(store, pools=["default"])["queue_depth"]["adhoc"]["ready"] == 1


def test_latency_and_throughput(store, clock):
    a = store.submit_task("sleep", {"seconds": 0})
    b = store.submit_task("sleep", {"seconds": 0})
    clock.advance(2)
    for tid, run_s in ((a["id"], 1), (b["id"], 3)):
        t = store.claim("default", "w1")
        assert t["id"] == tid
        store.start(tid, t["fence"], "w1")
        clock.advance(run_s)
        store.complete(tid, t["fence"], "w1", {})
    c = store.submit_task("fail_permanent", {})
    store.cancel(c["id"])

    m = snap(store)
    # wait: a claimed at +2s, b at +3s (after a ran for 1s); c was never claimed
    assert m["latency"]["wait"] == {"p50": 2.5, "p95": pytest.approx(2.95), "n": 2}
    assert m["latency"]["run"] == {"p50": 2.0, "p95": pytest.approx(2.9), "n": 2}
    totals = {k: sum(b[k] for b in m["throughput"]["series"]) for k in ("succeeded", "failed", "dead", "cancelled")}
    assert totals == {"succeeded": 2, "failed": 0, "dead": 0, "cancelled": 1}
    assert m["throughput"]["series"][-1]["cancelled"] == 1

    clock.advance(metrics.LATENCY_WINDOW_S + 1)  # out of the window
    m = snap(store)
    assert m["latency"]["wait"]["n"] == 0 and m["latency"]["run"]["n"] == 0
    assert sum(b["succeeded"] + b["cancelled"] for b in m["throughput"]["series"]) == 0


def test_worker_states(store, clock):
    store.register_worker("w1", "default", 1, status="idle")
    store.register_worker("w2", "default", 2, status="idle")
    store.register_worker("w3", "cpu", 3, status="idle")
    store.register_worker("w4", "cpu", 4, status="idle")
    store.worker_exited("w3", "dead", -9)
    store.worker_exited("w4", "stopped", 0)
    clock.advance(2)
    store.worker_heartbeat("w1", "idle")
    clock.advance(2)  # w2 last heartbeat 4s ago > 3 * 1s
    assert snap(store)["workers"] == {"alive": 1, "stale": 1, "dead": 1}


def test_prometheus_text(store, clock):
    store.submit_task("sleep", {"seconds": 0})
    Worker(store, worker_id="w1").run_one()
    text = metrics.prometheus(snap(store))
    lines = text.splitlines()
    assert 'taskconsole_tasks{status="SUCCEEDED"} 1' in lines
    assert 'taskconsole_queue_depth{pool="cpu",state="ready"} 0' in lines
    assert "# TYPE taskconsole_retries_total counter" in lines
    assert "taskconsole_fenced_writes_rejected_total 0" in lines
    assert "taskconsole_tasks_submitted_total 1" in lines
    assert any(l.startswith('taskconsole_task_wait_seconds{quantile="0.5"} ') for l in lines)
    assert 'taskconsole_task_run_seconds{quantile="0.95"} 0.0' in lines
    assert 'taskconsole_workers{state="alive"} 0' in lines
    for l in lines:  # every sample line is "name{labels} value" with a numeric value
        if not l.startswith("#"):
            float(l.rsplit(" ", 1)[1])
    empty = metrics.prometheus(metrics.snapshot(store, [], now=clock() + 10_000))
    assert "taskconsole_task_run_seconds{" not in empty


def test_endpoints(tmp_path):
    app = create_app(str(tmp_path / "api.db"), start_workers=False)
    with TestClient(app) as client:
        client.post("/api/tasks", json={"type": "sleep", "payload": {"seconds": 0}})
        m = client.get("/api/metrics").json()
        assert set(m) == {"now", "status_counts", "queue_depth", "counters", "latency", "throughput", "workers"}
        assert m["status_counts"]["PENDING"] == 1
        assert set(m["queue_depth"]) >= {"default", "cpu"}
        assert m["queue_depth"]["default"]["ready"] == 1
        r = client.get("/metrics")
        assert r.status_code == 200 and r.headers["content-type"].startswith("text/plain; version=0.0.4")
        assert 'taskconsole_tasks{status="PENDING"} 1' in r.text
