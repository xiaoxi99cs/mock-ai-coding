import time

import pytest
from fastapi.testclient import TestClient

from taskconsole import demos
from taskconsole.api import create_app

NAMES = ["happy_path", "retry_backoff", "crash_recovery", "poison_pill", "hang_timeout", "zombie_fencing",
         "idempotent_submit", "no_double_charge", "cancel_running", "multi_pool_burst"]


def wait_run(c, run_id, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        run = next(r for r in c.get("/api/demos/runs").json() if r["run_id"] == run_id)
        if run["status"] != "running":
            return run
        time.sleep(0.1)
    raise AssertionError(f"demo run {run_id} still running after {timeout}s")


@pytest.fixture
def idle_client(tmp_path):
    app = create_app(str(tmp_path / "d.db"), start_workers=False, pools={"default": 1})
    with TestClient(app) as c:
        yield c


def test_registry_and_listing(idle_client):
    assert list(demos.SCENARIOS) == NAMES
    listed = idle_client.get("/api/demos").json()
    assert [d["name"] for d in listed] == NAMES
    for d in listed:
        assert set(d) == {"name", "title", "description", "watch"} and all(d.values())


def test_unknown_demo_404(idle_client):
    r = idle_client.post("/api/demos/nope")
    assert r.status_code == 404 and r.json()["error"]["code"] == "demo_not_found"
    assert idle_client.get("/api/demos/runs").json() == []


def test_start_shape_and_bounded_error(idle_client):
    # happy_path needs the 'cpu' pool, which this app lacks: the run must end in "error", not hang.
    r = idle_client.post("/api/demos/happy_path")
    assert r.status_code == 202
    body = r.json()
    assert body["name"] == "happy_path" and body["status"] == "running"
    assert body["log"] == [] and body["task_ids"] == []
    run = wait_run(idle_client, body["run_id"], timeout=5)
    assert run["status"] == "error" and run["finished_at"] >= run["started_at"]
    assert "pool 'cpu' does not exist" in run["log"][-1]["msg"]
    assert {"t", "msg"} == set(run["log"][0])


def test_wait_times_out_with_error(tmp_path):
    app = create_app(str(tmp_path / "w.db"), start_workers=False, pools={"default": 1})
    run = demos.Run("x", app.state.store, app.state.supervisor, demos.threading.RLock())
    t, created = run.submit("sleep", {"seconds": 1})
    assert created and run.task_ids == [t["id"]]
    with pytest.raises(demos.DemoError, match="timed out"):
        run.wait_terminal(t["id"], 0.2)  # no workers: never finishes
    assert any("CREATED -> PENDING" in e["msg"] for e in run.log_entries)  # followed events are logged


def test_runs_most_recent_first_and_capped(idle_client):
    ids = [idle_client.post("/api/demos/multi_pool_burst").json()["run_id"] for _ in range(demos.MAX_RUNS + 3)]
    runs = idle_client.get("/api/demos/runs").json()
    assert [r["run_id"] for r in runs] == ids[::-1][:demos.MAX_RUNS]


def test_scenarios_end_to_end(tmp_path):
    app = create_app(str(tmp_path / "e2e.db"), pools={"default": 2, "cpu": 1}, lease_s=1.0, heartbeat_s=0.25)
    store = app.state.store
    with TestClient(app) as c:
        for name in ("happy_path", "idempotent_submit", "zombie_fencing", "no_double_charge"):
            run_id = c.post(f"/api/demos/{name}").json()["run_id"]
            run = wait_run(c, run_id)
            assert run["status"] == "done", (name, [e["msg"] for e in run["log"]])
            assert run["task_ids"] and all(store.get(t)["status"] == "SUCCEEDED" for t in run["task_ids"])
        counters = store.counters()
        assert counters["idempotent_submits_deduplicated"] == 1
        assert counters["fenced_writes_rejected"] >= 1
        assert counters["provider_duplicates_blocked"] == 1
        assert counters["external_charges"] == 2
