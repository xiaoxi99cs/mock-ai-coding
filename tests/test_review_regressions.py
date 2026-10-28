"""Regression tests for bugs found in code review (one test per bug, named after the failure)."""

import subprocess

import pytest
from fastapi.testclient import TestClient

from taskconsole.api import create_app
from taskconsole.states import Status
from taskconsole.supervisor import Proc, Supervisor
from taskconsole.worker import Worker


@pytest.fixture
def c(tmp_path, clock):
    app = create_app(str(tmp_path / "api.db"), start_workers=False, clock=clock)
    with TestClient(app, raise_server_exceptions=False) as client:
        yield client


# -- supervisor shutdown must outlast the drain grace -------------------------------

class SlowDrainPopen:
    """A worker that releases its task only once `drain_grace_s` has passed."""

    def __init__(self, drain_s):
        self.drain_s, self.pid, self.returncode, self.killed = drain_s, 1, None, False

    def send_signal(self, sig):
        pass

    def wait(self, timeout=None):
        if timeout is not None and timeout < self.drain_s:
            raise subprocess.TimeoutExpired("worker", timeout)
        self.returncode = 0
        return 0

    def kill(self):
        self.killed, self.returncode = True, -9


def test_stop_waits_out_drain_grace_instead_of_sigkilling(store):
    sup = Supervisor(store, store.path, {"default": 1}, drain_grace_s=10.0)
    procs = [SlowDrainPopen(drain_s=10.0) for _ in range(2)]
    for i, popen in enumerate(procs):
        sup.procs[f"w{i}"] = Proc(f"w{i}", "default", popen)
    sup.stop()
    assert not any(p.killed for p in procs), "worker SIGKILLed mid-drain: its task burns an attempt"


# -- a reused worker id is a fresh worker --------------------------------------------

def test_worker_id_reused_after_restart_is_listed(store, clock):
    store.register_worker("default-1", "default", 100, "idle")
    store.worker_exited("default-1", "stopped", 0)
    clock.advance(120)  # app restarted later; the supervisor numbers workers from 1 again
    store.register_worker("default-1", "default", 200, "starting")
    [w] = store.list_workers()
    assert (w["pid"], w["stopped_at"], w["exit_code"], w["tasks_succeeded"]) == (200, None, None, 0)


# -- API input that used to 500 ------------------------------------------------------

def test_empty_idempotency_key_is_rejected_not_a_500(c):
    for _ in range(2):
        r = c.post("/api/tasks", json={"type": "sleep", "idempotency_key": ""})
        assert r.status_code == 422 and r.json()["error"]["code"] == "validation_error", r.text


def test_store_treats_empty_idempotency_key_as_none(store):
    a, _ = store.submit("sleep", idempotency_key="")
    b, created = store.submit("sleep", idempotency_key="")
    assert created and a["id"] != b["id"] and b["idempotency_key"] is None


def test_out_of_range_priority_is_a_validation_error(c):
    r = c.post("/api/tasks", json={"type": "sleep", "priority": 2**70})
    assert r.status_code == 422 and r.json()["error"]["details"][0]["loc"] == ["body", "priority"]


def test_unexpected_errors_keep_the_error_envelope(c, monkeypatch):
    def boom(*a, **kw):
        raise RuntimeError("db exploded")
    monkeypatch.setattr(c.app.state.store, "list_tasks", boom)
    r = c.get("/api/tasks")
    assert r.status_code == 500 and r.json() == {"error": {"code": "internal_error", "message": "internal server error"}}


# -- bad payloads fail fast instead of being retried ---------------------------------

@pytest.mark.parametrize("type_, payload", [
    ("count_primes", {"chunks": 0}),
    ("count_primes", {"n": "lots"}),
    ("sleep", {"seconds": None}),
    ("flaky", {"fail_times": "x"}),
])
def test_bad_payload_is_non_retryable(store, type_, payload):
    task, _ = store.submit(type_, payload, pool="any", max_attempts=5)
    Worker(store, pool="any", hard_exit=lambda code: None).run_one()
    t = store.get(task["id"])
    assert (t["status"], t["attempts"]) == (Status.FAILED, 1), t["error"]
