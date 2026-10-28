"""API contract: one error shape, strict input validation, stable cursor pagination, idempotent cancel."""

import inspect

import pytest
from fastapi.testclient import TestClient

from taskconsole.api import create_app
from taskconsole.states import CLAIMABLE
from taskconsole.store import TaskStore


@pytest.fixture
def app(tmp_path, clock):
    return create_app(str(tmp_path / "api.db"), start_workers=False, clock=clock)


@pytest.fixture
def c(app):
    with TestClient(app) as client:
        yield client


def err(r, status, code):
    assert r.status_code == status, r.text
    body = r.json()
    assert set(body) == {"error"} and body["error"]["code"] == code and body["error"]["message"]
    return body["error"]


def submit(c, **kw):
    return c.post("/api/tasks", json={"type": "sleep", "payload": {"seconds": 0}, **kw}).json()["id"]


# -- error envelope --------------------------------------------------------------

def test_every_error_has_the_same_shape(c):
    err(c.get("/api/tasks/missing"), 404, "task_not_found")
    err(c.post("/api/tasks/missing/cancel"), 404, "task_not_found")
    err(c.get("/api/nope"), 404, "not_found")
    err(c.delete("/api/tasks"), 405, "method_not_allowed")
    assert "sleep" in err(c.post("/api/tasks", json={"type": "bogus"}), 422, "unknown_task_type")["details"]["types"]
    assert err(c.post("/api/tasks", json={"type": "sleep", "pool": "x"}), 422, "unknown_pool")["details"]["pools"]
    err(c.put("/api/pools/x", json={"size": 1}), 404, "pool_not_found")
    err(c.post("/api/workers/w-missing/kill"), 404, "worker_not_found")


def test_validation_errors_carry_field_details(c):
    e = err(c.post("/api/tasks", json={"type": "sleep", "max_attempts": 0}), 422, "validation_error")
    assert e["details"][0]["loc"] == ["body", "max_attempts"]
    err(c.post("/api/tasks", content=b"not json", headers={"content-type": "application/json"}),
        422, "validation_error")


def test_invalid_payload_is_rejected_at_submit(c):
    e = err(c.post("/api/tasks", json={"type": "count_primes", "payload": {"n": "abc"}}), 422, "invalid_payload")
    assert e["details"][0]["loc"] == ["n"] and "n:" in e["message"]
    e = err(c.post("/api/tasks", json={"type": "sleep", "payload": {"secnds": 1}}), 422, "invalid_payload")
    assert e["details"][0]["type"] == "extra_forbidden"  # a typo is an error, not a silent default
    assert c.get("/api/tasks").json()["items"] == []  # nothing was enqueued


def test_task_types_publish_their_payload_schema(c):
    schema = c.get("/api/task-types").json()["count_primes"]["payload_schema"]
    assert schema["properties"]["n"]["type"] == "integer" and schema["additionalProperties"] is False


def test_idempotency_conflict_code(c):
    submit(c, idempotency_key="k")
    err(c.post("/api/tasks", json={"type": "sleep", "payload": {"seconds": 9}, "idempotency_key": "k"}),
        409, "idempotency_conflict")


# -- strict query parameters -------------------------------------------------------

def test_list_rejects_unknown_filters_instead_of_returning_nothing(c):
    err(c.get("/api/tasks?status=BOGUS"), 422, "validation_error")
    err(c.get("/api/tasks?pool=nope"), 422, "unknown_pool")
    err(c.get("/api/tasks?limit=0"), 422, "validation_error")
    err(c.get("/api/tasks?limit=501"), 422, "validation_error")


def test_worker_actions_are_a_closed_set(c):
    err(c.post("/api/workers/w1/explode"), 422, "validation_error")


# -- cursor pagination -----------------------------------------------------------

def collect(c, limit, **params):
    ids, cursor = [], None
    while True:
        page = c.get("/api/tasks", params={"limit": limit, **params, **({"cursor": cursor} if cursor else {})}).json()
        ids += [t["id"] for t in page["items"]]
        if (cursor := page["next_cursor"]) is None:
            return ids


def test_pages_are_newest_first_and_complete(c, clock):
    created = []
    for _ in range(7):
        created.append(submit(c))
        clock.advance(1)
    assert collect(c, limit=3) == created[::-1]
    last = c.get("/api/tasks", params={"limit": 7}).json()
    assert len(last["items"]) == 7 and last["next_cursor"] is None


def test_ties_on_created_at_are_broken_by_id(c):
    # The fake clock doesn't advance: all tasks share one created_at.
    created = [submit(c) for _ in range(5)]
    assert collect(c, limit=2) == sorted(created, reverse=True)


def test_inserts_between_pages_never_shift_a_page(c, clock):
    old = []
    for _ in range(4):
        old.append(submit(c))
        clock.advance(1)
    first = c.get("/api/tasks", params={"limit": 2}).json()
    for _ in range(3):  # newer tasks arrive while the client is paging
        submit(c)
        clock.advance(1)
    rest = c.get("/api/tasks", params={"limit": 10, "cursor": first["next_cursor"]}).json()
    # Offset pagination would repeat items here; keyset continues exactly where it stopped.
    assert [t["id"] for t in first["items"] + rest["items"]] == old[::-1]


def test_cursor_respects_filters(c):
    a = submit(c)
    c.post(f"/api/tasks/{a}/cancel")
    submit(c)
    assert collect(c, limit=1, status="CANCELLED") == [a]


@pytest.mark.parametrize("cursor", ["garbage!", "bm90IGpzb24", "WzEsIDJd", "e30"])
def test_malformed_cursor(c, cursor):
    err(c.get("/api/tasks", params={"cursor": cursor}), 400, "invalid_cursor")


# -- cancel semantics --------------------------------------------------------------

def test_cancel_queued_is_idempotent(c):
    tid = submit(c)
    assert [c.post(f"/api/tasks/{tid}/cancel").status_code for _ in range(2)] == [200, 200]
    events = c.get(f"/api/tasks/{tid}").json()["events"]
    assert [e["to_status"] for e in events].count("CANCELLED") == 1


def test_cancel_running_is_accepted_once(c, app):
    tid = submit(c)
    store = app.state.store
    claimed = store.claim("default", "w1")
    store.start(tid, claimed["fence"], "w1")
    r1, r2 = (c.post(f"/api/tasks/{tid}/cancel") for _ in range(2))
    assert (r1.status_code, r2.status_code) == (202, 202)
    assert r2.json()["status"] == "RUNNING" and r2.json()["cancel_requested"]
    requests = [e for e in store.events(tid) if e["message"] == "cancel requested"]
    assert len(requests) == 1


def test_cancel_finished_task_conflicts(c, app):
    tid = submit(c)
    store = app.state.store
    claimed = store.claim("default", "w1")
    store.start(tid, claimed["fence"], "w1")
    store.complete(tid, claimed["fence"], "w1")
    assert "SUCCEEDED" in err(c.post(f"/api/tasks/{tid}/cancel"), 409, "task_already_finished")["message"]


# -- the claim and lease recovery stay index-backed ----------------------------------

def test_claim_reads_ready_tasks_in_order_from_partial_index(app):
    """The candidate scan walks idx_tasks_ready in claim order: no full scan, no sort of the backlog.
    The index is partial, so the claim must spell the claimable statuses as the same literals."""
    src = inspect.getsource(TaskStore.claim)
    assert "status IN ('PENDING','RETRY_WAIT')" in src and set(CLAIMABLE) == {"PENDING", "RETRY_WAIT"}
    plan = [row["detail"] for row in app.state.store.conn().execute(
        "EXPLAIN QUERY PLAN SELECT id FROM tasks WHERE pool='default' AND status IN ('PENDING','RETRY_WAIT')"
        " AND run_after<=0 ORDER BY priority DESC, run_after, created_at LIMIT 1")]
    assert any("idx_tasks_ready" in d for d in plan) and not any("TEMP B-TREE" in d for d in plan), plan


def test_lease_recovery_scan_uses_partial_index(app):
    plan = app.state.store.conn().execute(
        "EXPLAIN QUERY PLAN SELECT * FROM tasks WHERE status IN ('CLAIMED','RUNNING') AND lease_expires_at < 0"
    ).fetchall()
    assert any("idx_tasks_lease" in row["detail"] for row in plan), [row["detail"] for row in plan]
