"""Checks for the enforced rules in GUIDELINES.md. Each test names the rule it guards."""

import ast
import re
import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from taskconsole import db
from taskconsole.api import create_app
from taskconsole.store import LeaseLost, TaskStore
from taskconsole.worker import Worker

PKG = Path(__file__).resolve().parent.parent / "taskconsole"
WRITE_SQL = re.compile(r"^\s*(INSERT\s+(OR\s+\w+\s+)?INTO|UPDATE\s+\w+\s+SET|DELETE\s+FROM|REPLACE\s+INTO)\b")


def modules():
    return {p.name: ast.parse(p.read_text(), str(p)) for p in sorted(PKG.glob("*.py"))}


def strings(node):
    """Every string literal under `node`, including the literal parts of f-strings."""
    return [n.value for n in ast.walk(node) if isinstance(n, ast.Constant) and isinstance(n.value, str)]


def store_methods():
    tree = modules()["store.py"]
    cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "TaskStore")
    return [n for n in cls.body if isinstance(n, ast.FunctionDef)]


# -- C: connections -----------------------------------------------------------

def test_C1_sqlite_connect_only_in_db_module():
    offenders = [
        f"{name}:{n.lineno}"
        for name, tree in modules().items() if name != "db.py"
        for n in ast.walk(tree)
        if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "connect"
        and isinstance(n.func.value, ast.Name) and n.func.value.id == "sqlite3"
    ]
    assert not offenders, f"C1: open connections through db.connect(), not sqlite3.connect: {offenders}"


def test_C2_connection_settings(tmp_path):
    conn = db.connect(str(tmp_path / "c2.db"))
    try:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] > 0
        assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        assert conn.isolation_level is None, "C2: transactions are opened explicitly with BEGIN IMMEDIATE"
    finally:
        conn.close()


# -- W: writes ----------------------------------------------------------------

def test_W1_write_sql_only_in_store():
    offenders = [
        f"{name}: {s.split()[0]} {s.split()[1]}"
        for name, tree in modules().items() if name not in ("store.py", "db.py")
        for s in strings(tree) if WRITE_SQL.match(s)
    ]
    assert not offenders, f"W1: write SQL belongs in store.py: {offenders}"


@pytest.fixture
def traced_writes(monkeypatch):
    """Record (sql, in_transaction) for every write statement on every connection the store opens."""
    seen = []
    real_connect = db.connect

    def connect(path):
        conn = real_connect(path)
        conn.set_trace_callback(lambda sql: seen.append((sql, conn.in_transaction)) if WRITE_SQL.match(sql) else None)
        return conn

    monkeypatch.setattr(db, "connect", connect)
    return seen


def test_W2_trace_detects_autocommit_writes(tmp_path, traced_writes):
    # The detector itself: an autocommit write must show up as outside a transaction.
    store = TaskStore(str(tmp_path / "w2a.db"))
    store.conn().execute("INSERT INTO counters(name, value) VALUES ('x', 1)")
    assert traced_writes[-1][1] is False


def test_W2_every_write_runs_in_a_transaction(tmp_path, traced_writes, clock):
    store = TaskStore(str(tmp_path / "w2.db"), clock=clock, rng=lambda: 1.0)
    w = "w2-worker"
    store.register_worker(w, "default", 1, "idle")
    store.worker_heartbeat(w, "idle")
    store.incr("demo_counter")

    # submit (with an idempotent repeat), cancel a queued task
    a, _ = store.submit("sleep", {"seconds": 0}, idempotency_key="k")
    store.submit("sleep", {"seconds": 0}, idempotency_key="k")
    store.cancel(store.submit("sleep", {"seconds": 0})[0]["id"])

    # the whole worker path: claim, start, lease renewal, progress, side effect, complete
    t = store.claim("default", w, lease_s=30)
    store.start(t["id"], t["fence"], w)
    store.renew_lease(t["id"], t["fence"], w, 30)
    store.report_progress(t["id"], t["fence"], w, 0.5, {"step": 1})
    store.perform_effect(t["id"], t["fence"], w, "charge", "payment", {"amount": 1})
    store.perform_effect(t["id"], t["fence"], w, "charge", "payment", {"amount": 1})  # ledger hit
    store.complete(t["id"], t["fence"], w, {"ok": True})
    assert a["id"] == t["id"]

    # retry, release, cancel while running, lease expiry recovery, stale (fenced) write
    b = store.submit("sleep", {"seconds": 0}, max_attempts=5)[0]
    t = store.claim("default", w, lease_s=30)
    store.fail(t["id"], t["fence"], w, "boom", retryable=True)
    clock.advance(60)
    t = store.claim("default", w, lease_s=5)
    clock.advance(6)                          # the worker stops heartbeating...
    assert store.recover_expired() == 1       # ...so the lease is recovered
    with pytest.raises(LeaseLost):            # and its late write is fenced off
        store.fail(t["id"], t["fence"], w, "late", retryable=True)
    clock.advance(60)
    t = store.claim("default", w, lease_s=30)
    store.release(t["id"], t["fence"], w, "draining")
    t = store.claim("default", w, lease_s=30)
    store.cancel(t["id"])
    store.mark_cancelled(t["id"], t["fence"], w)
    assert store.get(b["id"])["status"] == "CANCELLED"
    store.worker_exited(w, "stopped", 0)

    # and a real worker running a real handler end to end
    c = store.submit("count_primes", {"n": 2000})[0]
    assert Worker(store, worker_id="w2-real", lease_s=30).run_one()
    assert store.get(c["id"])["status"] == "SUCCEEDED"

    assert len(traced_writes) > 30
    outside = sorted({sql.split("(")[0].strip() for sql, in_tx in traced_writes if not in_tx})
    assert not outside, f"W2: writes outside `with self.tx()`: {outside}"


def test_W4_status_changes_only_through_transition():
    allowed = {"_transition", "claim"}
    offenders = [
        m.name for m in store_methods() if m.name not in allowed
        if any(re.search(r"UPDATE\s+tasks\s+SET\b.*\bstatus\s*=", s, re.S) for s in strings(m))
    ]
    assert not offenders, f"W4: raw `UPDATE tasks SET status=` outside _transition()/claim(): {offenders}"


def test_W5_fenced_methods_go_through_fenced():
    def calls_fenced(fn):
        return any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "_fenced"
                   for n in ast.walk(fn))

    fenced = [m for m in store_methods()
              if not m.name.startswith("_") and "fence" in [a.arg for a in m.args.args]]
    assert {m.name for m in fenced} >= {"start", "renew_lease", "complete", "fail", "release"}
    offenders = [m.name for m in fenced if not calls_fenced(m)]
    assert not offenders, f"W5: methods taking a fence must write through _fenced(): {offenders}"


# -- R: retries -----------------------------------------------------------------

def test_R1_api_caps_max_attempts(tmp_path):
    app = create_app(str(tmp_path / "r1.db"), start_workers=False)
    with TestClient(app) as c:
        assert c.post("/api/tasks", json={"type": "sleep", "max_attempts": 10_000}).status_code == 422
        assert c.post("/api/tasks", json={"type": "sleep", "max_attempts": 0}).status_code == 422


@pytest.mark.parametrize("attempt", [1, 2, 5, 30, 1_100, 10**6])
def test_R2_backoff_is_capped_and_jittered_for_any_attempt(tmp_path, attempt):
    cap = 30.0
    lo = TaskStore(str(tmp_path / "lo.db"), backoff_base=1.0, backoff_cap=cap, rng=lambda: 0.0)
    hi = TaskStore(str(tmp_path / "hi.db"), backoff_base=1.0, backoff_cap=cap, rng=lambda: 0.999)
    ceiling = min(cap, 2.0 ** (attempt - 1)) if attempt <= 64 else cap
    assert lo.backoff(attempt) == 0.0                                  # full jitter reaches 0...
    assert hi.backoff(attempt) == pytest.approx(0.999 * ceiling)       # ...and the (capped) ceiling
    assert hi.backoff(attempt) <= cap
